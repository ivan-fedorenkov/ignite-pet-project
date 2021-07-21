package org.example.config;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.cluster.ClusterTopologyException;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.logger.slf4j.Slf4jLogger;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.services.ServiceContext;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;
import org.example.cache.Cache;
import org.example.service.CustomObjectService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Configuration
@Import({
    CacheConfig.class,
    BinaryConfig.class
})
public class ClusterNodeConfig {
    @Autowired
    private Collection<CacheConfiguration<?, ?>> cacheConfigurations;

    @Bean
    public IgniteConfiguration igniteConfiguration(
        ServiceConfiguration serviceConfig,
        BinaryConfiguration binaryConfiguration)
    {
        IgniteConfiguration config = new IgniteConfiguration();
        config.setDiscoverySpi(discoverySpi());
        config.setCommunicationSpi(communicationSpi());
        config.setCacheConfiguration(cacheConfigurations.toArray(new CacheConfiguration[0]));
        config.setServiceConfiguration(serviceConfig);
        config.setClusterStateOnStart(ClusterState.INACTIVE);
        config.setGridLogger(new Slf4jLogger());
        return config;
    }

    @Bean
    public ServiceConfiguration mySuperService() {
        ServiceConfiguration serviceConfiguration = new ServiceConfiguration();
        serviceConfiguration.setMaxPerNodeCount(1);
        serviceConfiguration.setService(new MySuperService());
        serviceConfiguration.setName(CustomObjectService.SERVICE_NAME);
        return serviceConfiguration;
    }

    @Bean
    public TcpDiscoverySpi discoverySpi() {
        TcpDiscoverySpi discoverySpi = new TcpDiscoverySpi();
        discoverySpi.setIpFinder(new TcpDiscoveryMulticastIpFinder());
        return discoverySpi;
    }

    @Bean
    public TcpCommunicationSpi communicationSpi() {
        return new TcpCommunicationSpi();
    }

    public static class MySuperService implements CustomObjectService, Service, Serializable {

        private static final long serialVersionUID = -5935242560080229790L;

        @IgniteInstanceResource
        private transient Ignite ignite;

        @LoggerResource(categoryClass = MySuperService.class)
        private transient IgniteLogger logger;

        @Override
        public void cancel(ServiceContext ctx) {
            logger.info("Cancelling my super service!");
        }

        @Override
        public void init(ServiceContext ctx) throws Exception {
            logger.info("Initializing my super service!");
        }

        @Override
        public void execute(ServiceContext ctx) throws Exception {
            logger.info("Executing my super service!");
        }

        @Override
        public void populateCache() {
            try {
                Set<Integer> keys = IntStream.rangeClosed(1, 100_000).boxed().collect(Collectors.toSet());
                invokeTasksAsyncWithRetry(keys, 1);
            } catch (Throwable t) {
                logger.error("Failed to populate cache", t);
            }
        }

        /**
         * Asynchronously invokes the cache loader task for the given set of keys and
         * synchronously waits for the response. In case of error retries the configured
         * number of times.
         * @param keys that should be inserted into the cache
         * @param retryAttempts number of retry attempts
         * @return set of failed keys
         */
        private Set<Integer> invokeTasksAsyncWithRetry(Set<Integer> keys, int retryAttempts) {
            int attempt = -1;
            Set<Integer> failedKeys;
            do {
                attempt++;
                failedKeys = invokeTasksAsync(keys);
                if (!failedKeys.isEmpty()) {
                    logger.warning("Failed to execute jobs. Will retry. Keys=" + failedKeys);
                }
            } while (!failedKeys.isEmpty() && attempt < retryAttempts);

            if (!failedKeys.isEmpty()) {
                logger.error("Failed to execute jobs. Will not retry any more. Keys=" + failedKeys);
            }

            return failedKeys;
        }

        /**
         * Asynchronously invokes the cache loader task for the given set of keys and
         * synchronously waits for the response.
         * @param keys that should be inserted into the cache
         * @return set of failed keys
         */
        private Set<Integer> invokeTasksAsync(Set<Integer> keys) {
            Set<Integer> failedKeys = ConcurrentHashMap.newKeySet();
            CountDownLatch cdl = new CountDownLatch(keys.size());

            for (Integer key : keys) {
                try {
                    IgniteFuture<?> promise = ignite.compute()
                        .affinityRunAsync(Cache.TEST.getName(), key, new MyCacheLoader(key));
                    promise.listen(completedPromise -> {
                        try {
                            completedPromise.get();
                        } catch (IgniteException e) {
                            logger.error("Failed due to ignite exception. Key=" + key);
                            failedKeys.add(key);
                        } finally {
                            cdl.countDown();
                        }
                    });
                } catch (IgniteException e) {
                    logger.error("Failed due to ignite exception. Key=" + key);
                    failedKeys.add(key);
                    cdl.countDown();
                }
            }

            try {
                cdl.await(1, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                logger.error("Timed out waiting for all jobs to be completed");
            }

            return failedKeys.isEmpty() ? Collections.emptySet() : new HashSet<>(failedKeys);
        }
    }

    public static class MyCacheLoader implements IgniteRunnable {
        private static final long serialVersionUID = 6203659886563577325L;

        @IgniteInstanceResource
        private transient Ignite ignite;

        @LoggerResource(categoryClass = MyCacheLoader.class)
        private transient IgniteLogger logger;

        private final int i;

        public MyCacheLoader(int i) {
            this.i = i;
        }

        @Override
        public void run() {
            IgniteCache<Long, String> testCache = ignite.cache(Cache.TEST.getName());
            testCache.put((long) i, "Person#" + i);
            //logger.info("Processed key=" + i);
        }
    }
}
