package org.example.service;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;
import org.example.cache.Cache;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SuperServiceImpl implements SuperService, Service {
    private static final long serialVersionUID = 8825495611648421812L;

    @IgniteInstanceResource
    private transient Ignite ignite;

    @LoggerResource(categoryClass = SuperServiceImpl.class)
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
        Set<Integer> remainingKeys = new HashSet<>(keys);
        do {
            attempt++;
            remainingKeys = invokeTasksAsync(remainingKeys);
            if (!remainingKeys.isEmpty()) {
                logger.warning("Failed to execute jobs. Will retry. Keys=" + remainingKeys);
            }
        } while (!remainingKeys.isEmpty() && attempt < retryAttempts);

        if (!remainingKeys.isEmpty()) {
            logger.error("Failed to execute jobs. Will not retry any more. Keys=" + remainingKeys);
        }

        return remainingKeys;
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
                    .affinityRunAsync(Cache.TEST.getName(), key, new CacheLoaderTask(key));
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

    public static class CacheLoaderTask implements IgniteRunnable {
        private static final long serialVersionUID = 6203659886563577325L;

        @IgniteInstanceResource
        private transient Ignite ignite;

        @LoggerResource(categoryClass = CacheLoaderTask.class)
        private transient IgniteLogger logger;

        private final int key;

        public CacheLoaderTask(int key) {
            this.key = key;
        }

        @Override
        public void run() {
            if (logger.isDebugEnabled()) {
                logger.debug("Inserting Key=" + key);
            }

            IgniteCache<Long, String> testCache = ignite.cache(Cache.TEST.getName());
            testCache.put((long) key, "Person#" + key);
        }
    }
}
