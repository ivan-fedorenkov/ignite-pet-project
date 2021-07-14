package org.example.config;

import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.logger.slf4j.Slf4jLogger;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.services.ServiceContext;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import java.io.Serializable;
import java.util.Collection;

@Configuration
@Import(CacheConfig.class)
public class ClusterNodeConfig {
    @Autowired
    private Collection<CacheConfiguration<?, ?>> cacheConfigurations;

    @Bean
    public IgniteConfiguration igniteConfiguration(ServiceConfiguration serviceConfig) {
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
        serviceConfiguration.setName("MySuperService");
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

    public static class MySuperService implements Service, Serializable {
        private static final Logger logger = LoggerFactory.getLogger(MySuperService.class);

        private static final long serialVersionUID = -5935242560080229790L;

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
    }
}
