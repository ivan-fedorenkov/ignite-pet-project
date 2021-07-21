package org.example.config;

import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.logger.slf4j.Slf4jLogger;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;
import org.example.service.SuperService;
import org.example.service.SuperServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import java.util.Collection;

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
        serviceConfiguration.setService(new SuperServiceImpl());
        serviceConfiguration.setName(SuperService.SERVICE_NAME);
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
}
