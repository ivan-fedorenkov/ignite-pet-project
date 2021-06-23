package org.example.config;

import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ClusterNodeConfig {
    @Bean
    public IgniteConfiguration igniteConfiguration() {
        IgniteConfiguration config = new IgniteConfiguration();
        config.setDiscoverySpi(discoverySpi());
        config.setCommunicationSpi(communicationSpi());
        return config;
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
