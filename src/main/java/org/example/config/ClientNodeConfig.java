package org.example.config;

import org.apache.ignite.configuration.ClientConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import(BinaryConfig.class)
public class ClientNodeConfig {
    @Bean
    public ClientConfiguration clientConfiguration() {
        ClientConfiguration config = new ClientConfiguration();
        config.setAddresses("127.0.0.1:10800");
        return config;
    }
}
