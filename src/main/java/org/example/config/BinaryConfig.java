package org.example.config;

import org.apache.ignite.binary.BinaryTypeConfiguration;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.example.domain.CustomObject;
import org.example.serialization.CustomObjectBinarySerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Collection;

@Configuration
public class BinaryConfig {
    @Bean
    public BinaryConfiguration binaryConfiguration(Collection<BinaryTypeConfiguration> binaryTypeConfigs) {
        BinaryConfiguration config = new BinaryConfiguration();
        config.setTypeConfigurations(binaryTypeConfigs);
        return config;
    }

    @Bean
    public BinaryTypeConfiguration customObjectBinaryTypeConfig() {
        BinaryTypeConfiguration config = new BinaryTypeConfiguration(CustomObject.class.getCanonicalName());
        config.setSerializer(new CustomObjectBinarySerializer());
        return config;
    }
}
