package org.example.config;

import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.example.cache.Cache;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class CacheConfig {
    private static final long REBALANCE_DELAY = 10_000;

    @Bean
    public CacheConfiguration<Long, String> testCache() {
        CacheConfiguration<Long, String> config = new CacheConfiguration<>(Cache.TEST.getName());
        config.setIndexedTypes(Long.class, String.class);
        config.setBackups(1);
        config.setCacheMode(CacheMode.PARTITIONED);
        config.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        config.setRebalanceDelay(REBALANCE_DELAY);
        return config;
    }
}
