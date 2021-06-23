package org.example;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.example.config.ClusterNodeConfig;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class ClusterNode {
    public static void main(String[] args) throws Throwable {
        AnnotationConfigApplicationContext applicationContext =
            new AnnotationConfigApplicationContext(ClusterNodeConfig.class);
        applicationContext.registerShutdownHook();

        IgniteConfiguration igniteConfiguration = applicationContext.getBean(IgniteConfiguration.class);
        try (Ignite ignite = Ignition.start(igniteConfiguration)) {
            System.in.read();
        }
    }
}
