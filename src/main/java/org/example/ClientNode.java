package org.example;

import org.apache.ignite.Ignition;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.example.config.ClientNodeConfig;
import org.example.service.SuperService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class ClientNode {
    private static final Logger logger = LoggerFactory.getLogger(ClientNode.class);

    public static void main(String[] args) throws Exception {
        AnnotationConfigApplicationContext applicationContext =
            new AnnotationConfigApplicationContext(ClientNodeConfig.class);
        applicationContext.registerShutdownHook();

        ClientConfiguration clientConfiguration = applicationContext.getBean(ClientConfiguration.class);
        try (IgniteClient igniteClient = Ignition.startClient(clientConfiguration)) {
            SuperService superService =
                igniteClient.services().serviceProxy(SuperService.SERVICE_NAME, SuperService.class);
            superService.populateCache();
            System.in.read();
        }
    }
}
