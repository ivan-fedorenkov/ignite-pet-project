package org.example;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.EventType;
import org.example.config.ClusterNodeConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.util.concurrent.atomic.AtomicBoolean;

public class ClusterNode {
    private static final Logger logger = LoggerFactory.getLogger(ClusterNode.class);

    private static final int QUORUM = 2;
    private static final int MIN_QUORUM = 1;
    private static final long BASELINE_AUTO_ADJUSTMENT_TIMEOUT = 20_000;

    public static void main(String[] args) throws Throwable {
        AnnotationConfigApplicationContext applicationContext =
            new AnnotationConfigApplicationContext(ClusterNodeConfig.class);
        applicationContext.registerShutdownHook();

        IgniteConfiguration igniteConfiguration = applicationContext.getBean(IgniteConfiguration.class);
        try (Ignite ignite = Ignition.start(igniteConfiguration)) {
            ignite.cluster().baselineAutoAdjustEnabled(true);
            ignite.cluster().baselineAutoAdjustTimeout(BASELINE_AUTO_ADJUSTMENT_TIMEOUT);

            Object nodeJoinedMonitor = new Object();
            Object nodeLeftMonitor = new Object();
            AtomicBoolean cancelled = new AtomicBoolean(false);

            ignite.events().localListen(event -> {
                logger.info(event.toString());
                if (event.type() == EventType.EVT_NODE_JOINED) {
                    synchronized (nodeJoinedMonitor) {
                        nodeJoinedMonitor.notifyAll();
                    }
                } else {
                    if (ignite.cluster().nodes().size() < QUORUM) {
                        synchronized (nodeLeftMonitor) {
                            nodeLeftMonitor.notifyAll();
                        }
                    }
                }
                return !cancelled.get(); // continue listening
            }, EventType.EVT_NODE_JOINED, EventType.EVT_NODE_LEFT, EventType.EVT_NODE_FAILED);

            new Thread(() -> {
                while (!cancelled.get()) {
                    try {
                        synchronized (nodeJoinedMonitor) {
                            nodeJoinedMonitor.wait();

                            if (ignite.cluster().nodes().size() >= QUORUM &&
                                ignite.cluster().state() != ClusterState.ACTIVE)
                            {
                                logger.info("Activating the cluster");
                                ignite.cluster().state(ClusterState.ACTIVE);
                                logger.info("Cluster has been activated");
                            }
                        }
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }).start();

            new Thread(() -> {
                while (!cancelled.get()) {
                    try {
                        synchronized (nodeLeftMonitor) {
                            nodeLeftMonitor.wait();

                            if (ignite.cluster().nodes().size() < MIN_QUORUM &&
                                ignite.cluster().state() != ClusterState.INACTIVE)
                            {
                                logger.info("Deactivating the cluster");
                                ignite.cluster().state(ClusterState.INACTIVE);
                                logger.info("Cluster has been deactivated");
                            }
                        }
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }).start();

            // Wait for any input to shut down
            System.in.read();

            // Clean up
            cancelled.set(true);
            synchronized (nodeLeftMonitor) {
                nodeLeftMonitor.notifyAll();
            }
            synchronized (nodeJoinedMonitor) {
                nodeJoinedMonitor.notifyAll();
            }

        }
    }
}
