package org.example;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.EventType;
import org.example.cache.Cache;
import org.example.config.ClusterNodeConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.util.concurrent.atomic.AtomicBoolean;

public class ClusterNode {
    private static final Logger logger = LoggerFactory.getLogger(ClusterNode.class);

    private static final int QUORUM = 3;
    private static final int MIN_QUORUM = 2;
    private static final long BASELINE_AUTO_ADJUSTMENT_TIMEOUT = 10_000;

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

                                logger.info("Populating cache");
                                populateCache(ignite);
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
                nodeJoinedMonitor.notifyAll();
            }
            synchronized (nodeJoinedMonitor) {
                nodeJoinedMonitor.notifyAll();
            }

        }
    }

    private static void populateCache(Ignite ignite) {
        try {
            IgniteCache<Long, String> testCache = ignite.cache(Cache.TEST.getName());
            for (int i = 1; i < 100_000; i++) {
                testCache.put((long) i, "Person#" + i);
            }
        } catch (Throwable t) {
            logger.error("Failed to populate cache", t);
        }
    }
}
