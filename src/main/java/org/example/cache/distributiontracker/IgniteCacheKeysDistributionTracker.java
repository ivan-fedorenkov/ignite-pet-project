package org.example.cache.distributiontracker;

import org.apache.ignite.Ignite;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.query.*;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.CacheRebalancingEvent;
import org.apache.ignite.lang.IgnitePredicate;
import org.example.domain.CacheKeyWithEventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.cache.Cache;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.event.EventType;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * The 'real' Ignite implementation of the cache keys distribution tracker.
 *
 * Think about three streams of events: partition re(distribution), cache entry creation/removal
 * and listeners management. Additionally, there is an initialization phase.
 * In order to simplify the implementation, all of these events and phases are serialized into a single queue
 * of actions, which is to be processed by the local {@link #executorService}.
 *
 * There are two ways of getting information about partitions distribution: {@link Affinity} and
 * partition rebalance events: {@link org.apache.ignite.events.EventType#EVT_CACHE_REBALANCE_PART_LOADED},
 * {@link org.apache.ignite.events.EventType#EVT_CACHE_REBALANCE_OBJECT_UNLOADED}.
 * This class subscribes on the rebalance events at the initialization time. Afterwards, it
 * builds the initial partition distribution though the affinity. So the primary source of knowledge
 * about partitions are the rebalance events. The initial snapshot may contain some extra
 * partitions that are no longer belong to this node any more (if an ongoing PME takes place). This is
 * kind of fine, as we can't track of those partitions only to which this node is the primary, but
 * we can keep track of those partitions only that are local to this node. Hence, users of this class
 * will have to perform the post affinity verification anyway.
 *
 * The only way to get the up to date information about cache keys is to use the {@link ContinuousQuery}.
 * It works good and allows us to keep track of every known cache key in the cluster.
 *
 * It is proven (on a paper) that serialized queue of actions guarantees that every listener will
 * be eventually notified upon every cache key rebalance event and will eventually
 * have an up to date view of all the local cache keys.
 */
public class IgniteCacheKeysDistributionTracker<K> implements CacheKeysDistributionTracker<K>, AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(IgniteCacheKeysDistributionTracker.class);

    private static final int[] REBALANCE_EVENT_TYPES = {
        org.apache.ignite.events.EventType.EVT_CACHE_REBALANCE_PART_LOADED,
        org.apache.ignite.events.EventType.EVT_CACHE_REBALANCE_PART_UNLOADED
    };

    // The local instance of Ignite
    private final Ignite ignite;

    // The distributed cache that is going to be tracked
    private final String cache;

    // A map of partition to a set of existing cache keys (all the cache keys)
    private final Map<Integer, Set<K>> partitionToCacheKeys = new HashMap<>();

    // A set of partitions that are local to this node, i.e. this node is primary or backup
    // for these partitions
    private final Set<Integer> localPartitions = new HashSet<>();

    // A set of cache keys that are local to this node, i.e. this node is primary or backup
    // for these keys
    private final Set<K> localCacheKeys = new HashSet<>();

    // A query that keeps track of cache entities being added or removed
    private final ContinuousQueryWithTransformer<K, Object, CacheKeyWithEventType<K>> continuousQuery;
    // Initialized in the #beginCacheTracking method
    private QueryCursor<Cache.Entry<K, Object>> queryCursor;

    // A local listener of partition rebalance events
    private final LocalPartitionRebalanceEventsListener partitionRebalanceEventsListener =
        new LocalPartitionRebalanceEventsListener();

    // A list of distribution listeners
    private final List<CacheKeysDistributionListener<K>> listeners = new ArrayList<>();

    // Executor service is required in order not to block the Ignite system thread
    private final ExecutorService executorService;

    // A flag that determines if this tracker has been initialized
    private volatile boolean initialized;

    // A flag that determines if this tracker has been stopped
    private volatile boolean stopped;

    // These variables get initialized after Ignite node start
    private ClusterNode localNode;
    private Affinity<K> affinity;

    public IgniteCacheKeysDistributionTracker(Ignite ignite, String cache) {
        this.ignite = ignite;
        this.cache = cache;

        executorService = Executors.newSingleThreadExecutor();

        // A query that keeps track of cache entities being added or removed
        continuousQuery = new ContinuousQueryWithTransformer<K, Object, CacheKeyWithEventType<K>>();
        continuousQuery.setInitialQuery(new ScanQuery<>());
        continuousQuery.setAutoUnsubscribe(true);
        continuousQuery.setLocalListener(new LocalCacheListener());
        continuousQuery.setRemoteFilterFactory(FactoryBuilder.factoryOf(new CacheKeysDistributionTrackerRmtFilter<>()));
        continuousQuery.setRemoteTransformerFactory(FactoryBuilder.factoryOf(
            new CacheKeysDistributionTrackerRmtTransformer<>()));
    }

    /**
     * See {@link AbstractContinuousQuery#setTimeInterval(long)}.
     */
    public void setQueryTimeInterval(long queryTimeInterval) {
        continuousQuery.setTimeInterval(queryTimeInterval);
    }

    /**
     * Initializes the distribution tracker.
     * Note that the Ignite node must be initialized before this method could be used.
     */
    public CompletableFuture<Void> initialize() {
        // If the tracker has been initialized already then just return
        if (initialized) {
            return CompletableFuture.completedFuture(null);
        }

        return CompletableFuture.runAsync(() -> {
            // If the tracker has been initialized already then just return (this time thread safe)
            if (initialized) {
                return;
            }

            // If the tracker has been stopped then just return
            if (stopped) {
                return;
            }

            localNode = ignite.cluster().localNode();
            affinity = ignite.affinity(cache);

            initializePartitionsMap();
            beginPartitionDistributionTracking();
            beginCacheTracking();

            initialized = true;
        }, executorService);
    }

    @Override
    public CompletableFuture<Void> addCacheKeysDistributionListener(CacheKeysDistributionListener<K> listener) {
        return CompletableFuture.runAsync(() -> {
            // If the tracker has been stopped then just return
            if (stopped) {
                return;
            }

            // Add the listener to the list of listeners
            listeners.add(listener);

            // Distribute the up to date snapshot of local cache keys
            localCacheKeys.forEach(listener::onLocalCacheKeyLoaded);
        }, executorService);
    }

    /**
     * Must be executed by the executor thread.
     */
    private void initializePartitionsMap() {
        for (int part = 0; part < affinity.partitions(); part++) {
            partitionToCacheKeys.put(part, new HashSet<>());
        }
    }

    /**
     * Must be executed by the executor thread.
     */
    private void beginPartitionDistributionTracking() {
        // Listen for the local partitions redistribution events
        ignite.events().localListen(partitionRebalanceEventsListener, REBALANCE_EVENT_TYPES);

        // After we have subscribed it is safe to add the partitions that currently belong
        // to this node to the set of local partitions.
        // In worst case we would add some extra partitions that have already been unloaded.
        // The map of cache keys is empty at this stage as we haven't run the cache query yet,
        // so there is no need to manage cache keys.
        for (int part = 0; part < affinity.partitions(); part++) {
            if (affinity.mapPartitionToPrimaryAndBackups(part).contains(localNode)) {
                localPartitions.add(part);
            }
        }
    }

    /**
     * Must be executed by the executor thread.
     */
    private void beginCacheTracking() {
        queryCursor = ignite.getOrCreateCache(cache).query(continuousQuery);

        // This code must be executed by the executor thread at the initialization time,
        // so that it would be guaranteed that no single cache event would be processed
        // beforehand.
        for (Cache.Entry<K, Object> cacheEntry : queryCursor) {
            onCacheKeyCreated(cacheEntry.getKey());
        }
    }

    /**
     * Must be executed by the executor thread.
     */
    private void notifyListenersEntityLoaded(K cacheKey) {
        if (stopped) {
            return;
        }

        for (CacheKeysDistributionListener<K> l : listeners) {
            l.onLocalCacheKeyLoaded(cacheKey);
        }
    }

    /**
     * Must be executed by the executor thread.
     */
    private void notifyListenersEntityUnloaded(K cacheKey) {
        if (stopped) {
            return;
        }

        for (CacheKeysDistributionListener<K> l : listeners) {
            l.onLocalCacheKeyUnloaded(cacheKey);
        }
    }

    /**
     * A callback when a cache key has been added into the distributed cache.
     * Must be executed by the executor thread.
     */
    private void onCacheKeyCreated(K cacheKey) {
        if (stopped) {
            return;
        }

        int part = affinity.partition(cacheKey);

        // Add the cache key to the global map of keys by partition
        boolean added = partitionToCacheKeys.get(part).add(cacheKey);

        // If the key is local to this node add it to the local cache keys set and notify listeners
        if (added && localPartitions.contains(part)) {
            onCacheKeyLoaded(cacheKey);
        }
    }

    /**
     * A callback when a cache key has been loaded into the local node.
     * Must be executed by the executor thread.
     */
    private void onCacheKeyLoaded(K cacheKey) {
        localCacheKeys.add(cacheKey);
        notifyListenersEntityLoaded(cacheKey);
    }

    /**
     * A callback when a cache key has been removed from the distributed cache.
     * Must be executed by the executor thread.
     */
    private void onCacheKeyRemoved(K cacheKey) {
        if (stopped) {
            return;
        }

        int part = affinity.partition(cacheKey);

        // Remove the cache key from the global map of keys by partition
        boolean removed = partitionToCacheKeys.get(part).remove(cacheKey);

        // If the key is local to this node remove it from the local cache keys set and notify listeners
        if (removed && localPartitions.contains(part)) {
            onCacheKeyUnloaded(cacheKey);
        }
    }

    /**
     * A callback when a cache key has been unloaded from the local node.
     * Must be executed by the executor thread.
     */
    private void onCacheKeyUnloaded(K cacheKey) {
        localCacheKeys.remove(cacheKey);
        notifyListenersEntityUnloaded(cacheKey);
    }

    /**
     * A callback when a partition has been loaded into the local node.
     * Must be executed by the executor thread.
     */
    private void onPartLoaded(int part) {
        if (stopped) {
            return;
        }

        localPartitions.add(part);

        // Add the cache keys that belong to the loaded partition to the set of local keys and
        // notify listeners
        Set<K> cacheKeys = partitionToCacheKeys.get(part);
        for (K cacheKey : cacheKeys) {
            localCacheKeys.add(cacheKey);
            onCacheKeyLoaded(cacheKey);
        }
    }

    /**
     * A callback when a partition has been unloaded from the local node.
     * Must be executed by the executor thread.
     */
    private void onPartUnloaded(int part) {
        if (stopped) {
            return;
        }

        localPartitions.remove(part);

        // Remove the cache keys that belong to the unloaded partition from the set of local keys and
        // notify listeners
        Set<K> cacheKeys = partitionToCacheKeys.get(part);
        for (K cacheKey : cacheKeys) {
            localCacheKeys.remove(cacheKey);
            onCacheKeyUnloaded(cacheKey);
        }
    }

    @Override
    public void close() {
        stopped = true;

        // Clean up resources before shutting down the executor

        if (queryCursor != null) {
            queryCursor.close();
        }

        ignite.events().stopLocalListen(partitionRebalanceEventsListener, REBALANCE_EVENT_TYPES);

        if (executorService != null) {
            executorService.shutdownNow();
            try {
                if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
                    logger.warn("Timed out waiting for executor service to shut down");
                }
            } catch (InterruptedException e) {
                logger.warn("Interrupted while waiting for executor service to shut down");
            }
        }
    }

    private class LocalCacheListener
        implements ContinuousQueryWithTransformer.EventListener<CacheKeyWithEventType<K>>
    {
        @Override
        public void onUpdated(Iterable<? extends CacheKeyWithEventType<K>> events) {
            for (CacheKeyWithEventType<K> event : events) {
                if (event.getEventType() == EventType.CREATED) {
                    executorService.execute(() -> onCacheKeyCreated(event.getKey()));
                } else if (event.getEventType() == EventType.REMOVED) {
                    executorService.execute(() -> onCacheKeyRemoved(event.getKey()));
                }
            }
        }
    }

    private class LocalPartitionRebalanceEventsListener implements IgnitePredicate<CacheRebalancingEvent> {
        private static final long serialVersionUID = 0;

        @Override
        public boolean apply(CacheRebalancingEvent cacheEvent) {
            if (cache.equals(cacheEvent.cacheName())) {
                if (cacheEvent.type() ==
                    org.apache.ignite.events.EventType.EVT_CACHE_REBALANCE_PART_LOADED)
                {
                    executorService.execute(() -> onPartLoaded(cacheEvent.partition()));
                } else if (cacheEvent.type() ==
                    org.apache.ignite.events.EventType.EVT_CACHE_REBALANCE_PART_UNLOADED)
                {
                    executorService.execute(() -> onPartUnloaded(cacheEvent.partition()));
                }
            }
            // Continue if the tracker hasn't been stopped
            return !stopped;
        }
    }
}
