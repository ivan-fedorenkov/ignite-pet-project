package org.example.cache.distributiontracker;

import java.util.concurrent.CompletableFuture;

/**
 * Keeping track of the up to date cache keys distribution across the cluster is
 * not a trivial task. At the same time, this task is very important for us in order
 * to be able to build an optimized system in terms of resources utilization.
 *
 * This class solves this task for a particular cache. It allows users to get the
 * up to date distribution and subscribe on subsequent updates.
 * For the obvious reason this tracker is only applicable to the distributed caches and is not
 * applicable to the replicated and to the local types of cache.
 *
 * Example use case: we would like to subscribe on external events for those
 * entities only that the local node owns, i.e. is primary for those entities.
 *
 * @see CacheKeysDistributionListener
 */
public interface CacheKeysDistributionTracker<K> {
    /**
     * Adds a new listener of the cache keys distribution.
     * Implementations must guarantee to distribute the initial snapshot first and only
     * afterwards distribute subsequent events. Both initial snapshot and subsequent events
     * are to be distributed through the corresponding listener`s methods.
     * @param listener that should be notified upon (re)distribution
     * @return promise that will be completed when the listener is actually added
     */
    CompletableFuture<Void> addCacheKeysDistributionListener(CacheKeysDistributionListener<K> listener);
}
