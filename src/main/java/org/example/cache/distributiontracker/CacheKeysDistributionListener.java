package org.example.cache.distributiontracker;

/**
 * An interface that allows to listen for the cache keys (re)distribution events for
 * the local Ignite node, i.e. the node from the Spring context.
 *
 * @see CacheKeysDistributionTracker
 */
public interface CacheKeysDistributionListener<K> {
    /**
     * Notifies that the local node has become primary or backup for a particular cache key.
     * Implementations must either process events very fast or process them in
     * a separate thread.
     */
    void onLocalCacheKeyLoaded(K cacheKey);

    /**
     * Notifies that the local node has stopped being primary and backup for a particular cache key,
     * or that the key has been removed from the cache.
     * Implementations must either process events very fast or process them in
     * a separate thread.
     */
    void onLocalCacheKeyUnloaded(K cacheKey);
}
