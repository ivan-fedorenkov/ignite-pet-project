package org.example.cache.distributiontracker;

import org.apache.ignite.lang.IgniteClosure;
import org.example.domain.CacheKeyWithEventType;

import javax.cache.event.CacheEntryEvent;

public class CacheKeysDistributionTrackerRmtTransformer<K>
    implements IgniteClosure<CacheEntryEvent<? extends K, ?>, CacheKeyWithEventType<K>>
{
    private static final long serialVersionUID = 0;

    @Override
    public CacheKeyWithEventType<K> apply(CacheEntryEvent<? extends K, ?> event) {
        return new CacheKeyWithEventType<K>(event.getKey(), event.getEventType());
    }
}
