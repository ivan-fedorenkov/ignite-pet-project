package org.example.cache.distributiontracker;

import org.apache.ignite.cache.CacheEntryEventSerializableFilter;

import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.EventType;

public class CacheKeysDistributionTrackerRmtFilter<K> implements CacheEntryEventSerializableFilter<K, Object> {
    private static final long serialVersionUID = 0;

    @Override
    public boolean evaluate(CacheEntryEvent<? extends K, ?> event) throws CacheEntryListenerException {
        return event.getEventType() == EventType.CREATED || event.getEventType() == EventType.REMOVED;
    }
}
