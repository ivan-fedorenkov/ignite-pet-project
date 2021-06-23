package org.example.domain;

import javax.cache.event.EventType;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class CacheKeyWithEventType<K> implements Externalizable {
    private static final long serialVersionUID = 3148763785199831598L;

    private K key;
    private EventType eventType;

    public CacheKeyWithEventType(K key, EventType eventType) {
        this.key = key;
        this.eventType = eventType;
    }

    public CacheKeyWithEventType() {
    }

    public K getKey() {
        return key;
    }

    public EventType getEventType() {
        return eventType;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(key);
        out.writeObject(eventType);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        key = (K) in.readObject();
        eventType = (EventType) in.readObject();
    }
}
