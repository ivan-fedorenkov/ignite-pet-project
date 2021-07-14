package org.example.cache;

public enum Cache {
    TEST("TestCache");

    private final String name;

    Cache(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
