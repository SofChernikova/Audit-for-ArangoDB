package com.arangodb.internal.audit;

public interface Audit {

    <T> void insert(T value);

    <T> void replace(T value);

    <T> void update(T value);

    void errors(Exception e);
}
