package com.arangodb.internal.audit;

import com.arangodb.ArangoCollection;
import com.arangodb.ArangoDBException;
import com.arangodb.ArangoVertexCollection;
import com.arangodb.entity.DocumentCreateEntity;
import com.arangodb.entity.VertexEntity;
import com.arangodb.entity.VertexUpdateEntity;
import com.arangodb.internal.*;
import com.arangodb.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class ArangoVertexCollectionImpl2 extends InternalArangoVertexCollection<ArangoDBImpl, ArangoDatabaseImpl, ArangoGraphImpl, ArangoExecutorSync> implements ArangoVertexCollection {

    private static final Logger LOGGER = LoggerFactory.getLogger(ArangoCollection.class);

    private Audit audit;

    public ArangoVertexCollectionImpl2(ArangoGraphImpl graph, String name, Audit audit) {
        super(graph, name);
        this.audit = audit;
    }

    public Audit getAudit() {
        return audit;
    }

    @Override
    public void drop() throws ArangoDBException {
        executor.execute(dropRequest(), Void.class);
    }

    @Override
    public <T> VertexEntity<T> insertVertex(T value) throws ArangoDBException {
        VertexEntity<T> vertex = executor.execute(insertVertexRequest(value, new VertexCreateOptions()), insertVertexResponseDeserializer(value));
        LOGGER.info("inserted value into db");
        vertex.setValue(value);
        CompletableFuture<Void> thread = CompletableFuture.runAsync(new Runnable() {
            @Override
            public void run() {
                try {
                    audit.insert(vertex);
                    LOGGER.info("inserted value into kafka");
                } catch (NullPointerException e) {
                    LOGGER.warn("you don't use audit for you document!");
                }
            }
        });
        try {
            thread.get();
        } catch (ExecutionException | InterruptedException e) {
            LOGGER.error("asynchronous startup error");
        }

        return vertex;
    }

    @Override
    public <T> VertexEntity<T> insertVertex(T value, VertexCreateOptions options) throws ArangoDBException {
        VertexEntity<T> vertex = executor.execute(insertVertexRequest(value, options), insertVertexResponseDeserializer(value));
        LOGGER.info("inserted value into db");
        vertex.setValue(value);
        CompletableFuture<Void> thread = CompletableFuture.runAsync(new Runnable() {
            @Override
            public void run() {
                try {
                    audit.insert(vertex);
                    LOGGER.info("inserted value into kafka");
                } catch (NullPointerException e) {
                    LOGGER.warn("you don't use audit for you document!");
                }
            }
        });
        try {
            thread.get();
        } catch (ExecutionException | InterruptedException e) {
            LOGGER.error("asynchronous startup error");
        }
        return vertex;
    }

    @Override
    public <T> T getVertex(String key, Class<T> type) throws ArangoDBException {
        return null;
    }

    @Override
    public <T> T getVertex(String key, Class<T> type, GraphDocumentReadOptions options) throws ArangoDBException {
        return null;
    }

    @Override
    public <T> VertexUpdateEntity<T> replaceVertex(String key, T value) throws ArangoDBException {
        VertexUpdateEntity<T> vertex = executor.execute(replaceVertexRequest(key, value, new VertexReplaceOptions()),
                replaceVertexResponseDeserializer(value));
        LOGGER.info("inserted value into db");
        vertex.setValue(value);
        CompletableFuture<Void> thread = CompletableFuture.runAsync(new Runnable() {
            @Override
            public void run() {
                try {
                    audit.replace(vertex);
                    LOGGER.info("inserted value into kafka");
                } catch (NullPointerException e) {
                    LOGGER.warn("you don't use audit for you document!");
                }
            }
        });
        try {
            thread.get();
        } catch (ExecutionException | InterruptedException e) {
            LOGGER.error("asynchronous startup error");
        }
        return vertex;
    }

    @Override
    public <T> VertexUpdateEntity<T> replaceVertex(String key, T value, VertexReplaceOptions options) throws ArangoDBException {
        VertexUpdateEntity<T> vertex = executor.execute(replaceVertexRequest(key, value, options),
                replaceVertexResponseDeserializer(value));
        LOGGER.info("inserted value into db");
        vertex.setValue(value);
        CompletableFuture<Void> thread = CompletableFuture.runAsync(new Runnable() {
            @Override
            public void run() {
                try {
                    audit.replace(vertex);
                    LOGGER.info("inserted value into kafka");
                } catch (NullPointerException e) {
                    LOGGER.warn("you don't use audit for you document!");
                }
            }
        });
        try {
            thread.get();
        } catch (ExecutionException | InterruptedException e) {
            LOGGER.error("asynchronous startup error");
        }
        return vertex;
    }

    @Override
    public <T> VertexUpdateEntity<T> updateVertex(String key, T value) throws ArangoDBException {
        VertexUpdateEntity<T> vertex = executor.execute(updateVertexRequest(key, value, new VertexUpdateOptions()),
                updateVertexResponseDeserializer(value));
        LOGGER.info("inserted value into db");
        vertex.setValue(value);
        CompletableFuture<Void> thread = CompletableFuture.runAsync(new Runnable() {
            @Override
            public void run() {
                try {
                    audit.update(vertex);
                    LOGGER.info("inserted value into kafka");
                } catch (NullPointerException e) {
                    LOGGER.warn("you don't use audit for you document!");
                }
            }
        });
        try {
            thread.get();
        } catch (ExecutionException | InterruptedException e) {
            LOGGER.error("asynchronous startup error");
        }
        return vertex;
    }

    @Override
    public <T> VertexUpdateEntity<T> updateVertex(String key, T value, VertexUpdateOptions options) throws ArangoDBException {
        VertexUpdateEntity<T> vertex = executor.execute(updateVertexRequest(key, value, options),
                updateVertexResponseDeserializer(value));
        LOGGER.info("inserted value into db");
        vertex.setValue(value);
        CompletableFuture<Void> thread = CompletableFuture.runAsync(new Runnable() {
            @Override
            public void run() {
                try {
                    audit.update(vertex);
                    LOGGER.info("inserted value into kafka");
                } catch (NullPointerException e) {
                    LOGGER.warn("you don't use audit for you document!");
                }
            }
        });
        try {
            thread.get();
        } catch (ExecutionException | InterruptedException e) {
            LOGGER.error("asynchronous startup error");
        }
        return vertex;
    }

    @Override
    public void deleteVertex(String key) throws ArangoDBException {
        executor.execute(deleteVertexRequest(key, new VertexDeleteOptions()), Void.class);
    }

    @Override
    public void deleteVertex(String key, VertexDeleteOptions options) throws ArangoDBException {
        executor.execute(deleteVertexRequest(key, options), Void.class);
    }
}
