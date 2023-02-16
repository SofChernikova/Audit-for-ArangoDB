package com.arangodb.internal.audit;

import com.arangodb.ArangoCollection;
import com.arangodb.ArangoDBException;
import com.arangodb.ArangoVertexCollection;
import com.arangodb.entity.VertexEntity;
import com.arangodb.entity.VertexUpdateEntity;
import com.arangodb.internal.*;
import com.arangodb.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ArangoVertexCollectionImpl2 extends InternalArangoVertexCollection<ArangoDBImpl, ArangoDatabaseImpl, ArangoGraphImpl, ArangoExecutorSync>
        implements ArangoVertexCollection {

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
        VertexEntity<T> vertex = executor.execute(insertVertexRequest(value, new VertexCreateOptions()),
                insertVertexResponseDeserializer(value));
        vertex.setValue(value);
        try {
            audit.insert(vertex.getValue());
        } catch (NullPointerException e) {
            LOGGER.info("WARN: you don't use audit for you document!");
        }
        return vertex;
    }

    @Override
    public <T> VertexEntity<T> insertVertex(T value, VertexCreateOptions options) throws ArangoDBException {
        VertexEntity<T> vertex = executor.execute(insertVertexRequest(value, options),
                insertVertexResponseDeserializer(value));
        vertex.setValue(value);
        try {
            audit.insert(vertex.getValue());
        } catch (NullPointerException e) {
            LOGGER.info("WARN: you don't use audit for you document!");
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
        vertex.setValue(value);
        try {
            audit.replace(vertex.getValue());
        } catch (NullPointerException e) {
            LOGGER.info("WARN: you don't use audit for you document!");
        }
        return vertex;
    }

    @Override
    public <T> VertexUpdateEntity<T> replaceVertex(String key, T value, VertexReplaceOptions options) throws ArangoDBException {
        VertexUpdateEntity<T> vertex = executor.execute(replaceVertexRequest(key, value, options),
                replaceVertexResponseDeserializer(value));
        vertex.setValue(value);
        try {
            audit.replace(vertex.getValue());
        } catch (NullPointerException e) {
            LOGGER.info("WARN: you don't use audit for you document!");
        }
        return vertex;
    }

    @Override
    public <T> VertexUpdateEntity<T> updateVertex(String key, T value) throws ArangoDBException {
        VertexUpdateEntity<T> vertex = executor.execute(updateVertexRequest(key, value, new VertexUpdateOptions()), updateVertexResponseDeserializer(value));
        vertex.setValue(value);
        try {
            audit.update(vertex.getValue());
        } catch (NullPointerException e) {
            LOGGER.info("WARN: you don't use audit for you document!");
        }
        return vertex;
    }

    @Override
    public <T> VertexUpdateEntity<T> updateVertex(String key, T value, VertexUpdateOptions options) throws ArangoDBException {
        VertexUpdateEntity<T> vertex = executor.execute(updateVertexRequest(key, value, options), updateVertexResponseDeserializer(value));
        vertex.setValue(value);
        try {
            audit.update(vertex.getValue());
        } catch (NullPointerException e) {
            LOGGER.info("WARN: you don't use audit for you document!");
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
