package com.arangodb.internal.audit;

import com.arangodb.ArangoCollection;
import com.arangodb.ArangoDBException;
import com.arangodb.ArangoEdgeCollection;
import com.arangodb.entity.EdgeEntity;
import com.arangodb.entity.EdgeUpdateEntity;
import com.arangodb.internal.*;
import com.arangodb.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ArangoEdgeCollectionImpl2 extends InternalArangoEdgeCollection<ArangoDBImpl, ArangoDatabaseImpl, ArangoGraphImpl, ArangoExecutorSync>
        implements ArangoEdgeCollection {

    private static final Logger LOGGER = LoggerFactory.getLogger(ArangoCollection.class);

    private Audit audit;
    public ArangoEdgeCollectionImpl2(ArangoGraphImpl graph, String name, Audit audit) {
        super(graph, name);
        this.audit = audit;
    }

    @Override
    public <T> EdgeEntity insertEdge(T value) throws ArangoDBException {
        EdgeEntity edgeEntity = executor.execute(insertEdgeRequest(value, new EdgeCreateOptions()),
                insertEdgeResponseDeserializer(value));
        try{
            audit.insert(edgeEntity);
        } catch (NullPointerException e) {
            LOGGER.info("WARN: you don't use audit for you document!");
        }
        return edgeEntity;
    }

    @Override
    public <T> EdgeEntity insertEdge(T value, EdgeCreateOptions options) throws ArangoDBException {
        EdgeEntity edgeEntity = executor.execute(insertEdgeRequest(value, options),
                insertEdgeResponseDeserializer(value));
        try{
            audit.insert(edgeEntity);
        } catch (NullPointerException e) {
            LOGGER.info("WARN: you don't use audit for you document!");
        }
        return edgeEntity;
    }

    @Override
    public <T> T getEdge(String key, Class<T> type) throws ArangoDBException {
        return null;
    }

    @Override
    public <T> T getEdge(String key, Class<T> type, GraphDocumentReadOptions options) throws ArangoDBException {
        return null;
    }

    @Override
    public <T> EdgeUpdateEntity replaceEdge(String key, T value) throws ArangoDBException {
        EdgeUpdateEntity edgeUpdate = executor.execute(replaceEdgeRequest(key, value, new EdgeReplaceOptions()),
                replaceEdgeResponseDeserializer(value));
        try{
            audit.replace(edgeUpdate);
        } catch (NullPointerException e) {
            LOGGER.info("WARN: you don't use audit for you document!");
        }
        return edgeUpdate;
    }

    @Override
    public <T> EdgeUpdateEntity replaceEdge(String key, T value, EdgeReplaceOptions options) throws ArangoDBException {
        EdgeUpdateEntity edgeUpdate = executor.execute(replaceEdgeRequest(key, value, options),
                replaceEdgeResponseDeserializer(value));
        try{
            audit.replace(edgeUpdate);
        } catch (NullPointerException e) {
            LOGGER.info("WARN: you don't use audit for you document!");
        }
        return edgeUpdate;
    }

    @Override
    public <T> EdgeUpdateEntity updateEdge(String key, T value) throws ArangoDBException {
        EdgeUpdateEntity edgeUpdate = executor.execute(updateEdgeRequest(key, value, new EdgeUpdateOptions()),
                updateEdgeResponseDeserializer(value));
        try{
            audit.update(edgeUpdate);
        } catch (NullPointerException e) {
            LOGGER.info("WARN: you don't use audit for you document!");
        }
        return edgeUpdate;
    }

    @Override
    public <T> EdgeUpdateEntity updateEdge(String key, T value, EdgeUpdateOptions options) throws ArangoDBException {
        EdgeUpdateEntity edgeUpdate = executor.execute(updateEdgeRequest(key, value, options),
                updateEdgeResponseDeserializer(value));
        try{
            audit.update(edgeUpdate);
        } catch (NullPointerException e) {
            LOGGER.info("WARN: you don't use audit for you document!");
        }
        return edgeUpdate;
    }

    @Override
    public void deleteEdge(String key) throws ArangoDBException {
        executor.execute(deleteEdgeRequest(key, new EdgeDeleteOptions()), Void.class);
    }

    @Override
    public void deleteEdge(String key, EdgeDeleteOptions options) throws ArangoDBException {
        executor.execute(deleteEdgeRequest(key, options), Void.class);
    }
}
