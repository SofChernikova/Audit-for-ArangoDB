package com.arangodb.internal.audit;

import com.arangodb.ArangoCollection;
import com.arangodb.ArangoDBException;
import com.arangodb.entity.*;
import com.arangodb.internal.ArangoDBImpl;
import com.arangodb.internal.ArangoDatabaseImpl;
import com.arangodb.internal.ArangoExecutorSync;
import com.arangodb.internal.InternalArangoCollection;
import com.arangodb.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class ArangoCollectionImpl2 extends InternalArangoCollection<ArangoDBImpl, ArangoDatabaseImpl, ArangoExecutorSync> implements ArangoCollection {

    private final Audit audit;
    private static final Logger LOGGER = LoggerFactory.getLogger(ArangoCollection.class);

    public ArangoCollectionImpl2(ArangoDatabaseImpl db, String name, Audit audit) {
        super(db, name);
        this.audit = audit;
    }

    public Audit getAudit() {
        return audit;
    }

    @Override
    public <T> DocumentCreateEntity<T> insertDocument(T value) throws ArangoDBException {
        DocumentCreateEntity<T> document = insertDocument(value, new DocumentCreateOptions().returnNew(true));
        LOGGER.info("inserted value into db");
        CompletableFuture<Void> thread = CompletableFuture.runAsync(new Runnable() {
            @Override
            public void run() {
                try {
                    audit.insert(document);
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
        return document;
    }

    @Override
    public <T> DocumentCreateEntity<T> insertDocument(T value, DocumentCreateOptions options) throws ArangoDBException {
        return executor.execute(insertDocumentRequest(value, options), insertDocumentResponseDeserializer(value, options));
    }

    @Override
    public <T> MultiDocumentEntity<DocumentCreateEntity<T>> insertDocuments(Collection<T> values) throws ArangoDBException {
        MultiDocumentEntity<DocumentCreateEntity<T>> collections = insertDocuments(values, new DocumentCreateOptions()
                .returnNew(true));
        LOGGER.info("inserted value into db");
        CompletableFuture<Void> thread = CompletableFuture.runAsync(new Runnable() {
            @Override
            public void run() {
                try {
                    for (DocumentCreateEntity<T> value : collections.getDocuments()) {
                        audit.insert(value);
                        LOGGER.info("inserted value into kafka");
                    }
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

        return collections;
    }

    @Override
    public <T> MultiDocumentEntity<DocumentCreateEntity<T>> insertDocuments(final Collection<T> values, final DocumentCreateOptions options) throws ArangoDBException {
        final DocumentCreateOptions params = (options != null ? options : new DocumentCreateOptions());
        return executor.execute(insertDocumentsRequest(values, params), insertDocumentsResponseDeserializer(values, params));
    }

    @Override
    public <T> DocumentUpdateEntity<T> replaceDocument(String key, T value) throws ArangoDBException {
        DocumentUpdateEntity<T> documentUpdate = replaceDocument(key, value, new DocumentReplaceOptions()
                .returnNew(true).returnOld(true));
        LOGGER.info("inserted value into db");
        CompletableFuture<Void> thread = CompletableFuture.runAsync(new Runnable() {
            @Override
            public void run() {
                try {
                    audit.replace(documentUpdate);
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
        return documentUpdate;
    }

    @Override
    public <T> DocumentUpdateEntity<T> replaceDocument(final String key, final T value, final DocumentReplaceOptions options) throws ArangoDBException {
        return executor.execute(replaceDocumentRequest(key, value, options), replaceDocumentResponseDeserializer(value, options));
    }

    @Override
    public <T> MultiDocumentEntity<DocumentUpdateEntity<T>> replaceDocuments(Collection<T> values) throws ArangoDBException {
        MultiDocumentEntity<DocumentUpdateEntity<T>> collections = replaceDocuments(values, new DocumentReplaceOptions()
                .returnNew(true).returnOld(true));
        LOGGER.info("inserted value into db");
        CompletableFuture<Void> thread = CompletableFuture.runAsync(new Runnable() {
            @Override
            public void run() {
                try {
                    for (DocumentUpdateEntity<T> value : collections.getDocuments()) {
                        audit.insert(value);
                        LOGGER.info("inserted value into kafka");
                    }
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
        return collections;
    }

    @Override
    public <T> MultiDocumentEntity<DocumentUpdateEntity<T>> replaceDocuments(final Collection<T> values, final DocumentReplaceOptions options) throws ArangoDBException {
        final DocumentReplaceOptions params = (options != null ? options : new DocumentReplaceOptions());
        return executor.execute(replaceDocumentsRequest(values, params), replaceDocumentsResponseDeserializer(values, params));
    }

    @Override
    public <T> DocumentUpdateEntity<T> updateDocument(String key, T value) throws ArangoDBException {
        DocumentUpdateEntity<T> documentUpdate = updateDocument(key, value, new DocumentUpdateOptions()
                .returnNew(true).returnOld(true));
        LOGGER.info("inserted value into db");
        CompletableFuture<Void> thread = CompletableFuture.runAsync(new Runnable() {
            @Override
            public void run() {
                try {
                    audit.update(documentUpdate);
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


        return documentUpdate;
    }

    @Override
    public <T> DocumentUpdateEntity<T> updateDocument(final String key, final T value, final DocumentUpdateOptions options) throws ArangoDBException {
        return updateDocument(key, value, options, (Class<T>) value.getClass());
    }

    @Override
    public <T, U> DocumentUpdateEntity<U> updateDocument(final String key, final T value, final DocumentUpdateOptions options, final Class<U> returnType) throws ArangoDBException {
        return executor.execute(updateDocumentRequest(key, value, options), updateDocumentResponseDeserializer(value, options, returnType));
    }

    @Override
    public <T> MultiDocumentEntity<DocumentUpdateEntity<T>> updateDocuments(Collection<T> values) throws ArangoDBException {
        MultiDocumentEntity<DocumentUpdateEntity<T>> collections = updateDocuments(values, new DocumentUpdateOptions().returnNew(true).returnOld(true));
        LOGGER.info("inserted value into db");
        CompletableFuture<Void> thread = CompletableFuture.runAsync(new Runnable() {
            @Override
            public void run() {
                try {
                    for (DocumentUpdateEntity<T> value : collections.getDocuments()) {
                        audit.update(value);
                    }
                } catch (NullPointerException e) {
                    LOGGER.info("WARN: you don't use audit for you document!");
                }
            }
        });
        try {
            thread.get();
        } catch (ExecutionException | InterruptedException e) {
            LOGGER.error("asynchronous startup error");
        }
        return collections;
    }

    @Override
    public <T> MultiDocumentEntity<DocumentUpdateEntity<T>> updateDocuments(final Collection<T> values, final DocumentUpdateOptions options) throws ArangoDBException {
        return updateDocuments(values, options, values.isEmpty() ? null : (Class<T>) values.iterator().next().getClass());
    }

    @Override
    public <T, U> MultiDocumentEntity<DocumentUpdateEntity<U>> updateDocuments(final Collection<T> values, final DocumentUpdateOptions options, final Class<U> returnType) throws ArangoDBException {
        final DocumentUpdateOptions params = (options != null ? options : new DocumentUpdateOptions());
        return executor.execute(updateDocumentsRequest(values, params), updateDocumentsResponseDeserializer(returnType));
    }

    @Override
    public DocumentDeleteEntity<Void> deleteDocument(final String key) throws ArangoDBException {
        return executor.execute(deleteDocumentRequest(key, new DocumentDeleteOptions()), deleteDocumentResponseDeserializer(Void.class));
    }

    @Override
    public <T> DocumentDeleteEntity<T> deleteDocument(final String key, final Class<T> type, final DocumentDeleteOptions options) throws ArangoDBException {
        return executor.execute(deleteDocumentRequest(key, options), deleteDocumentResponseDeserializer(type));
    }

    @Override
    public MultiDocumentEntity<DocumentDeleteEntity<Void>> deleteDocuments(final Collection<?> values) throws ArangoDBException {
        return executor.execute(deleteDocumentsRequest(values, new DocumentDeleteOptions()), deleteDocumentsResponseDeserializer(Void.class));
    }

    @Override
    public <T> MultiDocumentEntity<DocumentDeleteEntity<T>> deleteDocuments(final Collection<?> values, final Class<T> type, final DocumentDeleteOptions options) throws ArangoDBException {
        return executor.execute(deleteDocumentsRequest(values, options), deleteDocumentsResponseDeserializer(type));
    }

    /**
     * -------------------------------------------------------------------------------------------------------------------
     */

    @Override
    public DocumentImportEntity importDocuments(final Collection<?> values) throws ArangoDBException {
        return null;
    }

    @Override
    public DocumentImportEntity importDocuments(final Collection<?> values, final DocumentImportOptions options) throws ArangoDBException {
        return null;
    }

    @Override
    public DocumentImportEntity importDocuments(final String values) throws ArangoDBException {
        return null;
    }

    @Override
    public DocumentImportEntity importDocuments(final String values, final DocumentImportOptions options) throws ArangoDBException {
        return null;
    }

    @Override
    public <T> T getDocument(final String key, final Class<T> type) throws ArangoDBException {
        return null;
    }

    @Override
    public <T> T getDocument(final String key, final Class<T> type, final DocumentReadOptions options) throws ArangoDBException {
        return null;
    }

    @Override
    public <T> MultiDocumentEntity<T> getDocuments(final Collection<String> keys, final Class<T> type) throws ArangoDBException {
        return null;
    }

    @Override
    public <T> MultiDocumentEntity<T> getDocuments(final Collection<String> keys, final Class<T> type, final DocumentReadOptions options) throws ArangoDBException {
        return null;
    }

    @Override
    public Boolean documentExists(String key) {
        return null;
    }

    @Override
    public Boolean documentExists(String key, DocumentExistsOptions options) throws ArangoDBException {
        return null;
    }

    @Override
    public IndexEntity getIndex(String id) throws ArangoDBException {
        return null;
    }

    @Override
    public InvertedIndexEntity getInvertedIndex(String id) throws ArangoDBException {
        return null;
    }

    @Override
    public String deleteIndex(String id) throws ArangoDBException {
        return null;
    }

    @Override
    public IndexEntity ensureHashIndex(Iterable<String> fields, HashIndexOptions options) throws ArangoDBException {
        return null;
    }

    @Override
    public IndexEntity ensureSkiplistIndex(Iterable<String> fields, SkiplistIndexOptions options) throws ArangoDBException {
        return null;
    }

    @Override
    public IndexEntity ensurePersistentIndex(Iterable<String> fields, PersistentIndexOptions options) throws ArangoDBException {
        return null;
    }

    @Override
    public IndexEntity ensureGeoIndex(Iterable<String> fields, GeoIndexOptions options) throws ArangoDBException {
        return null;
    }

    @Override
    public IndexEntity ensureFulltextIndex(Iterable<String> fields, FulltextIndexOptions options) throws ArangoDBException {
        return null;
    }

    @Override
    public IndexEntity ensureTtlIndex(Iterable<String> fields, TtlIndexOptions options) throws ArangoDBException {
        return null;
    }

    @Override
    public IndexEntity ensureZKDIndex(Iterable<String> fields, ZKDIndexOptions options) throws ArangoDBException {
        return null;
    }

    @Override
    public InvertedIndexEntity ensureInvertedIndex(InvertedIndexOptions options) throws ArangoDBException {
        return null;
    }

    @Override
    public Collection<IndexEntity> getIndexes() throws ArangoDBException {
        return null;
    }

    @Override
    public Collection<InvertedIndexEntity> getInvertedIndexes() throws ArangoDBException {
        return null;
    }

    @Override
    public boolean exists() throws ArangoDBException {
        return false;
    }

    @Override
    public CollectionEntity truncate() throws ArangoDBException {
        return null;
    }

    @Override
    public CollectionEntity truncate(CollectionTruncateOptions options) throws ArangoDBException {
        return null;
    }

    @Override
    public CollectionPropertiesEntity count() throws ArangoDBException {
        return null;
    }

    @Override
    public CollectionPropertiesEntity count(CollectionCountOptions options) throws ArangoDBException {
        return null;
    }

    @Override
    public CollectionEntity create() throws ArangoDBException {
        return null;
    }

    @Override
    public CollectionEntity create(CollectionCreateOptions options) throws ArangoDBException {
        return null;
    }

    @Override
    public void drop() throws ArangoDBException {

    }

    @Override
    public void drop(boolean isSystem) throws ArangoDBException {

    }

    @Override
    public CollectionEntity load() throws ArangoDBException {
        return null;
    }

    @Override
    public CollectionEntity unload() throws ArangoDBException {
        return null;
    }

    @Override
    public CollectionEntity getInfo() throws ArangoDBException {
        return null;
    }

    @Override
    public CollectionPropertiesEntity getProperties() throws ArangoDBException {
        return null;
    }

    @Override
    public CollectionPropertiesEntity changeProperties(CollectionPropertiesOptions options) throws ArangoDBException {
        return null;
    }

    @Override
    public CollectionEntity rename(String newName) throws ArangoDBException {
        return null;
    }

    @Override
    public ShardEntity getResponsibleShard(Object value) {
        return null;
    }

    @Override
    public CollectionRevisionEntity getRevision() throws ArangoDBException {
        return null;
    }

    @Override
    public void grantAccess(String user, Permissions permissions) throws ArangoDBException {

    }

    @Override
    public void revokeAccess(String user) throws ArangoDBException {

    }

    @Override
    public void resetAccess(String user) throws ArangoDBException {

    }

    @Override
    public Permissions getPermissions(String user) throws ArangoDBException {
        return null;
    }
}
