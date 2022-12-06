package com.arangodb.internal;

import com.arangodb.ArangoDBException;
import com.arangodb.entity.DocumentCreateEntity;
import com.arangodb.entity.DocumentDeleteEntity;
import com.arangodb.entity.DocumentUpdateEntity;
import com.arangodb.entity.MultiDocumentEntity;
import com.arangodb.model.DocumentCreateOptions;
import com.arangodb.model.DocumentDeleteOptions;
import com.arangodb.model.DocumentReplaceOptions;

import java.util.Collection;

public class ArangoCollectionImpl2 extends ArangoCollectionImpl{
    protected ArangoCollectionImpl2(ArangoDatabaseImpl db, String name) {
        super(db, name);
    }

    @Override
    public <T> DocumentCreateEntity<T> insertDocument(T value) throws ArangoDBException {
        DocumentCreateEntity<T> document = super.insertDocument(value);
        if(document != null){
            System.out.println("ok");
        }
        return document;
    }

//    @Override
//    public <T> DocumentCreateEntity<T> insertDocument(T value, DocumentCreateOptions options) throws ArangoDBException {
//        return super.insertDocument(value, options);
//    }
//
//    @Override
//    public <T> MultiDocumentEntity<DocumentCreateEntity<T>> insertDocuments(Collection<T> values) throws ArangoDBException {
//        return super.insertDocuments(values);
//    }
//
//    @Override
//    public <T> MultiDocumentEntity<DocumentCreateEntity<T>> insertDocuments(Collection<T> values, DocumentCreateOptions options) throws ArangoDBException {
//        return super.insertDocuments(values, options);
//    }


    @Override
    public DocumentDeleteEntity<Void> deleteDocument(String key) throws ArangoDBException {
        DocumentDeleteEntity<Void> documentDeleteEntity = super.deleteDocument(key);
        if(documentDeleteEntity != null){
            System.out.println("ok");
        }
        return documentDeleteEntity;
    }

//    @Override
//    public <T> DocumentDeleteEntity<T> deleteDocument(String key, Class<T> type, DocumentDeleteOptions options) throws ArangoDBException {
//        return super.deleteDocument(key, type, options);
//    }
//
//    @Override
//    public MultiDocumentEntity<DocumentDeleteEntity<Void>> deleteDocuments(Collection<?> values) throws ArangoDBException {
//        return super.deleteDocuments(values);
//    }
//
//    @Override
//    public <T> MultiDocumentEntity<DocumentDeleteEntity<T>> deleteDocuments(Collection<?> values, Class<T> type, DocumentDeleteOptions options) throws ArangoDBException {
//        return super.deleteDocuments(values, type, options);
//    }

    @Override
    public <T> DocumentUpdateEntity<T> replaceDocument(String key, T value) throws ArangoDBException {
        return super.replaceDocument(key, value);
    }

//    @Override
//    public <T> DocumentUpdateEntity<T> replaceDocument(String key, T value, DocumentReplaceOptions options) throws ArangoDBException {
//        return super.replaceDocument(key, value, options);
//    }
//
//    @Override
//    public <T> MultiDocumentEntity<DocumentUpdateEntity<T>> replaceDocuments(Collection<T> values) throws ArangoDBException {
//        return super.replaceDocuments(values);
//    }
//
//    @Override
//    public <T> MultiDocumentEntity<DocumentUpdateEntity<T>> replaceDocuments(Collection<T> values, DocumentReplaceOptions options) throws ArangoDBException {
//        return super.replaceDocuments(values, options);
//    }
}
