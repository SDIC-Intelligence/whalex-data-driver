package com.meiya.whalex.db.module.document.statement;

import com.meiya.whalex.db.entity.document.MongoDatabaseInfo;
import com.mongodb.MongoClient;
import com.mongodb.MongoNamespace;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.RenameCollectionOptions;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

public class RenameCollectionStatement extends BaseMongoStatement {

    private String collectionName;

    private String target;

    private Boolean dropTarget;

    public RenameCollectionStatement(String collectionName, String statement) {
        super(statement);
        this.collectionName = collectionName;
    }

    @Override
    public List<Document> execute(MongoDatabaseInfo mongoDatabaseInfo, MongoClient mongoClient) {
        MongoDatabase database = mongoClient.getDatabase(mongoDatabaseInfo.getDatabaseName());
        MongoCollection<Document> collection = database.getCollection(collectionName);
        MongoNamespace mongoNamespace = new MongoNamespace(mongoDatabaseInfo.getDatabaseName(), target);
        if(dropTarget != null) {
            RenameCollectionOptions renameCollectionOptions = new RenameCollectionOptions();
            renameCollectionOptions.dropTarget(dropTarget);
            collection.renameCollection(mongoNamespace, renameCollectionOptions);
        }else {
            collection.renameCollection(mongoNamespace);
        }

        List<Document> list = new ArrayList<>();

        return list;
    }

    public void setTarget(String target) {
        this.target = target;
    }

    public void setDropTarget(Boolean dropTarget) {
        this.dropTarget = dropTarget;
    }
}
