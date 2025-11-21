package com.meiya.whalex.db.module.document.statement;

import com.meiya.whalex.db.entity.document.MongoDatabaseInfo;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.FindOneAndReplaceOptions;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

public class FindOneAndReplaceStatement extends BaseMongoStatement {

    private BasicDBObject filter;
    private Document replacement;
    private FindOneAndReplaceOptions findOneAndReplaceOptions;
    private String collectionName;

    public FindOneAndReplaceStatement(String collectionName, String statement) {
        super(statement);
        this.collectionName = collectionName;
    }

    @Override
    public List<Document> execute(MongoDatabaseInfo mongoDatabaseInfo, MongoClient mongoClient) {
        MongoDatabase database = mongoClient.getDatabase(mongoDatabaseInfo.getDatabaseName());
        MongoCollection<Document> collection = database.getCollection(collectionName);
        Document oneAndReplace;
        if(findOneAndReplaceOptions != null) {
            oneAndReplace = collection.findOneAndReplace(filter, replacement, findOneAndReplaceOptions);
        }else {
            oneAndReplace = collection.findOneAndReplace(filter, replacement);
        }
        List<Document> list = new ArrayList<>();
        if(oneAndReplace != null) {
            list.add(oneAndReplace);
        }
        return list;
    }

    public void setFilter(BasicDBObject filter) {
        this.filter = filter;
    }

    public void setReplacement(Document replacement) {
        this.replacement = replacement;
    }

    public void setFindOneAndReplaceOptions(FindOneAndReplaceOptions findOneAndReplaceOptions) {
        this.findOneAndReplaceOptions = findOneAndReplaceOptions;
    }
}


