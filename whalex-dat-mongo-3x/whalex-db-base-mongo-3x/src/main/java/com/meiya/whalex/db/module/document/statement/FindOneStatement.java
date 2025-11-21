package com.meiya.whalex.db.module.document.statement;

import com.meiya.whalex.db.entity.document.MongoDatabaseInfo;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

public class FindOneStatement extends BaseMongoStatement {

    private BasicDBObject query;
    private BasicDBObject projection;
    private String collectionName;

    public FindOneStatement(String collectionName, String statement) {
        super(statement);
        this.collectionName = collectionName;
    }

    @Override
    public List<Document> execute(MongoDatabaseInfo mongoDatabaseInfo, MongoClient mongoClient) {
        MongoDatabase database = mongoClient.getDatabase(mongoDatabaseInfo.getDatabaseName());
        MongoCollection<Document> collection = database.getCollection(collectionName);
        FindIterable<Document> documents;
        if(query == null) {
            documents = collection.find();
        }else {
            documents = collection.find(query);
        }
        if(projection != null) {
            documents.projection(projection);
        }
        documents.limit(1);
        MongoCursor<Document> iterator = documents.iterator();
        List<Document> list = new ArrayList<>();
        while (iterator.hasNext()) {
            list.add(iterator.next());
        }
        return list;
    }

    public void setQuery(BasicDBObject query) {
        this.query = query;
    }

    public void setProjection(BasicDBObject projection) {
        this.projection = projection;
    }

}


