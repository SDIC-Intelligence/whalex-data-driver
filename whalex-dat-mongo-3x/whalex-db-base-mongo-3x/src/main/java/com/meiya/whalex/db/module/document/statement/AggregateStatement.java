package com.meiya.whalex.db.module.document.statement;

import com.meiya.whalex.db.entity.document.MongoDatabaseInfo;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

public class AggregateStatement extends BaseMongoStatement {

    private List<BasicDBObject> aggregateList;
    private String collectionName;

    public AggregateStatement(String collectionName, String statement) {
        super(statement);
        this.collectionName = collectionName;
    }

    public void setAggregateList(List<BasicDBObject> aggregateList) {
        this.aggregateList = aggregateList;
    }

    @Override
    public List<Document> execute(MongoDatabaseInfo mongoDatabaseInfo, MongoClient mongoClient) {
        MongoDatabase database = mongoClient.getDatabase(mongoDatabaseInfo.getDatabaseName());
        MongoCollection<Document> collection = database.getCollection(collectionName);
        AggregateIterable<Document> aggregate = collection.aggregate(aggregateList);
        MongoCursor<Document> iterator = aggregate.iterator();
        List<Document> list = new ArrayList<>();
        while (iterator.hasNext()) {
            list.add(iterator.next());
        }
        return list;
    }
}


