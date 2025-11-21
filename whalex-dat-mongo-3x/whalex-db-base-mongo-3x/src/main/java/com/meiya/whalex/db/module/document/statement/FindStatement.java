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

public class FindStatement extends BaseMongoStatement {

    private Integer batchSize;
    private Integer skip;
    private String hint;
    private Integer limit;
    private BasicDBObject max;
    private BasicDBObject query;
    private BasicDBObject projection;
    private BasicDBObject min;
    private BasicDBObject sort;
    private String collectionName;
    private boolean isCount;

    public FindStatement(String collectionName, String statement) {
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
        if(batchSize != null) {
            documents.batchSize(batchSize);
        }
        if(skip != null) {
            documents.skip(skip);
        }
        if(limit != null) {
            documents.limit(limit);
        }
        if(hint != null) {
            BasicDBObject queryHint = new BasicDBObject("$hint", hint);
            documents.hint(queryHint);
        }
        if(max != null) {
            documents.max(max);
        }
        if(min != null) {
            documents.min(min);
        }
        if(sort != null) {
            documents.sort(sort);
        }
        MongoCursor<Document> iterator = documents.iterator();
        List<Document> list = new ArrayList<>();
        if(isCount) {
            int total = 0;
            while (iterator.hasNext()) {
                iterator.next();
                total++;
            }
            Document document = new Document();
            document.put("Result", total);
            list.add(document);
        }else {
            while (iterator.hasNext()) {
                list.add(iterator.next());
            }
        }

        return list;
    }

    public void setBatchSize(Integer batchSize) {
        this.batchSize = batchSize;
    }

    public void setSkip(Integer skip) {
        this.skip = skip;
    }

    public void setHint(String hint) {
        this.hint = hint;
    }

    public void setLimit(Integer limit) {
        this.limit = limit;
    }

    public void setMax(BasicDBObject max) {
        this.max = max;
    }

    public void setQuery(BasicDBObject query) {
        this.query = query;
    }

    public void setProjection(BasicDBObject projection) {
        this.projection = projection;
    }

    public void setMin(BasicDBObject min) {
        this.min = min;
    }

    public void setSort(BasicDBObject sort) {
        this.sort = sort;
    }

    public void setCount(boolean count) {
        isCount = count;
    }
}


