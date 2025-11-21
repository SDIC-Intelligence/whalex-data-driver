package com.meiya.whalex.db.module.document.statement;

import com.meiya.whalex.db.entity.document.MongoDatabaseInfo;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.CreateIndexOptions;
import com.mongodb.client.model.IndexModel;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

public class CreateIndexesStatement extends BaseMongoStatement {

    private String collectionName;

    private List<IndexModel> keyPatterns;

    private CreateIndexOptions createIndexOptions;

    public CreateIndexesStatement(String collectionName, String statement) {
        super(statement);
        this.collectionName = collectionName;
    }

    @Override
    public List<Document> execute(MongoDatabaseInfo mongoDatabaseInfo, MongoClient mongoClient) {
        MongoDatabase database = mongoClient.getDatabase(mongoDatabaseInfo.getDatabaseName());
        MongoCollection<Document> collection = database.getCollection(collectionName);
        List<String> resultList;
        if(createIndexOptions != null) {
            resultList = collection.createIndexes(keyPatterns, createIndexOptions);
        }else {
            resultList = collection.createIndexes(keyPatterns);
        }
        List<Document> list = new ArrayList<>();
        for (String result : resultList) {
            Document document = new Document();
            document.put("result", result);
            list.add(document);
        }
        return list;
    }

    public void setKeyPatterns(List<IndexModel> keyPatterns) {
        this.keyPatterns = keyPatterns;
    }

    public void setCreateIndexOptions(CreateIndexOptions createIndexOptions) {
        this.createIndexOptions = createIndexOptions;
    }
}
