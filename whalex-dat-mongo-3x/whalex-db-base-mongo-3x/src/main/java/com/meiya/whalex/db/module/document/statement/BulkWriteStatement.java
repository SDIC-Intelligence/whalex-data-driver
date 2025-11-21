package com.meiya.whalex.db.module.document.statement;

import com.meiya.whalex.db.entity.document.MongoDatabaseInfo;
import com.meiya.whalex.util.JsonUtil;
import com.mongodb.MongoClient;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.WriteModel;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

public class BulkWriteStatement extends BaseMongoStatement {

    private String collectionName;

    private List<WriteModel<Document>> writeModels;

    private BulkWriteOptions bulkWriteOptions;

    public BulkWriteStatement(String collectionName, String statement) {
        super(statement);
        this.collectionName = collectionName;
    }

    @Override
    public List<Document> execute(MongoDatabaseInfo mongoDatabaseInfo, MongoClient mongoClient) {
        MongoDatabase database = mongoClient.getDatabase(mongoDatabaseInfo.getDatabaseName());
        MongoCollection<Document> collection = database.getCollection(collectionName);

        BulkWriteResult bulkWriteResult;
        if(bulkWriteOptions != null) {
            bulkWriteResult = collection.bulkWrite(writeModels, bulkWriteOptions);
        }else {
            bulkWriteResult = collection.bulkWrite(writeModels);
        }
        Document document = Document.parse(JsonUtil.objectToStr(bulkWriteResult));
        ArrayList<Document> list = new ArrayList<>();
        list.add(document);
        return list;
    }


    public void setBulkWriteOptions(BulkWriteOptions bulkWriteOptions) {
        this.bulkWriteOptions = bulkWriteOptions;
    }

    public void setWriteModels(List<WriteModel<Document>> writeModels) {
        this.writeModels = writeModels;
    }
}
