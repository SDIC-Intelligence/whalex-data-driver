package com.meiya.whalex.db.module.document.statement;

import com.meiya.whalex.db.entity.document.MongoDatabaseInfo;
import com.mongodb.MongoClient;
import org.bson.Document;

import java.util.List;

public interface MongoStatement {
    List<Document> execute(MongoDatabaseInfo mongoDatabaseInfo, MongoClient mongoClient);
}

