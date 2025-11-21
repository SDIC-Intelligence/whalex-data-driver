package com.meiya.whalex.db.module.document.statement;

import com.meiya.whalex.db.entity.document.MongoDatabaseInfo;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.DistinctIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDbPointer;
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonNumber;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class DistinctStatement extends BaseMongoStatement {

    private String collectionName;

    private String field;

    private BasicDBObject query;

    public DistinctStatement(String collectionName, String statement) {
        super(statement);
        this.collectionName = collectionName;
    }

    @Override
    public List<Document> execute(MongoDatabaseInfo mongoDatabaseInfo, MongoClient mongoClient) {
        MongoDatabase database = mongoClient.getDatabase(mongoDatabaseInfo.getDatabaseName());
        MongoCollection<Document> collection = database.getCollection(collectionName);
        List<BsonValue> iterator;
        if(query != null) {
            iterator = collection.distinct(field, query, BsonValue.class).into(new ArrayList<>());
        }else {
            iterator = collection.distinct(field, BsonValue.class).into(new ArrayList<>());
        }

        List<Document> list = new ArrayList<>();
        for (BsonValue bsonValue : iterator) {
            Document document = new Document();
            document.put(field, getValue(bsonValue));
            list.add(document);
        }
        return list;
    }

    private Object getValue(BsonValue bsonValue) {
        Object value = null;
        if(bsonValue.isNumber()) {
            BsonNumber bsonNumber = bsonValue.asNumber();
            value = bsonNumber.doubleValue();
        }else if(bsonValue.isString()) {
            BsonString bsonString = bsonValue.asString();
            value = bsonString.getValue();
        }else if(bsonValue.isBoolean()){
            BsonBoolean bsonBoolean = bsonValue.asBoolean();
            value = bsonBoolean.getValue();
        }else if(bsonValue.isArray()) {
            BsonArray bsonValues = bsonValue.asArray();
            List<Object> list = new ArrayList<>();
            for (BsonValue bv : bsonValues) {
                list.add(getValue(bv));
            }
            value = list;
        }else if(bsonValue.isBinary()) {
            BsonBinary bsonBinary = bsonValue.asBinary();
            value = bsonBinary.getData();
        }else if(bsonValue.isDateTime()) {
            BsonDateTime bsonDateTime = bsonValue.asDateTime();
            value = bsonDateTime.getValue();
        }else if(bsonValue.isDouble()) {
            BsonDouble bsonDouble = bsonValue.asDouble();
            value = bsonDouble.getValue();
        }else if(bsonValue.isDocument()) {
            BsonDocument bsonDocument = bsonValue.asDocument();
            Set<String> keySet = bsonDocument.keySet();
            Document document = new Document();
            for (String key : keySet) {
                document.put(key, getValue(bsonDocument.get(key)));
            }
            value = document;
        }else if(bsonValue.isObjectId()) {
            BsonObjectId bsonObjectId = bsonValue.asObjectId();
            value = bsonObjectId.getValue();
        }else if(bsonValue.isDecimal128()) {
            BsonDecimal128 bsonDecimal128 = bsonValue.asDecimal128();
            value = bsonDecimal128.doubleValue();
        }else if(bsonValue.isInt32()) {
            BsonInt32 bsonInt32 = bsonValue.asInt32();
            value = bsonInt32.getValue();
        }else if(bsonValue.isInt64()) {
            BsonInt64 bsonInt64 = bsonValue.asInt64();
            value = bsonInt64.getValue();
        }else if(bsonValue.isTimestamp()) {
            BsonTimestamp bsonTimestamp = bsonValue.asTimestamp();
            value = bsonTimestamp.getValue();
        }else if(bsonValue.isNull()) {
            value = null;
        }

        return value;
    }

    public void setField(String field) {
        this.field = field;
    }

    public void setQuery(BasicDBObject query) {
        this.query = query;
    }
}
