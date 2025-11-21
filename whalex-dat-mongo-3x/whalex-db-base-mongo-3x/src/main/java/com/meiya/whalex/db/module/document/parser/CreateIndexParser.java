package com.meiya.whalex.db.module.document.parser;

import com.meiya.whalex.db.module.document.statement.CreateIndexStatement;
import com.meiya.whalex.db.module.document.statement.MongoStatement;
import com.mongodb.BasicDBObject;
import com.mongodb.client.model.IndexOptions;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

public class CreateIndexParser extends AbstractParser {


    public MongoStatement handler(Queue<String> queue, String statement, String collectionName) {

        CreateIndexStatement createIndexStatement = new CreateIndexStatement(collectionName, statement);

        List<String> methodParams = getMethodParams(queue, statement);
        int size = methodParams.size();
        switch (size) {
            case 2:
                BasicDBObject basicDBObject = BasicDBObject.parse(methodParams.get(1));
                IndexOptions indexOptions  = buildIndexOptions(basicDBObject);
                createIndexStatement.setIndexOptions(indexOptions);
            case 1:
                BasicDBObject keys = BasicDBObject.parse(methodParams.get(0));
                createIndexStatement.setKeys(keys);
                break;
            default:
                throw new RuntimeException("createIndex方法参数个数异常["+statement+"]");
        }

        if(!queue.isEmpty()) {
            throw new RuntimeException("未知的指令: " + statement);
        }

        return createIndexStatement;
    }

    protected IndexOptions buildIndexOptions(BasicDBObject basicDBObject) {

        IndexOptions indexOptions = new IndexOptions();
        Object background = basicDBObject.get("background");
        if(background != null) {
            indexOptions.background(Boolean.valueOf(background.toString()));
        }
        Object name = basicDBObject.get("name");
        if(name != null) {
            indexOptions.name(name.toString());
        }
        Object sparse = basicDBObject.get("sparse");
        if(sparse != null) {
            indexOptions.sparse(Boolean.valueOf(sparse.toString()));
        }
        Object expireAfterSeconds = basicDBObject.get("expireAfterSeconds");
        if(expireAfterSeconds != null) {
            indexOptions.expireAfter(Long.valueOf(expireAfterSeconds.toString()), TimeUnit.SECONDS);
        }
        Object version = basicDBObject.get("version");
        if(version != null) {
            indexOptions.version(Integer.valueOf(version.toString()));
        }
        Object weights = basicDBObject.get("weights");
        if(weights != null) {
            indexOptions.weights((BasicDBObject) weights);
        }
        Object defaultLanguage = basicDBObject.get("defaultLanguage");
        if(defaultLanguage != null) {
            indexOptions.defaultLanguage(defaultLanguage.toString());
        }
        Object languageOverride = basicDBObject.get("languageOverride");
        if(languageOverride != null) {
            indexOptions.languageOverride(languageOverride.toString());
        }
        Object textVersion = basicDBObject.get("textVersion");
        if(textVersion != null) {
            indexOptions.textVersion(Integer.valueOf(textVersion.toString()));
        }
        Object sphereVersion = basicDBObject.get("sphereVersion");
        if(sphereVersion != null) {
            indexOptions.sphereVersion(Integer.valueOf(sphereVersion.toString()));
        }
        Object bits = basicDBObject.get("bits");
        if(bits != null) {
            indexOptions.bits(Integer.valueOf(bits.toString()));
        }
        Object min = basicDBObject.get("min");
        if(min != null) {
            indexOptions.min(Double.valueOf(min.toString()));
        }
        Object max = basicDBObject.get("max");
        if(max != null) {
            indexOptions.max(Double.valueOf(max.toString()));
        }
        Object bucketSize = basicDBObject.get("bucketSize");
        if(bucketSize != null) {
            indexOptions.bucketSize(Double.valueOf(bucketSize.toString()));
        }
        Object storageEngine = basicDBObject.get("storageEngine");
        if(storageEngine != null) {
            indexOptions.storageEngine((BasicDBObject) storageEngine);
        }
        Object partialFilterExpression = basicDBObject.get("partialFilterExpression");
        if(partialFilterExpression != null) {
            indexOptions.partialFilterExpression((BasicDBObject) partialFilterExpression);
        }
        Object collation = basicDBObject.get("collation");
        if(collation != null) {
            indexOptions.collation(buildCollation((BasicDBObject) collation));
        }
        return indexOptions;
    }


}
