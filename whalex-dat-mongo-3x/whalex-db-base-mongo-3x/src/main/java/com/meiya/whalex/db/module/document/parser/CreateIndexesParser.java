package com.meiya.whalex.db.module.document.parser;

import com.meiya.whalex.db.module.document.statement.CreateIndexesStatement;
import com.meiya.whalex.db.module.document.statement.MongoStatement;
import com.mongodb.BasicDBObject;
import com.mongodb.client.model.CreateIndexOptions;
import com.mongodb.client.model.IndexModel;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

public class CreateIndexesParser extends CreateIndexParser {


    public MongoStatement handler(Queue<String> queue, String statement, String collectionName) {

        CreateIndexesStatement createIndexesStatement = new CreateIndexesStatement(collectionName, statement);

        List<String> methodParams = getMethodParams(queue, statement);
        int size = methodParams.size();
        switch (size) {
            case 2:
                BasicDBObject basicDBObject = BasicDBObject.parse(methodParams.get(1));
                CreateIndexOptions createIndexOptions = new CreateIndexOptions();
                Object maxTimeMS = basicDBObject.get("maxTimeMS");
                if(maxTimeMS != null) {
                    createIndexOptions.maxTime(Long.valueOf(maxTimeMS.toString()), TimeUnit.MILLISECONDS);
                }
                createIndexesStatement.setCreateIndexOptions(createIndexOptions);
            case 1:
                List<String> strList = strToList(methodParams.get(0));
                createIndexesStatement.setKeyPatterns(buildIndexModels(strList, statement));
                break;
            default:
                throw new RuntimeException("createIndexes方法参数个数异常["+statement+"]");
        }

        if(!queue.isEmpty()) {
            throw new RuntimeException("未知的指令: " + statement);
        }

        return createIndexesStatement;
    }

    private List<IndexModel> buildIndexModels(List<String> strList, String statement) {
        List<IndexModel> list = new ArrayList<>();
        for (String str : strList) {
            BasicDBObject basicDBObject = BasicDBObject.parse(str);
            Object key = basicDBObject.get("key");
            Object options = basicDBObject.get("options");
            if(key == null) {
                throw new RuntimeException("索引参数不能为空: " + statement);
            }
            if(options == null) {
                options = basicDBObject.remove(key);
            }
            IndexModel indexModel;
            if(options != null) {
                indexModel = new IndexModel((BasicDBObject) key, buildIndexOptions((BasicDBObject) options));
            }else {
                indexModel = new IndexModel((BasicDBObject) key);
            }
            list.add(indexModel);
        }
        return list;
    }


}
