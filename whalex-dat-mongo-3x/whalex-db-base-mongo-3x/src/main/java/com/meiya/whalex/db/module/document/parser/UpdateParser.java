package com.meiya.whalex.db.module.document.parser;

import com.meiya.whalex.db.module.document.statement.MongoStatement;
import com.meiya.whalex.db.module.document.statement.UpdateStatement;
import com.mongodb.BasicDBObject;
import com.mongodb.client.model.UpdateOptions;

import java.util.List;
import java.util.Queue;

public class UpdateParser extends AbstractParser {


    public MongoStatement handler(Queue<String> queue, String statement, String collectionName) {

        UpdateStatement updateStatement = new UpdateStatement(collectionName, statement);

        List<String> methodParams = getMethodParams(queue, statement);
        int size = methodParams.size();
        switch (size) {
            case 3:
                BasicDBObject basicDBObject = BasicDBObject.parse(methodParams.get(2));
                UpdateOptions updateOptions = buildUpdateOptions(basicDBObject, updateStatement);
                updateStatement.setUpdateOptions(updateOptions);
            case 2:
                BasicDBObject filter = BasicDBObject.parse(methodParams.get(0));
                BasicDBObject update = BasicDBObject.parse(methodParams.get(1));
                updateStatement.setFilter(filter);
                updateStatement.setUpdate(update);
                break;
            default:
                throw new RuntimeException("update方法参数个数异常["+statement+"]");
        }

        if(!queue.isEmpty()) {
            throw new RuntimeException("未知的指令: " + statement);
        }

        return updateStatement;
    }

    private UpdateOptions buildUpdateOptions(BasicDBObject basicDBObject, UpdateStatement updateStatement) {
        Object upsert = basicDBObject.get("upsert");
        Object bypassDocumentValidation = basicDBObject.get("bypassDocumentValidation");
        Object collation = basicDBObject.get("collation");
        Object arrayFilters = basicDBObject.get("arrayFilters");
        Object multi = basicDBObject.get("multi");
        UpdateOptions updateOptions = new UpdateOptions();
        if(upsert != null) {
            updateOptions.upsert(Boolean.valueOf(upsert.toString()));
        }
        if(bypassDocumentValidation != null) {
            updateOptions.bypassDocumentValidation(Boolean.valueOf(bypassDocumentValidation.toString()));
        }
        if(collation != null) {
            updateOptions.collation(buildCollation((BasicDBObject) collation));
        }
        if(arrayFilters != null) {
            updateOptions.arrayFilters((List<? extends BasicDBObject>) arrayFilters);
        }

        if(multi != null) {
            updateStatement.setMulti(Boolean.valueOf(multi.toString()));
        }

        return updateOptions;
    }


}
