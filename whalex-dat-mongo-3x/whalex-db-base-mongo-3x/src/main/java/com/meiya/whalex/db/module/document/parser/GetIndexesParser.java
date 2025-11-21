package com.meiya.whalex.db.module.document.parser;

import com.meiya.whalex.db.module.document.statement.GetIndexesStatement;
import com.meiya.whalex.db.module.document.statement.MongoStatement;

import java.util.List;
import java.util.Queue;

public class GetIndexesParser extends AbstractParser {

    public MongoStatement handler(Queue<String> queue, String statement, String collectionName) {

        GetIndexesStatement getIndexesStatement = new GetIndexesStatement(collectionName, statement);

        List<String> methodParams = getMethodParams(queue, statement);
        int size = methodParams.size();
        if(size != 0) {
            throw new RuntimeException("getIndexes是无参数方法["+statement+"]");
        }
        if(!queue.isEmpty()) {
            throw new RuntimeException("未知的指令: " + statement);
        }

        return getIndexesStatement;
    }

}
