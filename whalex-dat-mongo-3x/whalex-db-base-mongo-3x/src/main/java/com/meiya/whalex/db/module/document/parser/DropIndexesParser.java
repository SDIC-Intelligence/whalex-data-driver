package com.meiya.whalex.db.module.document.parser;

import com.meiya.whalex.db.module.document.statement.DropIndexesStatement;
import com.meiya.whalex.db.module.document.statement.MongoStatement;

import java.util.List;
import java.util.Queue;

public class DropIndexesParser extends AbstractParser {

    public MongoStatement handler(Queue<String> queue, String statement, String collectionName) {

        DropIndexesStatement dropIndexesStatement = new DropIndexesStatement(collectionName, statement);

        List<String> methodParams = getMethodParams(queue, statement);
        int size = methodParams.size();
        if(size != 0) {
            throw new RuntimeException("dropIndexes是无参数方法["+statement+"]");
        }
        if(!queue.isEmpty()) {
            throw new RuntimeException("未知的指令: " + statement);
        }

        return dropIndexesStatement;
    }

}
