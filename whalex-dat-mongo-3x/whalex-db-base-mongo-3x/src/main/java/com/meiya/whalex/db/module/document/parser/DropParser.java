package com.meiya.whalex.db.module.document.parser;

import com.meiya.whalex.db.module.document.statement.DropStatement;
import com.meiya.whalex.db.module.document.statement.MongoStatement;

import java.util.List;
import java.util.Queue;

public class DropParser extends AbstractParser {


    public MongoStatement handler(Queue<String> queue, String statement, String collectionName) {

        List<String> methodParams = getMethodParams(queue, statement);

        if(!methodParams.isEmpty()) {
            throw new RuntimeException("drop方法无需参数["+statement+"]");
        }

        if(!queue.isEmpty()) {
            throw new RuntimeException("未知的指令: " + statement);
        }

        DropStatement dropStatement = new DropStatement(collectionName, statement);
        return dropStatement;
    }





}
