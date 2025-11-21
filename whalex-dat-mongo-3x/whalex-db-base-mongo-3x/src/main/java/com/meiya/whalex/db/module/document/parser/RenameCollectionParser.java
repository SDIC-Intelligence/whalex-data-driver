package com.meiya.whalex.db.module.document.parser;

import com.meiya.whalex.db.module.document.statement.MongoStatement;
import com.meiya.whalex.db.module.document.statement.RenameCollectionStatement;
import com.mongodb.BasicDBObject;

import java.util.List;
import java.util.Queue;

public class RenameCollectionParser extends AbstractParser {


    public MongoStatement handler(Queue<String> queue, String statement, String collectionName) {

        RenameCollectionStatement renameCollectionStatement = new RenameCollectionStatement(collectionName, statement);

        List<String> methodParams = getMethodParams(queue, statement);
        int size = methodParams.size();
        switch (size) {
            case 2:
                renameCollectionStatement.setDropTarget(Boolean.valueOf(methodParams.get(1)));
            case 1:
                renameCollectionStatement.setTarget(getValue(methodParams.get(0)));
                break;
            default:
                throw new RuntimeException("renameCollection方法参数个数异常["+statement+"]");
        }

        if(!queue.isEmpty()) {
            throw new RuntimeException("未知的指令: " + statement);
        }

        return renameCollectionStatement;
    }





}
