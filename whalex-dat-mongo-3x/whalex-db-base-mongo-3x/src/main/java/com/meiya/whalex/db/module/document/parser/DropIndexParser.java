package com.meiya.whalex.db.module.document.parser;

import com.meiya.whalex.db.module.document.statement.DropIndexStatement;
import com.meiya.whalex.db.module.document.statement.MongoStatement;
import com.mongodb.BasicDBObject;

import java.util.List;
import java.util.Queue;

public class DropIndexParser extends AbstractParser {


    public MongoStatement handler(Queue<String> queue, String statement, String collectionName) {

        DropIndexStatement dropIndexStatement = new DropIndexStatement(collectionName, statement);

        List<String> methodParams = getMethodParams(queue, statement);
        int size = methodParams.size();
        if(size != 1) {
            throw new RuntimeException("dropIndex方法参数个数异常["+statement+"]");
        }

        String param = methodParams.get(0);
        if(param.startsWith("{")) {
            dropIndexStatement.setIndex(BasicDBObject.parse(param));
        }else {
            dropIndexStatement.setIndex(getValue(param));
        }

        if(!queue.isEmpty()) {
            throw new RuntimeException("未知的指令: " + statement);
        }

        return dropIndexStatement;
    }


}
