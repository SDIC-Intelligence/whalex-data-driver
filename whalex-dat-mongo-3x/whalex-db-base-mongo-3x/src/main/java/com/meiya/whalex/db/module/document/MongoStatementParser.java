package com.meiya.whalex.db.module.document;

import com.meiya.whalex.db.module.document.parser.AbstractParser;
import com.meiya.whalex.db.module.document.parser.AggregateParser;
import com.meiya.whalex.db.module.document.parser.BulkWriteParser;
import com.meiya.whalex.db.module.document.parser.CountParser;
import com.meiya.whalex.db.module.document.parser.CreateCollectionParser;
import com.meiya.whalex.db.module.document.parser.CreateIndexParser;
import com.meiya.whalex.db.module.document.parser.CreateIndexesParser;
import com.meiya.whalex.db.module.document.parser.DeleteManyParser;
import com.meiya.whalex.db.module.document.parser.DeleteOneParser;
import com.meiya.whalex.db.module.document.parser.DistinctParser;
import com.meiya.whalex.db.module.document.parser.DropIndexParser;
import com.meiya.whalex.db.module.document.parser.DropIndexesParser;
import com.meiya.whalex.db.module.document.parser.DropParser;
import com.meiya.whalex.db.module.document.parser.FindAndModifyParser;
import com.meiya.whalex.db.module.document.parser.FindOneAndDeleteParser;
import com.meiya.whalex.db.module.document.parser.FindOneAndReplaceParser;
import com.meiya.whalex.db.module.document.parser.FindOneAndUpdateParser;
import com.meiya.whalex.db.module.document.parser.FindOneParser;
import com.meiya.whalex.db.module.document.parser.FindParser;
import com.meiya.whalex.db.module.document.parser.GetIndexesParser;
import com.meiya.whalex.db.module.document.parser.InsertManyParser;
import com.meiya.whalex.db.module.document.parser.InsertParser;
import com.meiya.whalex.db.module.document.parser.RenameCollectionParser;
import com.meiya.whalex.db.module.document.parser.ReplaceOneParser;
import com.meiya.whalex.db.module.document.parser.UpdateManyParser;
import com.meiya.whalex.db.module.document.parser.UpdateParser;
import com.meiya.whalex.db.module.document.statement.MongoStatement;
import com.meiya.whalex.db.module.document.statement.ShowCollectionsStatement;
import com.meiya.whalex.db.module.document.statement.ShowDbsStatement;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

public class MongoStatementParser {


    private Map<String, AbstractParser> parserMap;

    public MongoStatementParser() {
        parserMap = new HashMap<>();

        parserMap.put("renameCollection", new RenameCollectionParser());

        parserMap.put("insert", new InsertParser());
        parserMap.put("insertOne", new InsertParser());
        parserMap.put("insertMany", new InsertManyParser());

        parserMap.put("deleteOne", new DeleteOneParser());
        parserMap.put("deleteMany", new DeleteManyParser());
        parserMap.put("remove", new DeleteManyParser());

        parserMap.put("find", new FindParser());
        parserMap.put("findOne", new FindOneParser());
        parserMap.put("findAndModify", new FindAndModifyParser());
        parserMap.put("findOneAndDelete", new FindOneAndDeleteParser());
        parserMap.put("findOneAndUpdate", new FindOneAndUpdateParser());
        parserMap.put("findOneAndReplace", new FindOneAndReplaceParser());

        parserMap.put("distinct", new DistinctParser());

        parserMap.put("count", new CountParser());

        parserMap.put("update", new UpdateParser());
        parserMap.put("updateOne", new UpdateParser());
        parserMap.put("updateMany", new UpdateManyParser());

        parserMap.put("replaceOne", new ReplaceOneParser());

        parserMap.put("aggregate", new AggregateParser());

        parserMap.put("drop", new DropParser());

        parserMap.put("bulkWrite", new BulkWriteParser());

        parserMap.put("createIndex", new CreateIndexParser());
        parserMap.put("createIndexes", new CreateIndexesParser());

        parserMap.put("getIndexes", new GetIndexesParser());

        parserMap.put("dropIndexes", new DropIndexesParser());
        parserMap.put("dropIndex", new DropIndexParser());
    }

    private Queue<String> splitStatement(String statement) {
        //去空
        String trim = statement.trim();
        //去分号
        if(trim.endsWith(";")){
            trim = trim.substring(0, trim.length() - 1).trim();
        }
        char[] chars = trim.toCharArray();
        Queue<String> queue = new LinkedList<>();
        int bigCounter = 0;
        int counter = 0;
        boolean singleQuotation = false;
        boolean doubleQuotation = false;
        //元素包括 单词 （ ） json ,
        StringBuilder itemBuilder = new StringBuilder();
        for (char aChar : chars) {
            //判断是不是元素结尾
            if(aChar == '\'') {
                singleQuotation = !singleQuotation;
            }
            if(aChar == '"') {
                doubleQuotation = !doubleQuotation;
            }

            if(isEnd(aChar) && bigCounter == 0 && counter == 0 && !singleQuotation && !doubleQuotation) {
                if(itemBuilder.length() > 0){
                    queue.offer(itemBuilder.toString());
                    itemBuilder.setLength(0);
                }
                if(isNeedSave(aChar)) {
                    queue.offer(aChar + "");
                }
                continue;
            }
            if(aChar == '{') {
                bigCounter++;
            }else if(aChar == '}') {
                bigCounter--;
            }
            if(aChar == '[') {
                counter++;
            }else if(aChar == ']') {
                counter--;
            }
            itemBuilder.append(aChar);
        }

        if(itemBuilder.length() > 0) {
            queue.offer(itemBuilder.toString());
        }

        return queue;
    }

    public MongoStatement parse(String statement) {
        Queue<String> queue = splitStatement(statement);
        String word = queue.poll();
        if(word.equalsIgnoreCase("show")) {
           return parseShow(queue, statement);
        }else if(word.equalsIgnoreCase("db")){
            return parseDb(queue, statement);
        }

        throw new RuntimeException("未知的指令: " + statement);
    }

    private MongoStatement parseDb(Queue<String> queue, String statement) {
        //db.getCollectionNames();
        //db.getCollection('mongodb_raw_statement')
        //db.mongodb_raw_statement
        String word = queue.poll();
        String collectionName = word;
        if(word.equalsIgnoreCase("getCollectionNames")) {
            List<String> methodParams = getMethodParams(queue, statement);
            if(methodParams.isEmpty() && queue.isEmpty()) {
                return new ShowCollectionsStatement(statement);
            }
            throw new RuntimeException("未知的指令: " + statement);
        }else if(word.equalsIgnoreCase("createCollection")) {
            return new CreateCollectionParser().handler(queue, statement, null);
        }else if(word.equalsIgnoreCase("getCollection")) {
            List<String> methodParams = getMethodParams(queue, statement);
            if(methodParams.isEmpty()) {
                throw new RuntimeException("getCollection缺少必要的参数");
            }
            if(methodParams.size() != 1) {
                throw new RuntimeException("getCollection只有一个参数");
            }
            collectionName = getCollectionName(methodParams.get(0));
        }

        return parseCollection(queue, statement, collectionName);

    }

    private MongoStatement parseCollection(Queue<String> queue, String statement, String collectionName) {

        String operator = queue.poll();
        AbstractParser parser = parserMap.get(operator);
        if(parser == null) {
            throw new RuntimeException("未知的方法: " + operator + ", [" + statement + "]");
        }
        return parser.handler(queue, statement, collectionName);
    }

    private String getCollectionName(String value) {
        if(value.startsWith("'") && value.endsWith("'")) {
            return value.substring(1,value.length() - 1);
        }
        if(value.startsWith("\"") && value.endsWith("\"")) {
            return value.substring(1,value.length() - 1);
        }
        throw new RuntimeException("集合名称" + value + "不应是变量");
    }

    private List<String> getMethodParams(Queue<String> queue, String statement) {
        List<String> paramList = new ArrayList<>();
        queue.poll();
        while (!queue.isEmpty()) {
            String word = queue.poll();
            if(word.equalsIgnoreCase(")")) {
                return paramList;
            }
            paramList.add(word);
        }
        throw new RuntimeException("未知的指令: " + statement);
    }

    private MongoStatement parseShow(Queue<String> queue, String statement) {
        if(queue.size() != 1) {
            throw new RuntimeException("未知的指令: " + statement);
        }

        String word = queue.poll();
        if(word.equalsIgnoreCase("dbs")) {
            return new ShowDbsStatement(statement);
        }else if(word.equalsIgnoreCase("collections")) {
            return new ShowCollectionsStatement(statement);
        }

        throw new RuntimeException("未知的指令: " + statement);
    }

    private boolean isNeedSave(char c) {
        if(c == '(' || c == ')') {
            return true;
        }
        return false;
    }

    private boolean isEnd(char c) {
        if(c == '.' || c == ' ' || c == ',' || c == '(' || c == ')' || c == '\n') {
            return true;
        }
        return false;
    }
}
