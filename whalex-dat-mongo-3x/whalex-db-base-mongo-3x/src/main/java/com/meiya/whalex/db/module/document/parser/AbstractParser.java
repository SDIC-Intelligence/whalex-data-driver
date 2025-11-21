package com.meiya.whalex.db.module.document.parser;

import com.meiya.whalex.db.module.document.statement.MongoStatement;
import com.mongodb.BasicDBObject;
import com.mongodb.client.model.Collation;
import com.mongodb.client.model.CollationAlternate;
import com.mongodb.client.model.CollationCaseFirst;
import com.mongodb.client.model.CollationMaxVariable;
import com.mongodb.client.model.CollationStrength;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

public abstract class AbstractParser {

    protected Collation buildCollation(BasicDBObject basicDBObject) {
        if(basicDBObject == null) {
            return null;
        }

        Object locale = basicDBObject.get("locale");
        Object caseLevel = basicDBObject.get("caseLevel");
        Object caseFirst = basicDBObject.get("caseFirst");
        Object strength = basicDBObject.get("strength");
        Object numericOrdering = basicDBObject.get("numericOrdering");
        Object alternate = basicDBObject.get("alternate");
        Object maxVariable = basicDBObject.get("maxVariable");
        Object normalization = basicDBObject.get("normalization");
        Object backwards = basicDBObject.get("backwards");

        Collation.Builder builder = Collation.builder();
        if(locale != null) {
            builder.locale(locale.toString());
        }
        if(caseLevel != null) {
            builder.caseLevel(Boolean.valueOf(caseLevel.toString()));
        }
        if(caseFirst != null) {
            builder.collationCaseFirst(CollationCaseFirst.fromString(caseFirst.toString()));
        }
        if(strength != null) {
            builder.collationStrength(CollationStrength.fromInt(Integer.parseInt(strength.toString())));
        }
        if(numericOrdering != null) {
            builder.numericOrdering(Boolean.valueOf(numericOrdering.toString()));
        }
        if(alternate != null) {
            builder.collationAlternate(CollationAlternate.fromString(alternate.toString()));
        }
        if(maxVariable != null) {
            builder.collationMaxVariable(CollationMaxVariable.fromString(maxVariable.toString()));
        }
        if(normalization != null) {
            builder.normalization(Boolean.valueOf(normalization.toString()));
        }
        if(backwards != null) {
            builder.backwards(Boolean.valueOf(backwards.toString()));
        }

        return builder.build();
    }

    private boolean isEnd(char c) {
        if(c == ',') {
            return true;
        }
        return false;
    }

    protected List<String> strToList(String arrayJsonStr) {

        //去掉最外层的[]
        char[] chars = arrayJsonStr.substring(1, arrayJsonStr.length() - 1).toCharArray();
        int bigCounter = 0;
        int counter = 0;
        List<String> list  = new ArrayList<>();
        StringBuilder itemBuilder = new StringBuilder();
        for (char aChar : chars) {
            //判断是不是元素结尾
            if(isEnd(aChar) && bigCounter == 0 && counter == 0) {
                if(itemBuilder.length() > 0){
                    list.add(itemBuilder.toString());
                    itemBuilder.setLength(0);
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
            list.add(itemBuilder.toString());
        }

        return list;
    }

    protected List<String> getMethodParams(Queue<String> queue, String statement) {
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

    protected String getValue(String value) {
        if(value.startsWith("'") && value.endsWith("'")) {
            return value.substring(1,value.length() - 1);
        }
        if(value.startsWith("\"") && value.endsWith("\"")) {
            return value.substring(1,value.length() - 1);
        }
        throw new RuntimeException(value + "不应是变量");
    }

    public abstract MongoStatement handler(Queue<String> queue, String statement, String collectionName);

}
