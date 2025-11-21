package com.meiya.whalex.sql2dsl.parser;

import org.apache.commons.lang3.StringUtils;

import java.util.LinkedList;
import java.util.Queue;

/**
 * sql转dsl
 *
 * @author 蔡荣桂
 * @date 2023/02/02
 * @project whalex-dat-sql
 */
public abstract class AbstractSqlToDslParser<T> implements ISqlToDslParser<T>{

    /**
     * 非关系型数据库，占位符处理
     *
     * @param sql
     * @return
     */
    protected String getSqlConvertPlaceholders(String sql) {
        return wordQueueToSql(sqlConvertPlaceholders(parserWord(sql.toCharArray())));
    }

    /**
     * 占位符处理
     */
    protected Queue<String> sqlConvertPlaceholders(Queue<String> queue) {
        Queue<String> convertQueue = new LinkedList<>();
        int counter = 0;
        while (!queue.isEmpty()) {
            String poll = queue.poll();
            if (StringUtils.equalsIgnoreCase(poll, "?")) {
                convertQueue.offer("'" + poll + ++counter + "'");
            } else {
                convertQueue.offer(poll);
            }
        }
        return convertQueue;
    }

    /**
     * SQL 还原
     *
     * @param queue
     * @return
     */
    protected String wordQueueToSql(Queue<String> queue) {
        StringBuilder sqlSb = new StringBuilder();
        while (!queue.isEmpty()) {
            String poll = queue.poll();
            if (poll.length() == 1 && isNeedOfferQueue(poll.toCharArray()[0])) {
                sqlSb.append(poll);
            } else {
                if (sqlSb.length() > 0) {
                    sqlSb.append(" ");
                }
                sqlSb.append(poll);
            }
        }
        return sqlSb.toString();
    }

    protected Queue<String> parserWord(char[] chars) {
        int index = 0;

        StringBuilder word = new StringBuilder();

        //词队列
        Queue<String> queue = new LinkedList<>();

        //是否可以下一个词
        boolean canNext = true;

        //转义字符处理
        boolean isEscape = false;

        while (index < chars.length) {

            //当前word是否完整了
            if (isNext(chars[index]) && canNext) {

                if (word.length() > 0) {
                    queue.offer(word.toString());
                }
                //置空
                word.setLength(0);

                //当前字符是否需要加入队列当中
                if (isNeedOfferQueue(chars[index])) {
                    queue.offer(chars[index] + "");
                }

            } else {

                //'' 内的字符串是一起的
                if(chars[index] == '\'' && !isEscape) {
                    canNext = !canNext;
                }

                //转义字符处理
                if(!isEscape && chars[index] == '\\') {
                    isEscape = true;
                }else{
                    isEscape = false;
                    word.append(chars[index]);
                }


            }

            index++;
        }

        if (word.length() > 0) {

            queue.offer(word.toString());
        }
        return queue;
    }

    private boolean isNext(char c) {

        if (c == ' '
                || c == '('
                || c == ')'
                || c == ','
                || c == '\r'
                || c == '\n'
                || c == '='
                || c == '\t'
                || c == ';') {
            return true;
        }

        return false;
    }

    private boolean isNeedOfferQueue(char c) {

        if (c == '(' || c == ')' || c == ',' || c == '=') {
            return true;
        }
        return false;
    }

    protected String removeStartEndStr(String str, String key) {
        if (str.startsWith(key) && str.endsWith(key)) {
            str = str.substring(key.length(), str.length() - key.length());
        }
        return str;
    }

    protected String removeQuoting(String key) {
        return removeStartEndStr(key, "`");
    }


    protected String removeApostrophe(String key) {
        return removeStartEndStr(key, "'");
    }

    protected String removeDoubleQuotationMarks(String key) {
        return removeStartEndStr(key, "\"");
    }

    protected void throwParseException(String sql, String parseContent, String unKnowKey, String knowKey) {
        throw new RuntimeException("未知的sql:" + sql + ", 解析" + "， 无法识别关键字[" + unKnowKey + "], " +
                "支持关键字[" + knowKey + "]");
    }
}
