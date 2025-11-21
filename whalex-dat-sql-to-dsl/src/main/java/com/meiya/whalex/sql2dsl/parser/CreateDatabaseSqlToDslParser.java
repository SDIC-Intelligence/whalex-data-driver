package com.meiya.whalex.sql2dsl.parser;

import com.meiya.whalex.db.builder.CreateDatabaseBuilder;
import com.meiya.whalex.interior.db.operation.in.CreateDatabaseParamCondition;
import org.apache.calcite.sql.parser.SqlParseException;

import java.util.Queue;

/**
 * sql转dsl
 *
 * @author 蔡荣桂
 * @date 2023/02/02
 * @project whalex-dat-sql
 */
public class CreateDatabaseSqlToDslParser extends AbstractSqlToDslParser<CreateDatabaseParamCondition> {

    private String sql;

    public CreateDatabaseSqlToDslParser(String sql) {
        this.sql = sql;
    }

    @Override
    public CreateDatabaseParamCondition handle() throws SqlParseException {

        CreateDatabaseBuilder builder = CreateDatabaseBuilder.builder();

        Queue<String> queue = parserWord(sql.toCharArray());


        if (!queue.poll().equalsIgnoreCase("create")
                || !queue.poll().equalsIgnoreCase("database")) {
            throw new RuntimeException("不是create database类型的sql: " + sql);
        }

        String dbName = removeQuoting(queue.poll());
        builder.dbName(dbName);

        if(!queue.isEmpty()) {
            // 【default】 CHARACTER SET

            if(queue.element().equalsIgnoreCase("default")) {
                queue.poll();
            }

            String character = queue.poll();
            String set = queue.poll();
            if(!character.equalsIgnoreCase("CHARACTER")
                    || !set.equalsIgnoreCase("SET")) {
                throw new RuntimeException("未知的sql:" + sql + ", 无法识别关键字[" + character + " " + set + "], " +
                        "支持关键字[[DEFAULT] CHARACTER SET]");
            }

            builder.character(queue.poll());
        }

        if(!queue.isEmpty()) {
            // COLLATE
            String word = queue.poll();
            if(!word.equalsIgnoreCase("COLLATE")) {
                throw new RuntimeException("未知的sql:" + sql + ", 无法识别关键字[" + word + "], " +
                        "支持关键字[COLLATE]");
            }

            builder.collate(queue.poll());
        }

        return builder.build();
    }

    @Override
    public String getTableName() {
        return null;
    }

}
