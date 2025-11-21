package com.meiya.whalex.sql2dsl.parser;

import com.meiya.whalex.db.builder.DropDatabaseBuilder;
import com.meiya.whalex.interior.db.operation.in.DropDatabaseParamCondition;
import org.apache.calcite.sql.parser.SqlParseException;

import java.util.Queue;

/**
 * sql转dsl
 *
 * @author 蔡荣桂
 * @date 2023/02/02
 * @project whalex-dat-sql
 */
public class DropDatabaseSqlToDslParser extends AbstractSqlToDslParser<DropDatabaseParamCondition> {

    private String sql;

    public DropDatabaseSqlToDslParser(String sql) {
        this.sql = sql;
    }

    @Override
    public DropDatabaseParamCondition handle() throws SqlParseException {

        DropDatabaseBuilder builder = DropDatabaseBuilder.builder();

        Queue<String> queue = parserWord(sql.toCharArray());


        if (!queue.poll().equalsIgnoreCase("drop")
                || !queue.poll().equalsIgnoreCase("database")) {
            throw new RuntimeException("不是drop database类型的sql: " + sql);
        }

        String dbName = removeQuoting(queue.poll());
        builder.dbName(dbName);

        return builder.build();
    }

    @Override
    public String getTableName() {
        return null;
    }

}
