package com.meiya.whalex.sql2dsl.parser;

import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.commons.lang.StringUtils;

/**
 * sql转dsl
 *
 * @author 蔡荣桂
 * @date 2023/02/02
 * @project whalex-dat-sql
 */
public class DescTableSqlToDslParser implements ISqlToDslParser {

    private String sql;

    private String tableName;

    public DescTableSqlToDslParser(String sql) {
        this.sql = sql;
    }

    @Override
    public Object handle() throws SqlParseException {

        String[] split = sql.split("\\s+");

        if(!split[0].equalsIgnoreCase("desc")) {
            throw new RuntimeException("不是desc类型的sql");
        }

        if(split.length != 2) {
            throw new RuntimeException("不是desc类型的sql");
        }

        tableName = split[1];

        if(StringUtils.isBlank(tableName)) {
            throw new RuntimeException("表名不能为空");
        }

        return null;
    }

    @Override
    public String getTableName() {
        return tableName;
    }

}
