package com.meiya.whalex.sql2dsl.parser;

import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.Assert;
import org.junit.Test;

public class DescTableSqlToDslParserTest {

    @Test
    public void descTest() throws SqlParseException {
        String sql = "desc test";
        DescTableSqlToDslParser parser = new DescTableSqlToDslParser(sql);
        parser.handle();
        String tableName = parser.getTableName();
        Assert.assertEquals("test", tableName);
    }
}
