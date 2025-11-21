package com.meiya.whalex.sql2dsl.parser;

import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.Assert;
import org.junit.Test;

public class DropTableSqlToDslParserTest {

    @Test
    public void dropTableTest() throws SqlParseException {

        String sql = "drop table test";
        DropTableSqlToDslParser parser = new DropTableSqlToDslParser(sql);
        parser.handle();
        Assert.assertEquals("test", parser.getTableName());
    }
}
