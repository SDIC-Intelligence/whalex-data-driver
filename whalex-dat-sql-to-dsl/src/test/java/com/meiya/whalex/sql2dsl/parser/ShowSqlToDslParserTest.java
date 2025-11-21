package com.meiya.whalex.sql2dsl.parser;

import com.meiya.whalex.interior.db.operation.in.QueryDatabasesCondition;
import com.meiya.whalex.interior.db.operation.in.QueryTablesCondition;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.Assert;
import org.junit.Test;

public class ShowSqlToDslParserTest {

    @Test
    public void showTableLikeTest() throws SqlParseException {

        String sql = "show tables like 'test'";
        ShowSqlToDslParser<QueryTablesCondition> parser = new ShowSqlToDslParser<>(sql);
        QueryTablesCondition condition = parser.handle();
        Assert.assertEquals("test", condition.getTableMatch());

    }

    @Test
    public void showTableTest() throws SqlParseException {
        String sql = "show tables";
        ShowSqlToDslParser<QueryTablesCondition> parser = new ShowSqlToDslParser<>(sql);
        QueryTablesCondition condition = parser.handle();
        Assert.assertNull(condition.getTableMatch());
    }

    @Test
    public void showTableInDBTest() throws SqlParseException {
        String sql = "show tables in database";
        ShowSqlToDslParser<QueryTablesCondition> parser = new ShowSqlToDslParser<>(sql);
        QueryTablesCondition condition = parser.handle();
        Assert.assertNull(condition.getTableMatch());
        Assert.assertEquals("database", condition.getDatabaseName());
    }

    @Test
    public void showTableInDBLikeTest() throws SqlParseException {
        String sql = "show tables in `database` 'test'";
        ShowSqlToDslParser<QueryTablesCondition> parser = new ShowSqlToDslParser<>(sql);
        QueryTablesCondition condition = parser.handle();
        Assert.assertEquals("database", condition.getDatabaseName());
        Assert.assertEquals("test", condition.getTableMatch());
    }

    @Test
    public void showDatabaseLikeTest() throws SqlParseException {
        String sql = "show databases like 'test'";
        ShowSqlToDslParser<QueryDatabasesCondition> parser = new ShowSqlToDslParser<>(sql);
        QueryDatabasesCondition condition = parser.handle();
        Assert.assertEquals("test", condition.getDatabaseMatch());
    }

    @Test
    public void showDatabaseTest() throws SqlParseException {
        String sql = "show databases";
        ShowSqlToDslParser<QueryDatabasesCondition> parser = new ShowSqlToDslParser<>(sql);
        QueryDatabasesCondition condition = parser.handle();
        Assert.assertNull(condition.getDatabaseMatch());
    }

    @Test
    public void showIndex() throws SqlParseException {
        String sql = "show index from table";
        ShowSqlToDslParser parser = new ShowSqlToDslParser<>(sql);
        parser.handle();
        Assert.assertEquals("table", parser.getTableName());
    }
}
