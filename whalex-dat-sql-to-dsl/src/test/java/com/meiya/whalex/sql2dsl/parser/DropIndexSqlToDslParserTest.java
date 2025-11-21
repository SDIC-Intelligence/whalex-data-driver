package com.meiya.whalex.sql2dsl.parser;

import com.meiya.whalex.db.entity.IndexParamCondition;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.Assert;
import org.junit.Test;

public class DropIndexSqlToDslParserTest {

    @Test
    public void  dropIndexTest() throws SqlParseException {

        String sql = "drop index test_id on test";
        DropIndexSqlToDslParser parser = new DropIndexSqlToDslParser(sql);
        IndexParamCondition condition = parser.handle();
        Assert.assertEquals("test", parser.getTableName());
        Assert.assertEquals("test_id", condition.getIndexName());


    }
}
