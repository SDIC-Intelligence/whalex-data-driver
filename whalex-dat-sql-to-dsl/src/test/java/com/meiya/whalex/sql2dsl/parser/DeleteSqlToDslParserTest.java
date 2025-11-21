package com.meiya.whalex.sql2dsl.parser;

import com.meiya.whalex.db.entity.DelParamCondition;
import com.meiya.whalex.interior.db.search.condition.Rel;
import com.meiya.whalex.interior.db.search.in.Where;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class DeleteSqlToDslParserTest {

    @Test
    public void deleteTest() throws SqlParseException {
        String sql = "delete from test where id = 1";
        DeleteSqlToDslParser parser = new DeleteSqlToDslParser(sql, new Object[0]);
        DelParamCondition condition = parser.handle();
        Assert.assertEquals("test", parser.getTableName());
        List<Where> whereList = condition.getWhere();
        Where where = whereList.get(0);
        Assert.assertEquals("id", where.getField());
        Assert.assertEquals(Rel.EQ, where.getType());
        Assert.assertEquals("1", where.getParam().toString());

        sql = "delete from test where id = ?";
        parser = new DeleteSqlToDslParser(sql, new Object[]{1});
        condition = parser.handle();
        Assert.assertEquals("test", parser.getTableName());
        whereList = condition.getWhere();
        where = whereList.get(0);
        Assert.assertEquals("id", where.getField());
        Assert.assertEquals(Rel.EQ, where.getType());
        Assert.assertEquals("1", where.getParam().toString());
    }

}
