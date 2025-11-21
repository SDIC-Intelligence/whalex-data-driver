package com.meiya.whalex.sql2dsl.parser;

import com.meiya.whalex.db.entity.UpdateParamCondition;
import com.meiya.whalex.interior.db.search.condition.Rel;
import com.meiya.whalex.interior.db.search.in.Where;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class UpdateSqlToDslParserTest {
    
    @Test
    public void updateTest() throws SqlParseException {
        
        String sql = "update test set age = 1 where id = 1";
        UpdateSqlToDslParser parser = new UpdateSqlToDslParser(sql, new Object[0]);
        UpdateParamCondition condition = parser.handle();
        Assert.assertEquals("test", parser.getTableName());
        List<Where> whereList = condition.getWhere();
        Where where = whereList.get(0);
        Assert.assertEquals("id", where.getField());
        Assert.assertEquals(Rel.EQ, where.getType());
        Assert.assertEquals("1", where.getParam().toString());

    }
}
