package com.meiya.whalex.sql2dsl.parser;

import com.meiya.whalex.interior.db.operation.in.AlterTableParamCondition;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class CommentSqlToDslParserTest {

    @Test
    public void commentTest() throws SqlParseException {

        String sql = "comment on column public.test.name is '姓名'";

        CommentSqlToDslParser parser = new CommentSqlToDslParser(sql);
        AlterTableParamCondition condition = parser.handle();

        Assert.assertEquals("test", parser.getTableName());

        List<AlterTableParamCondition.UpdateTableFieldParam> fieldParamList = condition.getUpdateTableFieldParamList();
        AlterTableParamCondition.UpdateTableFieldParam fieldParam = fieldParamList.get(0);
        String fieldComment = fieldParam.getFieldComment();
        String fieldName = fieldParam.getFieldName();

        Assert.assertEquals("name", fieldName);
        Assert.assertEquals("姓名", fieldComment);

    }

    @Test
    public void commentTest2() throws SqlParseException {

        String sql = "comment on column test.name is '姓名'";

        CommentSqlToDslParser parser = new CommentSqlToDslParser(sql);
        AlterTableParamCondition condition = parser.handle();

        Assert.assertEquals("test", parser.getTableName());

        List<AlterTableParamCondition.UpdateTableFieldParam> fieldParamList = condition.getUpdateTableFieldParamList();
        AlterTableParamCondition.UpdateTableFieldParam fieldParam = fieldParamList.get(0);
        String fieldComment = fieldParam.getFieldComment();
        String fieldName = fieldParam.getFieldName();

        Assert.assertEquals("name", fieldName);
        Assert.assertEquals("姓名", fieldComment);

    }

}
