package com.meiya.whalex.sql2dsl.parser;

import com.meiya.whalex.db.entity.IndexParamCondition;
import com.meiya.whalex.interior.db.constant.ItemFieldTypeEnum;
import com.meiya.whalex.interior.db.operation.in.AlterTableParamCondition;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class AlterTableSqlToDslParserTest {

    @Test
    public void addColumnTest() throws SqlParseException {

        String sql = "alter table test add column age int(3) default 10 not null";
        AlterTableSqlToDslParser parser = new AlterTableSqlToDslParser(sql);
        AlterTableParamCondition condition = (AlterTableParamCondition) parser.handle();
        String tableName = parser.getTableName();
        Assert.assertEquals("test", tableName);
        List<AlterTableParamCondition.AddTableFieldParam> addTableFieldParamList = condition.getAddTableFieldParamList();
        AlterTableParamCondition.AddTableFieldParam fieldParam = addTableFieldParamList.get(0);
        String defaultValue = fieldParam.getDefaultValue();
        Assert.assertEquals("10", defaultValue);
        Boolean notNull = fieldParam.getNotNull();
        Assert.assertEquals(true, notNull);

        sql = "alter table pulbic.test add column age int(3) default 10 not null";
        parser = new AlterTableSqlToDslParser(sql);
        condition = (AlterTableParamCondition) parser.handle();
        tableName = parser.getTableName();
        Assert.assertEquals("test", tableName);
        addTableFieldParamList = condition.getAddTableFieldParamList();
        fieldParam = addTableFieldParamList.get(0);
        defaultValue = fieldParam.getDefaultValue();
        Assert.assertEquals("10", defaultValue);
        notNull = fieldParam.getNotNull();
        Assert.assertEquals(true, notNull);

        sql = "alter table test add column array varchar(100)[] default null";
        parser = new AlterTableSqlToDslParser(sql);
        condition = (AlterTableParamCondition) parser.handle();
        tableName = parser.getTableName();
        Assert.assertEquals("test", tableName);
        addTableFieldParamList = condition.getAddTableFieldParamList();
        fieldParam = addTableFieldParamList.get(0);
        String fieldType = fieldParam.getFieldType();
        Assert.assertEquals(ItemFieldTypeEnum.ARRAY_STRING.getVal(), fieldType);
        Assert.assertEquals("100", fieldParam.getFieldLength().toString());
        defaultValue = fieldParam.getDefaultValue();
        Assert.assertEquals("null", defaultValue);
        notNull = fieldParam.getNotNull();
        Assert.assertEquals(false, notNull);
    }


    @Test
    public void modifyColumnTest() throws SqlParseException {
        String sql = "alter table test modify column content longblob";
        AlterTableSqlToDslParser parser = new AlterTableSqlToDslParser(sql);
        AlterTableParamCondition condition = (AlterTableParamCondition) parser.handle();
        String tableName = parser.getTableName();
        Assert.assertEquals("test", tableName);
        List<AlterTableParamCondition.UpdateTableFieldParam> updateTableFieldParamList = condition.getUpdateTableFieldParamList();
        String field = updateTableFieldParamList.get(0).getFieldName();
        Assert.assertEquals("content", field);
    }

    @Test
    public void dropColumnTest() throws SqlParseException {
        String sql = "alter table test drop column name";
        AlterTableSqlToDslParser parser = new AlterTableSqlToDslParser(sql);
        AlterTableParamCondition condition = (AlterTableParamCondition) parser.handle();
        String tableName = parser.getTableName();
        Assert.assertEquals("test", tableName);
        List<String> delTableFieldParamList = condition.getDelTableFieldParamList();
        String field = delTableFieldParamList.get(0);
        Assert.assertEquals("name", field);
    }

    @Test
    public void alterColumn() throws SqlParseException {
        String sql = "alter table test alter column age type int(3) default 10 not null";
        AlterTableSqlToDslParser parser = new AlterTableSqlToDslParser(sql);
        AlterTableParamCondition condition = (AlterTableParamCondition) parser.handle();
        String tableName = parser.getTableName();
        Assert.assertEquals("test", tableName);
        List<AlterTableParamCondition.UpdateTableFieldParam> updateTableFieldParamList = condition.getUpdateTableFieldParamList();
        AlterTableParamCondition.UpdateTableFieldParam fieldParam = updateTableFieldParamList.get(0);
        Assert.assertEquals("age", fieldParam.getFieldName());
        Assert.assertEquals(3L, (long)fieldParam.getFieldLength());
        Assert.assertEquals("10", fieldParam.getDefaultValue());
        Assert.assertEquals(true, fieldParam.getNotNull());

        sql = "alter table test alter column age set not null";
        parser = new AlterTableSqlToDslParser(sql);
        condition = (AlterTableParamCondition) parser.handle();
        tableName = parser.getTableName();
        Assert.assertEquals("test", tableName);
        updateTableFieldParamList = condition.getUpdateTableFieldParamList();
        fieldParam = updateTableFieldParamList.get(0);
        Assert.assertEquals("age", fieldParam.getFieldName());
        Assert.assertEquals(true, fieldParam.getNotNull());

        sql = "alter table test alter column age drop not null";
        parser = new AlterTableSqlToDslParser(sql);
        condition = (AlterTableParamCondition) parser.handle();
        tableName = parser.getTableName();
        Assert.assertEquals("test", tableName);
        updateTableFieldParamList = condition.getUpdateTableFieldParamList();
        fieldParam = updateTableFieldParamList.get(0);
        Assert.assertEquals("age", fieldParam.getFieldName());
        Assert.assertEquals(false, fieldParam.getNotNull());
    }

    @Test
    public void renameTest() throws SqlParseException {
        String sql = "alter table test rename to test2";
        AlterTableSqlToDslParser parser = new AlterTableSqlToDslParser(sql);
        AlterTableParamCondition condition = (AlterTableParamCondition) parser.handle();
        String tableName = parser.getTableName();
        Assert.assertEquals("test", tableName);
        String newTableName = condition.getNewTableName();
        Assert.assertEquals("test2", newTableName);

        sql = "alter table test rename column age to age2";
        parser = new AlterTableSqlToDslParser(sql);
        condition = (AlterTableParamCondition) parser.handle();
        tableName = parser.getTableName();
        Assert.assertEquals("test", tableName);
        List<AlterTableParamCondition.UpdateTableFieldParam> updateTableFieldParamList = condition.getUpdateTableFieldParamList();
        AlterTableParamCondition.UpdateTableFieldParam fieldParam = updateTableFieldParamList.get(0);
        Assert.assertEquals("age2", fieldParam.getNewFieldName());
        Assert.assertEquals("age", fieldParam.getFieldName());
    }

    @Test
    public void addIndexTest() throws SqlParseException {
        String sql = "alter table test ADD INDEX age_index(age)";
        AlterTableSqlToDslParser parser = new AlterTableSqlToDslParser(sql);
        IndexParamCondition condition = (IndexParamCondition) parser.handle();
        String tableName = parser.getTableName();
        Assert.assertEquals("test", tableName);
        String indexName = condition.getIndexName();
        Assert.assertEquals("age_index", indexName);
        String column = condition.getColumns().get(0).getColumn();
        Assert.assertEquals("age", column);
    }


}
