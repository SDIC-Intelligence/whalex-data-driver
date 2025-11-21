package com.meiya.whalex.sql2dsl.parser;

import com.meiya.whalex.db.entity.AddParamCondition;
import com.meiya.whalex.db.entity.UpsertParamBatchCondition;
import com.meiya.whalex.db.entity.UpsertParamCondition;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class InsertSqlToDslParserTest {

    @Test
    public void insertTest() throws SqlParseException {
        String sql = "INSERT INTO dat_table (id, name, age, email) VALUES (10, 'Jone', 18, 'test1@baomidou.com')";
        InsertSqlToDslParser<AddParamCondition> parser = new InsertSqlToDslParser<>(sql, new Object[0]);
        AddParamCondition condition = parser.handle();
        Assert.assertEquals("dat_table", parser.getTableName());
        List<Map<String, Object>> fieldValueList = condition.getFieldValueList();
        Map<String, Object> map = fieldValueList.get(0);
        Assert.assertEquals(10L, map.get("id"));
        Assert.assertEquals("Jone", map.get("name"));
        Assert.assertEquals(18L, map.get("age"));
        Assert.assertEquals("test1@baomidou.com", map.get("email"));
    }

    @Test
    public void insertBatchTest() throws SqlParseException {
        String sql = "INSERT INTO dat_table (id, name, age, email) VALUES (10, 'Jone', 18, 'test1@baomidou.com'),(11, 'test', 18, 'test@baomidou.com')";
        InsertSqlToDslParser<AddParamCondition> parser = new InsertSqlToDslParser<>(sql, new Object[0]);
        AddParamCondition condition = parser.handle();
        Assert.assertEquals("dat_table", parser.getTableName());
        List<Map<String, Object>> fieldValueList = condition.getFieldValueList();
        Map<String, Object> map = fieldValueList.get(0);
        Assert.assertEquals(10L, map.get("id"));
        Assert.assertEquals("Jone", map.get("name"));
        Assert.assertEquals(18L, map.get("age"));
        Assert.assertEquals("test1@baomidou.com", map.get("email"));

        map = fieldValueList.get(1);
        Assert.assertEquals(11L, map.get("id"));
        Assert.assertEquals("test", map.get("name"));
        Assert.assertEquals(18L, map.get("age"));
        Assert.assertEquals("test@baomidou.com", map.get("email"));
    }

    @Test
    public void upsertTest() throws SqlParseException {
        String sql = "insert into dat_table(id, name, age) values(1, 'test', 20) on conflict(id) do update set name = 'aaaa', age = 21";
        InsertSqlToDslParser<UpsertParamCondition> parser = new InsertSqlToDslParser<>(sql, new Object[0]);
        UpsertParamCondition condition = parser.handle();
        Assert.assertEquals("dat_table", parser.getTableName());
        List<String> conflictFieldList = condition.getConflictFieldList();
        Assert.assertEquals("id", conflictFieldList.get(0));
        Map<String, Object> updateParamMap = condition.getUpdateParamMap();
        Assert.assertEquals("aaaa", updateParamMap.get("name"));
        Assert.assertEquals(21L, updateParamMap.get("age"));
        Map<String, Object> insertParamMap = condition.getUpsertParamMap();
        Assert.assertEquals(1L, insertParamMap.get("id"));
        Assert.assertEquals("test", insertParamMap.get("name"));
        Assert.assertEquals(20L, insertParamMap.get("age"));
    }

    @Test
    public void upsertBatchTest() throws SqlParseException {
        String sql = "insert into dat_table(id, name, age) values(1, 'test', 19),(2, 'test', 19) on conflict(id) do update set name = values(name), age=values(age)";
        InsertSqlToDslParser<UpsertParamBatchCondition> parser = new InsertSqlToDslParser<>(sql, new Object[0]);
        UpsertParamBatchCondition condition = parser.handle();
        Assert.assertEquals("dat_table", parser.getTableName());
        List<String> conflictFieldList = condition.getConflictFieldList();
        Assert.assertEquals("id", conflictFieldList.get(0));
        List<String> updateKeys = condition.getUpdateKeys();
        Assert.assertEquals("name", updateKeys.get(0));
        Assert.assertEquals("age", updateKeys.get(1));

        List<Map<String, Object>> paramList = condition.getUpsertParamList();
        Map<String, Object> insertParamMap = paramList.get(0);
        Assert.assertEquals(1L, insertParamMap.get("id"));
        Assert.assertEquals("test", insertParamMap.get("name"));
        Assert.assertEquals(19L, insertParamMap.get("age"));

        insertParamMap = paramList.get(1);
        Assert.assertEquals(2L, insertParamMap.get("id"));
        Assert.assertEquals("test", insertParamMap.get("name"));
        Assert.assertEquals(19L, insertParamMap.get("age"));
    }
}
