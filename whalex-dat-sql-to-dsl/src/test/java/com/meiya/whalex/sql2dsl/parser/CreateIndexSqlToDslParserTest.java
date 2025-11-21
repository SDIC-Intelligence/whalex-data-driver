package com.meiya.whalex.sql2dsl.parser;

import com.meiya.whalex.db.constant.IndexType;
import com.meiya.whalex.db.entity.IndexParamCondition;
import com.meiya.whalex.interior.db.search.condition.Sort;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.Assert;
import org.junit.Test;

public class CreateIndexSqlToDslParserTest {

    @Test
    public void createIndexTest() throws SqlParseException {

        String sql = "create index age_index on test(age)";

        CreateIndexSqlToDslParser parser = new CreateIndexSqlToDslParser(sql);
        IndexParamCondition condition = parser.handle();

        Assert.assertEquals("age_index", condition.getIndexName());

        Assert.assertEquals("test", parser.getTableName());

        Assert.assertEquals("age", condition.getColumns().get(0).getColumn());
    }

    @Test
    public void createIndexTestForUnique() throws SqlParseException {

        String sql = "create unique index age_index on test(age)";

        CreateIndexSqlToDslParser parser = new CreateIndexSqlToDslParser(sql);
        IndexParamCondition condition = parser.handle();

        Assert.assertEquals("age_index", condition.getIndexName());

        Assert.assertEquals(IndexType.UNIQUE, condition.getIndexType());

        Assert.assertEquals("test", parser.getTableName());

        Assert.assertEquals("age", condition.getColumns().get(0).getColumn());
    }

    @Test
    public void createIndexTestForSort() throws SqlParseException {

        String sql = "create unique index age_index on test(age asc, name desc)";

        CreateIndexSqlToDslParser parser = new CreateIndexSqlToDslParser(sql);
        IndexParamCondition condition = parser.handle();

        Assert.assertEquals(IndexType.UNIQUE, condition.getIndexType());

        Assert.assertEquals("age_index", condition.getIndexName());

        Assert.assertEquals("test", parser.getTableName());

        Assert.assertEquals("age", condition.getColumns().get(0).getColumn());

        Assert.assertEquals(Sort.ASC, condition.getColumns().get(0).getSort());

        Assert.assertEquals("name", condition.getColumns().get(1).getColumn());

        Assert.assertEquals(Sort.DESC, condition.getColumns().get(1).getSort());
    }

    @Test
    public void createIndexTestForSortOnly() throws SqlParseException {

        String sql = "create index age_index on test(age desc)";

        CreateIndexSqlToDslParser parser = new CreateIndexSqlToDslParser(sql);
        IndexParamCondition condition = parser.handle();

        Assert.assertEquals("age_index", condition.getIndexName());

        Assert.assertEquals("test", parser.getTableName());

        Assert.assertEquals("age", condition.getColumns().get(0).getColumn());

        Assert.assertEquals(Sort.DESC, condition.getColumns().get(0).getSort());
    }

    @Test
    public void createIndexTestForSortNoUnique() throws SqlParseException {

        String sql = "create index age_index on test(age desc, id desc)";

        CreateIndexSqlToDslParser parser = new CreateIndexSqlToDslParser(sql);
        IndexParamCondition condition = parser.handle();

        Assert.assertEquals("age_index", condition.getIndexName());

        Assert.assertEquals("test", parser.getTableName());

        Assert.assertEquals("age", condition.getColumns().get(0).getColumn());

        Assert.assertEquals(Sort.DESC, condition.getColumns().get(0).getSort());

        Assert.assertEquals("id", condition.getColumns().get(1).getColumn());

        Assert.assertEquals(Sort.DESC, condition.getColumns().get(1).getSort());
    }

    @Test
    public void createIndexTestForNoSort() throws SqlParseException {

        String sql = "create index `age_index` on `test`(`age`, `id`)";

        CreateIndexSqlToDslParser parser = new CreateIndexSqlToDslParser(sql);
        IndexParamCondition condition = parser.handle();

        Assert.assertEquals("age_index", condition.getIndexName());

        Assert.assertEquals("test", parser.getTableName());

        Assert.assertEquals("age", condition.getColumns().get(0).getColumn());

        Assert.assertEquals("id", condition.getColumns().get(1).getColumn());
    }
}
