package com.meiya.whalex.sql2dsl.parser;

import cn.hutool.core.collection.CollectionUtil;
import com.meiya.whalex.db.constant.IndexType;
import com.meiya.whalex.db.entity.IndexParamCondition;
import com.meiya.whalex.interior.db.constant.ItemFieldTypeEnum;
import com.meiya.whalex.interior.db.operation.in.CreateTableLikeParam;
import com.meiya.whalex.interior.db.operation.in.CreateTableParamCondition;
import com.meiya.whalex.interior.db.search.condition.Sort;
import com.meiya.whalex.sql2dsl.entity.SqlCreateTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class CreateTableSqlToDslParserTest {

    @Test
    public void commentTest() throws SqlParseException {
        String sql = "CREATE TABLE test (\n" +
                "                id int(11) NOT NULL AUTO_INCREMENT,\n" +
                "                real_Name varchar(255) DEFAULT NULL,\n" +
                "                city varchar(255) DEFAULT NULL,\n" +
                "                age int(11) unsigned DEFAULT NULL,\n" +
                "                count int(11) DEFAULT NULL,\n" +
                "                testUser varchar(255) DEFAULT NULL,\n" +
                "                testUsers varchar(255) DEFAULT NULL,\n" +
                "                codes varchar(255) DEFAULT NULL,\n" +
                "                codeStrs varchar(255) DEFAULT NULL,\n" +
                "                param_1 varchar(255) DEFAULT NULL,\n" +
                "                param_2 boolean DEFAULT NULL,\n" +
                "                param_3 bigint(10) DEFAULT NULL,\n" +
                "                param_4 json DEFAULT NULL,\n" +
                "                param_5 decimal(10,2) DEFAULT NULL,\n" +
                "                param_6 text[] DEFAULT NULL,\n" +
                "                param_7 varchar(100)[] DEFAULT NULL,\n" +
                "                param_8 tinyint DEFAULT NULL,\n" +
                "                param_9 smallint DEFAULT NULL,\n" +
                "                param_10 mediumint DEFAULT NULL,\n" +
                "                param_11 bigint DEFAULT NULL,\n" +
                "                param_12 real DEFAULT NULL,\n" +
                "                param_13 float DEFAULT NULL,\n" +
                "                param_14 double DEFAULT NULL,\n" +
                "                param_15 decimal DEFAULT NULL,\n" +
                "                param_16 numeric DEFAULT NULL,\n" +
                "                param_17 money DEFAULT NULL,\n" +
                "                param_18 bit DEFAULT NULL,\n" +
                "                param_19 year DEFAULT NULL,\n" +
                "                param_20 date DEFAULT NULL,\n" +
                "                param_21 datetime DEFAULT NULL,\n" +
                "                param_22 time DEFAULT NULL,\n" +
                "                param_23 timestamp DEFAULT NULL,\n" +
                "                param_24 char(5) DEFAULT NULL,\n" +
                "                param_25 tinytext DEFAULT NULL,\n" +
                "                param_26 text DEFAULT NULL,\n" +
                "                param_27 mediumtext DEFAULT NULL,\n" +
                "                param_28 longtext DEFAULT NULL,\n" +
                "                param_29 binary(10) DEFAULT NULL,\n" +
                "                param_30 varbinary(10) DEFAULT NULL,\n" +
                "                param_31 image DEFAULT NULL,\n" +
                "                param_32 blob DEFAULT NULL,\n" +
                "                param_33 tinyblob DEFAULT NULL,\n" +
                "                param_34 mediumblob DEFAULT NULL,\n" +
                "                param_35 longblob DEFAULT NULL,\n" +
                "                param_36 point DEFAULT NULL,\n" +
                "                param_37 tinyint[] DEFAULT NULL,\n" +
                "                PRIMARY KEY (id)\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8;";

        CreateTableSqlToDslParser parser = new CreateTableSqlToDslParser(sql);
        SqlCreateTable sqlCreateTable = parser.handle();

        CreateTableParamCondition condition = sqlCreateTable.getCreateTable();

        Assert.assertEquals("test", parser.getTableName());

        List<CreateTableParamCondition.CreateTableFieldParam> createTableFieldParamList = condition.getCreateTableFieldParamList();
        checkFieldParam(createTableFieldParamList.get(0), "id", 11, null, "Integer", false);
        checkFieldParam(createTableFieldParamList.get(1), "real_Name", 255, null, "String", false);
        checkFieldParam(createTableFieldParamList.get(2), "city", 255, null, "String", false);
        checkFieldParam(createTableFieldParamList.get(3), "age", 11, null, "Integer", true);
        checkFieldParam(createTableFieldParamList.get(4), "count", 11, null, "Integer", false);
        checkFieldParam(createTableFieldParamList.get(5), "testUser", 255, null, "String", false);
        checkFieldParam(createTableFieldParamList.get(6), "testUsers", 255, null, "String", false);
        checkFieldParam(createTableFieldParamList.get(7), "codes", 255, null, "String", false);
        checkFieldParam(createTableFieldParamList.get(8), "codeStrs", 255, null, "String", false);
        checkFieldParam(createTableFieldParamList.get(9), "param_1", 255, null, "String", false);
        checkFieldParam(createTableFieldParamList.get(10), "param_2", null, null, "Boolean", false);
        checkFieldParam(createTableFieldParamList.get(11), "param_3", 10, null, "Long", false);
        checkFieldParam(createTableFieldParamList.get(12), "param_4", null, null, "Object", false);
        checkFieldParam(createTableFieldParamList.get(13), "param_5", 10, 2, "Decimal", false);
        checkFieldParam(createTableFieldParamList.get(14), "param_6", null, null, "Array<Text>", false);
        checkFieldParam(createTableFieldParamList.get(15), "param_7", 100, null, ItemFieldTypeEnum.ARRAY_STRING.getVal(), false);
        checkFieldParam(createTableFieldParamList.get(16), "param_8", null, null, ItemFieldTypeEnum.TINYINT.getVal(), false);
        checkFieldParam(createTableFieldParamList.get(17), "param_9", null, null, ItemFieldTypeEnum.SMALLINT.getVal(), false);
        checkFieldParam(createTableFieldParamList.get(18), "param_10", null, null, ItemFieldTypeEnum.MEDIUMINT.getVal(), false);
        checkFieldParam(createTableFieldParamList.get(19), "param_11", null, null, ItemFieldTypeEnum.LONG.getVal(), false);
        checkFieldParam(createTableFieldParamList.get(20), "param_12", null, null, ItemFieldTypeEnum.REAL.getVal(), false);
        checkFieldParam(createTableFieldParamList.get(21), "param_13", null, null, ItemFieldTypeEnum.FLOAT.getVal(), false);
        checkFieldParam(createTableFieldParamList.get(22), "param_14", null, null, ItemFieldTypeEnum.DOUBLE.getVal(), false);
        checkFieldParam(createTableFieldParamList.get(23), "param_15", null, null, ItemFieldTypeEnum.DECIMAL.getVal(), false);
        checkFieldParam(createTableFieldParamList.get(24), "param_16", null, null, ItemFieldTypeEnum.NUMERIC.getVal(), false);
        checkFieldParam(createTableFieldParamList.get(25), "param_17", null, null, ItemFieldTypeEnum.MONEY.getVal(), false);
        checkFieldParam(createTableFieldParamList.get(26), "param_18", null, null, ItemFieldTypeEnum.BIT.getVal(), false);
        checkFieldParam(createTableFieldParamList.get(27), "param_19", null, null, ItemFieldTypeEnum.YEAR.getVal(), false);
        checkFieldParam(createTableFieldParamList.get(28), "param_20", null, null, ItemFieldTypeEnum.DATE.getVal(), false);
        checkFieldParam(createTableFieldParamList.get(29), "param_21", null, null, ItemFieldTypeEnum.DATETIME.getVal(), false);
        checkFieldParam(createTableFieldParamList.get(30), "param_22", null, null, ItemFieldTypeEnum.SMART_TIME.getVal(), false);
        checkFieldParam(createTableFieldParamList.get(31), "param_23", null, null, ItemFieldTypeEnum.TIMESTAMP.getVal(), false);
        checkFieldParam(createTableFieldParamList.get(32), "param_24", 5, null, ItemFieldTypeEnum.CHAR.getVal(), false);
        checkFieldParam(createTableFieldParamList.get(33), "param_25", null, null, ItemFieldTypeEnum.TINYTEXT.getVal(), false);
        checkFieldParam(createTableFieldParamList.get(34), "param_26", null, null, ItemFieldTypeEnum.TEXT.getVal(), false);
        checkFieldParam(createTableFieldParamList.get(35), "param_27", null, null, ItemFieldTypeEnum.MEDIUMTEXT.getVal(), false);
        checkFieldParam(createTableFieldParamList.get(36), "param_28", null, null, ItemFieldTypeEnum.LONGTEXT.getVal(), false);
        checkFieldParam(createTableFieldParamList.get(37), "param_29", 10, null, ItemFieldTypeEnum.BINARY.getVal(), false);
        checkFieldParam(createTableFieldParamList.get(38), "param_30", 10, null, ItemFieldTypeEnum.VARBINARY.getVal(), false);
        checkFieldParam(createTableFieldParamList.get(39), "param_31", null, null, ItemFieldTypeEnum.IMAGE.getVal(), false);
        checkFieldParam(createTableFieldParamList.get(40), "param_32", null, null, ItemFieldTypeEnum.BLOB.getVal(), false);
        checkFieldParam(createTableFieldParamList.get(41), "param_33", null, null, ItemFieldTypeEnum.TINYBLOB.getVal(), false);
        checkFieldParam(createTableFieldParamList.get(42), "param_34", null, null, ItemFieldTypeEnum.MEDIUMBLOB.getVal(), false);
        checkFieldParam(createTableFieldParamList.get(43), "param_35", null, null, ItemFieldTypeEnum.LONGBLOB.getVal(), false);
        checkFieldParam(createTableFieldParamList.get(44), "param_36", null, null, ItemFieldTypeEnum.POINT.getVal(), false);
        checkFieldParam(createTableFieldParamList.get(45), "param_37", null, null, ItemFieldTypeEnum.ARRAY_TINYINT.getVal(), false);
    }

    private void checkFieldParam(CreateTableParamCondition.CreateTableFieldParam fieldParam, String fieldName, Integer fieldLength, Integer decimalPoint, String fieldType, boolean unsigned) {
        Assert.assertEquals(fieldName, fieldParam.getFieldName());
        Assert.assertEquals(fieldLength, fieldParam.getFieldLength());
        Assert.assertEquals(decimalPoint, fieldParam.getFieldDecimalPoint());
        Assert.assertEquals(fieldType, fieldParam.getFieldType());
        Assert.assertEquals(unsigned, fieldParam.getUnsigned());
    }

    @Test
    public void createTableOnIndex() throws SqlParseException {
        String sql = "CREATE TABLE test (\n" +
                "                id int(11) NOT NULL AUTO_INCREMENT,\n" +
                "                real_Name varchar(255) DEFAULT NULL,\n" +
                "                city varchar(255) DEFAULT NULL,\n" +
                "                age int(11) unsigned DEFAULT NULL,\n" +
                "                count int(11) DEFAULT NULL,\n" +
                "                testUser varchar(255) DEFAULT NULL,\n" +
                "                testUsers varchar(255) DEFAULT NULL,\n" +
                "                codes varchar(255) DEFAULT NULL,\n" +
                "                codeStrs varchar(255) DEFAULT NULL,\n" +
                "                PRIMARY KEY (id),\n" +
                "                KEY(`app_code` desc,`unify_key`)\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8;";

        CreateTableSqlToDslParser parser = new CreateTableSqlToDslParser(sql);
        SqlCreateTable sqlCreateTable = parser.handle();

        CreateTableParamCondition condition = sqlCreateTable.getCreateTable();

        Assert.assertEquals("test", parser.getTableName());

        List<CreateTableParamCondition.CreateTableFieldParam> createTableFieldParamList = condition.getCreateTableFieldParamList();
        checkFieldParam(createTableFieldParamList.get(0), "id", 11, null, "Integer", false);
        checkFieldParam(createTableFieldParamList.get(1), "real_Name", 255, null, "String", false);
        checkFieldParam(createTableFieldParamList.get(2), "city", 255, null, "String", false);
        checkFieldParam(createTableFieldParamList.get(3), "age", 11, null, "Integer", true);
        checkFieldParam(createTableFieldParamList.get(4), "count", 11, null, "Integer", false);
        checkFieldParam(createTableFieldParamList.get(5), "testUser", 255, null, "String", false);
        checkFieldParam(createTableFieldParamList.get(6), "testUsers", 255, null, "String", false);
        checkFieldParam(createTableFieldParamList.get(7), "codes", 255, null, "String", false);
        checkFieldParam(createTableFieldParamList.get(8), "codeStrs", 255, null, "String", false);

        List<IndexParamCondition> createIndex = sqlCreateTable.getCreateIndex();

        Assert.assertEquals(createIndex.size(), 1);

        IndexParamCondition indexParamCondition = createIndex.get(0);

        checkIndexParam(indexParamCondition, null, null, CollectionUtil.newArrayList(
                IndexParamCondition.IndexColumn.builder().column("app_code").sort(Sort.DESC).build(),
                IndexParamCondition.IndexColumn.builder().column("unify_key").build()
        ), false);
    }

    @Test
    public void createTableOnIndex2() throws SqlParseException {
        String sql = "CREATE TABLE test (\n" +
                "                id int(11) NOT NULL AUTO_INCREMENT,\n" +
                "                real_Name varchar(255) DEFAULT NULL,\n" +
                "                city varchar(255) DEFAULT NULL,\n" +
                "                age int(11) unsigned DEFAULT NULL,\n" +
                "                count int(11) DEFAULT NULL,\n" +
                "                testUser varchar(255) DEFAULT NULL,\n" +
                "                testUsers varchar(255) DEFAULT NULL,\n" +
                "                codes varchar(255) DEFAULT NULL,\n" +
                "                codeStrs varchar(255) DEFAULT NULL,\n" +
                "                PRIMARY KEY (id),\n" +
                "                INDEX(`app_code` desc,`unify_key`),\n" +
                "                key(`app_code`)\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8;";

        CreateTableSqlToDslParser parser = new CreateTableSqlToDslParser(sql);
        SqlCreateTable sqlCreateTable = parser.handle();

        CreateTableParamCondition condition = sqlCreateTable.getCreateTable();

        Assert.assertEquals("test", parser.getTableName());

        List<CreateTableParamCondition.CreateTableFieldParam> createTableFieldParamList = condition.getCreateTableFieldParamList();
        checkFieldParam(createTableFieldParamList.get(0), "id", 11, null, "Integer", false);
        checkFieldParam(createTableFieldParamList.get(1), "real_Name", 255, null, "String", false);
        checkFieldParam(createTableFieldParamList.get(2), "city", 255, null, "String", false);
        checkFieldParam(createTableFieldParamList.get(3), "age", 11, null, "Integer", true);
        checkFieldParam(createTableFieldParamList.get(4), "count", 11, null, "Integer", false);
        checkFieldParam(createTableFieldParamList.get(5), "testUser", 255, null, "String", false);
        checkFieldParam(createTableFieldParamList.get(6), "testUsers", 255, null, "String", false);
        checkFieldParam(createTableFieldParamList.get(7), "codes", 255, null, "String", false);
        checkFieldParam(createTableFieldParamList.get(8), "codeStrs", 255, null, "String", false);

        List<IndexParamCondition> createIndex = sqlCreateTable.getCreateIndex();

        Assert.assertEquals(createIndex.size(), 2);

        IndexParamCondition indexParamCondition = createIndex.get(0);

        checkIndexParam(indexParamCondition, null, null, CollectionUtil.newArrayList(
                IndexParamCondition.IndexColumn.builder().column("app_code").sort(Sort.DESC).build(),
                IndexParamCondition.IndexColumn.builder().column("unify_key").build()
        ), false);

        indexParamCondition = createIndex.get(1);
        checkIndexParam(indexParamCondition, null, null, CollectionUtil.newArrayList(
                IndexParamCondition.IndexColumn.builder().column("app_code").build()
        ), false);
    }

    @Test
    public void createTableOnIndex3() throws SqlParseException {
        String sql = "CREATE TABLE test (\n" +
                "                id int(11) NOT NULL AUTO_INCREMENT,\n" +
                "                real_Name varchar(255) DEFAULT NULL,\n" +
                "                city varchar(255) DEFAULT NULL,\n" +
                "                age int(11) unsigned DEFAULT NULL,\n" +
                "                count int(11) DEFAULT NULL,\n" +
                "                testUser varchar(255) DEFAULT NULL,\n" +
                "                testUsers varchar(255) DEFAULT NULL,\n" +
                "                codes varchar(255) DEFAULT NULL,\n" +
                "                codeStrs varchar(255) DEFAULT NULL,\n" +
                "                PRIMARY KEY (id),\n" +
                "                INDEX au_index(`app_code` desc,`unify_key`),\n" +
                "                key `aa` (`app_code`)\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8;";

        CreateTableSqlToDslParser parser = new CreateTableSqlToDslParser(sql);
        SqlCreateTable sqlCreateTable = parser.handle();

        CreateTableParamCondition condition = sqlCreateTable.getCreateTable();

        Assert.assertEquals("test", parser.getTableName());

        List<CreateTableParamCondition.CreateTableFieldParam> createTableFieldParamList = condition.getCreateTableFieldParamList();
        checkFieldParam(createTableFieldParamList.get(0), "id", 11, null, "Integer", false);
        checkFieldParam(createTableFieldParamList.get(1), "real_Name", 255, null, "String", false);
        checkFieldParam(createTableFieldParamList.get(2), "city", 255, null, "String", false);
        checkFieldParam(createTableFieldParamList.get(3), "age", 11, null, "Integer", true);
        checkFieldParam(createTableFieldParamList.get(4), "count", 11, null, "Integer", false);
        checkFieldParam(createTableFieldParamList.get(5), "testUser", 255, null, "String", false);
        checkFieldParam(createTableFieldParamList.get(6), "testUsers", 255, null, "String", false);
        checkFieldParam(createTableFieldParamList.get(7), "codes", 255, null, "String", false);
        checkFieldParam(createTableFieldParamList.get(8), "codeStrs", 255, null, "String", false);

        List<IndexParamCondition> createIndex = sqlCreateTable.getCreateIndex();

        Assert.assertEquals(createIndex.size(), 2);

        IndexParamCondition indexParamCondition = createIndex.get(0);

        checkIndexParam(indexParamCondition, "au_index", null, CollectionUtil.newArrayList(
                IndexParamCondition.IndexColumn.builder().column("app_code").sort(Sort.DESC).build(),
                IndexParamCondition.IndexColumn.builder().column("unify_key").build()
        ), false);

        indexParamCondition = createIndex.get(1);
        checkIndexParam(indexParamCondition, "aa", null, CollectionUtil.newArrayList(
                IndexParamCondition.IndexColumn.builder().column("app_code").build()
        ), false);
    }

    @Test
    public void createTableOnIndex4() throws SqlParseException {
        String sql = "CREATE TABLE IF NOT EXISTS test (\n" +
                "                id int(11) NOT NULL AUTO_INCREMENT,\n" +
                "                real_Name varchar(255) DEFAULT NULL,\n" +
                "                city varchar(255) DEFAULT NULL,\n" +
                "                age int(11) unsigned DEFAULT NULL,\n" +
                "                count int(11) DEFAULT NULL,\n" +
                "                testUser varchar(255) DEFAULT NULL,\n" +
                "                testUsers varchar(255) DEFAULT NULL,\n" +
                "                codes varchar(255) DEFAULT NULL,\n" +
                "                codeStrs varchar(255) DEFAULT NULL,\n" +
                "                PRIMARY KEY (id),\n" +
                "                UNIQUE INDEX au_index(`app_code` desc,`unify_key`),\n" +
                "                UNIQUE key `aa` (`app_code`)\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8;";

        CreateTableSqlToDslParser parser = new CreateTableSqlToDslParser(sql);
        SqlCreateTable sqlCreateTable = parser.handle();

        CreateTableParamCondition condition = sqlCreateTable.getCreateTable();

        Assert.assertEquals("test", parser.getTableName());

        List<CreateTableParamCondition.CreateTableFieldParam> createTableFieldParamList = condition.getCreateTableFieldParamList();
        checkFieldParam(createTableFieldParamList.get(0), "id", 11, null, "Integer", false);
        checkFieldParam(createTableFieldParamList.get(1), "real_Name", 255, null, "String", false);
        checkFieldParam(createTableFieldParamList.get(2), "city", 255, null, "String", false);
        checkFieldParam(createTableFieldParamList.get(3), "age", 11, null, "Integer", true);
        checkFieldParam(createTableFieldParamList.get(4), "count", 11, null, "Integer", false);
        checkFieldParam(createTableFieldParamList.get(5), "testUser", 255, null, "String", false);
        checkFieldParam(createTableFieldParamList.get(6), "testUsers", 255, null, "String", false);
        checkFieldParam(createTableFieldParamList.get(7), "codes", 255, null, "String", false);
        checkFieldParam(createTableFieldParamList.get(8), "codeStrs", 255, null, "String", false);

        List<IndexParamCondition> createIndex = sqlCreateTable.getCreateIndex();

        Assert.assertEquals(createIndex.size(), 2);

        IndexParamCondition indexParamCondition = createIndex.get(0);

        checkIndexParam(indexParamCondition, "au_index", IndexType.UNIQUE, CollectionUtil.newArrayList(
                IndexParamCondition.IndexColumn.builder().column("app_code").sort(Sort.DESC).build(),
                IndexParamCondition.IndexColumn.builder().column("unify_key").build()
        ), true);

        indexParamCondition = createIndex.get(1);
        checkIndexParam(indexParamCondition, "aa", IndexType.UNIQUE, CollectionUtil.newArrayList(
                IndexParamCondition.IndexColumn.builder().column("app_code").build()
        ), true);
    }

    private void checkIndexParam(IndexParamCondition indexParamCondition, String indexName, IndexType indexType, List<IndexParamCondition.IndexColumn> columns, boolean isNotExists) {
        Assert.assertEquals(indexParamCondition.getIndexName(), indexName);
        Assert.assertEquals(indexParamCondition.getIndexType(), indexType);
        Assert.assertEquals(indexParamCondition.getColumns().size(), columns.size());
        List<IndexParamCondition.IndexColumn> columns1 = indexParamCondition.getColumns();
        for (int i = 0; i < columns.size(); i++) {
            IndexParamCondition.IndexColumn indexColumn = columns.get(i);
            IndexParamCondition.IndexColumn indexColumn1 = columns1.get(i);
            Assert.assertEquals(indexColumn1.getColumn(), indexColumn.getColumn());
            Assert.assertEquals(indexColumn1.getSort(), indexColumn.getSort());
        }
    }

    @Test
    public void createTableLike() throws SqlParseException {
        String sql = "CREATE TABLE IF NOT EXISTS test (LIKE copyTable)";

        CreateTableSqlToDslParser parser = new CreateTableSqlToDslParser(sql);
        SqlCreateTable sqlCreateTable = parser.handle();

        CreateTableParamCondition condition = sqlCreateTable.getCreateTable();

        CreateTableLikeParam createTableLike = condition.getCreateTableLike();
        Assert.assertNotNull("解析 createTableLike 对象为空", createTableLike);
        Assert.assertEquals("copyTable", createTableLike.getCopyTableName());
        Assert.assertEquals("test", parser.getTableName());
    }

    @Test
    public void createTableLike_1() throws SqlParseException {
        String sql = "CREATE TABLE IF NOT EXISTS test LIKE copy";

        CreateTableSqlToDslParser parser = new CreateTableSqlToDslParser(sql);
        SqlCreateTable sqlCreateTable = parser.handle();

        CreateTableParamCondition condition = sqlCreateTable.getCreateTable();

        CreateTableLikeParam createTableLike = condition.getCreateTableLike();
        Assert.assertNotNull("解析 createTableLike 对象为空", createTableLike);
        Assert.assertEquals("copy", createTableLike.getCopyTableName());
        Assert.assertEquals("test", parser.getTableName());
        Assert.assertTrue("应该是 IF NOT EXISTS SQL", condition.isNotExists());
    }

    @Test
    public void createTableLike_3() throws SqlParseException {
        String sql = "CREATE TABLE test LIKE `copy`";

        CreateTableSqlToDslParser parser = new CreateTableSqlToDslParser(sql);
        SqlCreateTable sqlCreateTable = parser.handle();

        CreateTableParamCondition condition = sqlCreateTable.getCreateTable();

        CreateTableLikeParam createTableLike = condition.getCreateTableLike();
        Assert.assertNotNull("解析 createTableLike 对象为空", createTableLike);
        Assert.assertEquals("copy", createTableLike.getCopyTableName());
        Assert.assertEquals("test", parser.getTableName());
        Assert.assertFalse("应该不是 IF NOT EXISTS SQL", condition.isNotExists());
    }

    @Test
    public void createTableLike_2() throws SqlParseException {
        String sql = "CREATE TABLE IF NOT EXISTS test (LIKE `int`)";

        CreateTableSqlToDslParser parser = new CreateTableSqlToDslParser(sql);
        SqlCreateTable sqlCreateTable = parser.handle();

        CreateTableParamCondition condition = sqlCreateTable.getCreateTable();

        CreateTableLikeParam createTableLike = condition.getCreateTableLike();
        Assert.assertNotNull("解析 createTableLike 对象为空", createTableLike);
        Assert.assertEquals("int", createTableLike.getCopyTableName());
        Assert.assertEquals("test", parser.getTableName());
    }

    @Test
    public void createTableLike_4() throws SqlParseException {
        String sql = "CREATE TABLE IF NOT EXISTS test (LIKE int)";

        CreateTableSqlToDslParser parser = new CreateTableSqlToDslParser(sql);
        SqlCreateTable sqlCreateTable = parser.handle();

        CreateTableParamCondition condition = sqlCreateTable.getCreateTable();

        CreateTableLikeParam createTableLike = condition.getCreateTableLike();
        Assert.assertNull("解析 createTableLike 对象不为空", createTableLike);

        List<CreateTableParamCondition.CreateTableFieldParam> createTableFieldParamList = condition.getCreateTableFieldParamList();
        Assert.assertNotNull("解析 字段 对象为空", createTableFieldParamList);
        CreateTableParamCondition.CreateTableFieldParam tableFieldParam = createTableFieldParamList.get(0);
        String fieldName = tableFieldParam.getFieldName();
        String fieldType = tableFieldParam.getFieldType();
        Assert.assertEquals("LIKE", fieldName);
        Assert.assertEquals("Integer", fieldType);

        Assert.assertEquals("test", parser.getTableName());
    }

    @Test
    public void createTableLike_5() throws SqlParseException {
        String sql = "CREATE TABLE IF NOT EXISTS test (a int)";

        CreateTableSqlToDslParser parser = new CreateTableSqlToDslParser(sql);
        SqlCreateTable sqlCreateTable = parser.handle();

        CreateTableParamCondition condition = sqlCreateTable.getCreateTable();

        CreateTableLikeParam createTableLike = condition.getCreateTableLike();
        Assert.assertNull("解析 createTableLike 对象不为空", createTableLike);

        List<CreateTableParamCondition.CreateTableFieldParam> createTableFieldParamList = condition.getCreateTableFieldParamList();
        Assert.assertNotNull("解析 字段 对象为空", createTableFieldParamList);
        CreateTableParamCondition.CreateTableFieldParam tableFieldParam = createTableFieldParamList.get(0);
        String fieldName = tableFieldParam.getFieldName();
        String fieldType = tableFieldParam.getFieldType();
        Assert.assertEquals("a", fieldName);
        Assert.assertEquals("Integer", fieldType);

        Assert.assertEquals("test", parser.getTableName());
    }

    @Test
    public void createTableLike_6() throws SqlParseException {
        String sql = "CREATE TABLE IF NOT EXISTS test (LIKE `int` INCLUDING DEFAULTS INCLUDING INDEXES)";

        CreateTableSqlToDslParser parser = new CreateTableSqlToDslParser(sql);
        SqlCreateTable sqlCreateTable = parser.handle();

        CreateTableParamCondition condition = sqlCreateTable.getCreateTable();

        CreateTableLikeParam createTableLike = condition.getCreateTableLike();
        Assert.assertNotNull("解析 createTableLike 对象为空", createTableLike);
        Assert.assertEquals("int", createTableLike.getCopyTableName());
        Assert.assertEquals("test", parser.getTableName());
    }

    @Test
    public void createTableLike_7() throws SqlParseException {
        String sql = "CREATE TABLE IF NOT EXISTS test LIKE `int` INCLUDING DEFAULTS INCLUDING INDEXES";

        CreateTableSqlToDslParser parser = new CreateTableSqlToDslParser(sql);
        SqlCreateTable sqlCreateTable = parser.handle();

        CreateTableParamCondition condition = sqlCreateTable.getCreateTable();

        CreateTableLikeParam createTableLike = condition.getCreateTableLike();
        Assert.assertNotNull("解析 createTableLike 对象为空", createTableLike);
        Assert.assertEquals("int", createTableLike.getCopyTableName());
        Assert.assertEquals("test", parser.getTableName());
    }
}
