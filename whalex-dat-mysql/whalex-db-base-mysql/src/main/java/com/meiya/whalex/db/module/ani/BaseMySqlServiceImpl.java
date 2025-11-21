package com.meiya.whalex.db.module.ani;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.map.MapUtil;
import cn.hutool.core.util.StrUtil;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.meiya.whalex.annotation.Support;
import com.meiya.whalex.converter.JdbcTypeConverter;
import com.meiya.whalex.db.constant.SupportPower;
import com.meiya.whalex.db.converter.MysqlTypeConverter;
import com.meiya.whalex.db.entity.DatabaseSetting;
import com.meiya.whalex.db.entity.PageResult;
import com.meiya.whalex.db.entity.QueryMethodResult;
import com.meiya.whalex.db.entity.TableSetting;
import com.meiya.whalex.db.entity.table.expressions.Expression;
import com.meiya.whalex.db.entity.table.expressions.FunctionExpression;
import com.meiya.whalex.db.entity.table.expressions.NamedReference;
import com.meiya.whalex.db.entity.table.expressions.literals.Literal;
import com.meiya.whalex.db.entity.table.expressions.literals.Literals;
import com.meiya.whalex.db.entity.table.expressions.transforms.Transform;
import com.meiya.whalex.db.entity.table.expressions.transforms.Transforms;
import com.meiya.whalex.db.entity.table.infomaration.MysqlTableInformation;
import com.meiya.whalex.db.entity.table.infomaration.TableInformation;
import com.meiya.whalex.db.entity.ani.*;
import com.meiya.whalex.db.entity.table.partition.TablePartitions;
import com.meiya.whalex.db.entity.table.parttions.Partition;
import com.meiya.whalex.db.entity.table.parttions.Partitions;
import com.meiya.whalex.db.entity.table.parttions.RangeColumnsPartition;
import com.meiya.whalex.db.entity.table.parttions.RangePartition;
import com.meiya.whalex.db.entity.table.types.Type;
import com.meiya.whalex.db.entity.table.types.Types;
import com.meiya.whalex.interior.db.constant.ItemFieldTypeEnum;
import com.meiya.whalex.interior.db.constant.PartitionType;
import com.meiya.whalex.interior.db.constant.PartitionTypeV2Constants;
import com.meiya.whalex.interior.db.search.condition.Sort;
import com.meiya.whalex.sql.ani.RdbmsCursorCache;
import com.meiya.whalex.sql.module.AbstractRdbmsModuleBaseService;
import com.meiya.whalex.exception.BusinessException;
import com.meiya.whalex.exception.ExceptionCode;;
import com.meiya.whalex.sql.module.SequenceHandler;
import com.meiya.whalex.sql.module.SqlParseHandler;
import com.meiya.whalex.util.JsonUtil;
import com.meiya.whalex.util.WildcardRegularConversion;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.dbutils.BasicRowProcessor;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.MapHandler;
import org.apache.commons.dbutils.handlers.MapListHandler;
import org.apache.commons.lang3.StringUtils;

import java.sql.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * MySql 服务
 *
 * @author 黄河森
 * @date 2019/12/23
 * @project whale-cloud-platformX
 */
@Slf4j
@Support(value = {
        SupportPower.SHOW_SCHEMA,
        SupportPower.QUERY_INDEX,
        SupportPower.CREATE_INDEX,
        SupportPower.CREATE_TABLE,
        SupportPower.DROP_TABLE,
        SupportPower.MODIFY_TABLE,
        SupportPower.SHOW_TABLE_LIST
})
public class BaseMySqlServiceImpl<S extends QueryRunner, Q extends AniHandler, D extends BaseMySqlDatabaseInfo, T extends BaseMySqlTableInfo, C extends RdbmsCursorCache>
        extends AbstractRdbmsModuleBaseService<S, Q, D, T, C> {

    /**
     * 查询库中表信息SQL
     */
    private static final String SHOW_TABLE_SQL = "select table_name, table_type, table_comment, table_schema,'%s' AS userName from information_schema.`TABLES` where Table_schema='%s'";


    private static final String SHOW_BINLOG = "show variables like 'log_bin'";

    /**
     * 查询表索引信息SQL
     */
    private static final String SHOW_INDEX_SQL = "SHOW INDEX FROM `%s`";

    /**
     * 删表SQL
     */
    private static final String DROP_TABLE_SQL = "DROP TABLE `%s`";

    /**
     * 清空表SQL
     */
    private static final String EMPTY_TABLE_SQL = "TRUNCATE TABLE `%s`";

    /**
     * 判断指定表是否存在
     */
    private static final String EXISTS_TABLE_SQL = "SELECT COUNT(*) AS count FROM information_schema.`TABLES` WHERE table_schema = '%s' AND table_name = '%s'";

    /**
     * 查询数据库列表
     */
    private static final String SHOW_DATABASE_SQL = "show databases";

    private static final String CREATE_DATABASE = "CREATE DATABASE `%s`";

    private static final String DROP_DATABASE = "DROP DATABASE `%s`";

    private static final String UPDATE_DATABASE = "RENAME DATABASE `%s` TO `%s`";

    private static final Pattern FUNCTION_PATTERN = Pattern.compile(
            "^\\s*(\\w+)\\s*\\(\\s*([a-zA-Z_][a-zA-Z0-9_]*)\\s*\\)\\s*$"
    );

    /**
     * 查询表元数据信息
     */
    private static final String TABLE_META = "SHOW TABLE status FROM %s WHERE name = '%s'";

    private static  JdbcTypeConverter typeConverter =new MysqlTypeConverter();

    @Override
    protected BasicRowProcessor getRowProcessor(D databaseConf) {
        return new BaseMyRowProcessor(databaseConf.getBit1isBoolean(), databaseConf.getJsonToObject());
    }

    @Override
    protected String formatSql(String sql, D databaseConf, T tableConf) {
        sql = String.format(sql, tableConf.getTableName());
        sql = StringUtils.replace(sql, "${tableName}", tableConf.getTableName());
        return StringUtils.replace(sql, "${localTable}", tableConf.getTableName());

    }

    @Override
    protected SqlParseHandler getSqlParseHandler(D database) {
        return new MysqlSqlParseHandler();
    }


    @Override
    protected QueryMethodResult changelog(S connect, D databaseConf) throws Exception {
        List<Map<String, Object>> query = queryExecute(databaseConf, connect, SHOW_BINLOG, new MapListHandler(), new Object[0]);
        Map<String, Object> changeLogMap = new HashMap<>();
        changeLogMap.put("configuration", new HashMap<>());
        changeLogMap.put("changelog", "off");
        if(!CollectionUtil.isEmpty(query)) {
            Map<String, Object> map = query.get(0);
            String logBin = (String) map.get("Variable_name");
            String value = (String) map.get("Value");

            if("log_bin".equalsIgnoreCase(logBin) && "on".equalsIgnoreCase(value)) {
                changeLogMap.put("changelog", "on");
            }
        }
        return new QueryMethodResult(1, CollectionUtil.newArrayList(changeLogMap));
    }

    @Override
    protected QueryMethodResult showTablesMethod(S connect, Q queryEntity, D databaseConf) throws Exception {
        String sql = String.format(SHOW_TABLE_SQL, databaseConf.getUserName(), databaseConf.getDatabaseName());
        if (StringUtils.isNotBlank(queryEntity.getListTable().getTableMatch())) {
            sql = "select * from (" + sql + ") d where d.table_name like \'" + queryEntity.getListTable().getTableMatch() + "\'";
        }
        List<Map<String, Object>> query = queryExecute(databaseConf, connect, sql, new MapListHandler(), new Object[0]);
        for (Map<String, Object> map : query) {
            map.put("tableName", map.get("TABLE_NAME"));
            map.remove("TABLE_NAME");
            map.put("tableComment", map.get("TABLE_COMMENT"));
            map.remove("TABLE_COMMENT");
            map.put("schemaName", map.get("TABLE_SCHEMA"));
            map.remove("TABLE_SCHEMA");
            map.put("tableType", map.get("TABLE_TYPE"));
            map.remove("TABLE_TYPE");
        }
        return new QueryMethodResult(query.size(), query);
    }

    @Override
    protected QueryMethodResult getIndexesMethod(S connect, D databaseConf, T tableConf) throws Exception {
        QueryMethodResult queryMethodResult = new QueryMethodResult();
        List<Map<String, Object>> dataList = new ArrayList<>();
        queryMethodResult.setRows(dataList);
        String showIndexSql = String.format(SHOW_INDEX_SQL, tableConf.getTableName());
        List<Map<String, Object>> queryList = queryExecute(databaseConf, connect, showIndexSql, new MapListHandler(), new Object[0]);
        Map<String, Map<String, Object>> indexMap = new HashMap<>();
        Map<String, Boolean> isUniqueMap = new HashMap<>();
        if (CollectionUtil.isNotEmpty(queryList)) {
            for (int i = 0; i < queryList.size(); i++) {
                Map<String, Object> map = queryList.get(i);
                String columnName = (String) map.get("Column_name");
                String indexName = (String) map.get("Key_name");
                Object uniqueObj = map.get("Non_unique");
                Long isNoUnique = 1L;
                if (uniqueObj != null) {
                    String valueOf = String.valueOf(uniqueObj);
                    if (StrUtil.isNumeric(valueOf)) {
                        isNoUnique = Long.valueOf(valueOf);
                    }
                }
                isUniqueMap.put(indexName, isNoUnique == 0);
                String collation = (String) map.get("Collation");
                Map<String, Object> column = indexMap.get(indexName);
                if (column == null) {
                    column = new LinkedHashMap<>();
                    indexMap.put(indexName, column);
                }
                column.put(columnName, StringUtils.equalsIgnoreCase(collation, "A") ? Sort.ASC.name() : Sort.DESC.name());
            }
        }

        for (Map.Entry<String, Map<String, Object>> entry : indexMap.entrySet()) {
            String indexName = entry.getKey();
            Map<String, Object> value = entry.getValue();
            Map<String, Object> resultMap = new HashMap<>(1);
            dataList.add(resultMap);
            resultMap.put("column", CollectionUtil.join(value.keySet(), ","));
            resultMap.put("indexName", indexName);
            resultMap.put("columns", value);
            if (StringUtils.equalsIgnoreCase(indexName, "PRIMARY")) {
                // 主键
                resultMap.put("isUnique", true);
                resultMap.put("isPrimaryKey", true);
            } else {
                resultMap.put("isUnique", isUniqueMap.get(entry.getKey()));
                resultMap.put("isPrimaryKey", false);
            }
        }

        return queryMethodResult;
    }

    @Override
    protected QueryMethodResult createTableMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        String sql = queryEntity.getAniCreateTable().getSql();
        boolean isNotExists = queryEntity.getAniCreateTable().isNotExists();
        if(isNotExists) {
            if(tableExists(connect, databaseConf, tableConf)) {
               return new QueryMethodResult();
            }
        }
        updateExecute(databaseConf, connect, sql, new Object[0]);
        return new QueryMethodResult();
    }

    @Override
    protected QueryMethodResult dropTableMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        boolean ifExists = queryEntity.getAniDropTable().isIfExists();
        if(ifExists) {
            if(!tableExists(connect, databaseConf, tableConf)) {
                return new QueryMethodResult();
            }
        }
        String dropTableSQL = String.format(DROP_TABLE_SQL, tableConf.getTableName());
        int update = updateExecute(databaseConf, connect, dropTableSQL, new Object[0]);
        return new QueryMethodResult(update, null);
    }

    @Override
    protected QueryMethodResult emptyTableMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        String emptyTableSQL = String.format(EMPTY_TABLE_SQL, tableConf.getTableName());
        int update = updateExecute(databaseConf, connect, emptyTableSQL, new Object[0]);
        return new QueryMethodResult(update, null);
    }

    @Override
    protected QueryMethodResult deleteIndexMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        String sql = queryEntity.getAniDropIndex().getSql();
        updateExecute(databaseConf, connect, sql, new Object[0]);
        return new QueryMethodResult();
    }

    @Override
    protected QueryMethodResult createIndexMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        String sql = queryEntity.getAniCreateIndex().getSql();
        if (queryEntity.getAniCreateIndex().isNotExists()) {
            QueryMethodResult indexesMethod = getIndexesMethod(connect, databaseConf, tableConf);
            List<Map<String, Object>> rows = indexesMethod.getRows();
            List<String> indexNames = rows.stream().flatMap(row -> {
                String indexName = (String) row.get("indexName");
                return Stream.of(indexName);
            }).collect(Collectors.toList());
            if (CollectionUtil.isNotEmpty(indexNames) && indexNames.contains(queryEntity.getAniCreateIndex().getIndexName())) {
                return new QueryMethodResult();
            }
        }
        updateExecute(databaseConf, connect, sql, new Object[0]);
        return new QueryMethodResult();
    }

    @Override
    protected QueryMethodResult monitorStatusMethod(S connect, D databaseConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "MySqlServiceImpl.monitorStatusMethod");
    }

    @Override
    protected QueryMethodResult querySchemaMethod(S connect, D databaseConf, T tableConf) throws Exception {
        List<Map<String, Object>> schemaInfoList = new ArrayList<>();
        Connection conn = null;
        ResultSet primaryKeys = null;
        ResultSet columnSet = null;
        String tableName = tableConf.getTableName();
        boolean isTransaction = true;
        try {
            conn = getConnection(databaseConf, connect);
            if(conn == null) {
                conn = connect.getDataSource().getConnection();
                isTransaction = false;
            }
            // 获取主键
            primaryKeys = conn.getMetaData().getPrimaryKeys(databaseConf.getDatabaseName(), null, tableName);
            List<String> primaryKeyList = new ArrayList<>();
            while (primaryKeys.next()) {
                primaryKeyList.add(primaryKeys.getString("COLUMN_NAME"));
            }
            columnSet = conn.getMetaData().getColumns(databaseConf.getDatabaseName(), null, tableName, "%");

            while (columnSet.next()) {
                Map<String, Object> map = new HashMap<>();
                String columnName = columnSet.getString("COLUMN_NAME");
                map.put("col_name", columnName);
                String isAutoincrement = columnSet.getString("IS_AUTOINCREMENT");
                if (StringUtils.equalsIgnoreCase(isAutoincrement, "YES")) {
                    map.put("autoincrement", 1);
                } else {
                    map.put("autoincrement", 0);
                }
                int isKey = 0;
                if (primaryKeyList.contains(columnName)) {
                    //主键
                    isKey = 1;
                }
                map.put("isKey", isKey);
                String columnDesc = columnSet.getString("REMARKS");
                columnDesc = columnDesc == null ? "" : columnDesc;
                map.put("comment", columnDesc);
                String columnType = columnSet.getString("TYPE_NAME").toLowerCase();
                // GEOMETRY 为 mysql 几何类型的基类，因为这边将基类转为 POINT 类型
                if (StringUtils.equalsIgnoreCase(columnType, "GEOMETRY")) {
                    columnType = "point";
                }
                if (StringUtils.containsIgnoreCase(columnType, "unsigned")) {
                    columnType = StringUtils.replaceIgnoreCase(columnType, "unsigned", "").trim();
                    map.put("unsigned", true);
                }

                map.put("data_type", columnType);

                String columnSize = columnSet.getString("COLUMN_SIZE");
                if (StringUtils.isEmpty(columnSize)) {
                    map.put("columnSize", "");
                } else {
                    map.put("columnSize", columnSize);
                }
                // 小数点
                String decimalDigits = columnSet.getString("DECIMAL_DIGITS");
                if (StringUtils.isEmpty(decimalDigits)) {
                    map.put("decimalDigits", "");
                } else {
                    map.put("decimalDigits", decimalDigits);
                }
                map.put("std_data_type", getItemFieldTypeEnum(columnType, StringUtils.isNotBlank(columnSize) ? Integer.valueOf(columnSize) : null, databaseConf.getBit1isBoolean()).getVal());
                int nullableInt = columnSet.getInt("NULLABLE");
                map.put("nullable", nullableInt);
                String columnDef = columnSet.getString("COLUMN_DEF");
                columnDef = columnDef == null ? "" : columnDef;
                map.put("columnDef", columnDef);
                schemaInfoList.add(map);
            }
        } finally {
            if (columnSet != null) {
                try {
                    columnSet.close();
                } catch (SQLException e) {
                    log.error("MySql ResultSet 对象关闭失败!", e);
                }
            }

            if (primaryKeys != null) {
                try {
                    primaryKeys.close();
                } catch (SQLException e) {
                    log.error("MySql ResultSet 对象关闭失败!", e);

                }
            }

            if (conn != null && !isTransaction) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    log.error("MySql conn 对象关闭失败!", e);
                }
            }
        }
        return new QueryMethodResult(schemaInfoList.size(), schemaInfoList);
    }

    @Override
    protected QueryMethodResult queryDatabaseSchemaMethod(S connect, D databaseConf, T tableConf) throws Exception {
        return new QueryMethodResult(0L, new ArrayList<>());
    }

    @Override
    protected QueryMethodResult saveOrUpdateBatchMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        List<String> sqlList = queryEntity.getAniUpsertBatch().getSql();
        int update = updateExecute(databaseConf, connect, sqlList.get(0), queryEntity.getAniUpsertBatch().getParamArray().get(0));
        return new QueryMethodResult(update, null);
    }

    @Override
    protected QueryMethodResult alterTableMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        AniHandler.AniAlterTable aniAlterTable = queryEntity.getAniAlterTable();
        List<AniHandler.AlterTableSqlInfo> alterTableSqlInfos = aniAlterTable.getAlterTableSqlInfos();
        Map<String, Map<String, Object>> fieldName2fieldProperties = getFieldName2fieldProperties(connect, queryEntity, databaseConf,  tableConf);;
        for (AniHandler.AlterTableSqlInfo alterTableSqlInfo : alterTableSqlInfos) {
            String sql = alterTableSqlInfo.getSql();
            if(sql.contains("${dataType_")) {
                String fieldName = getFieldName("dataType_", sql);
                Map<String, Object> fieldProperties = getFieldProperties(fieldName2fieldProperties, fieldName);
                sql = StringUtils.replace(sql, "${dataType_" + fieldName + "}", (String) fieldProperties.get("Type"));
            }

            if(sql.contains("${notNull_")) {
                String fieldName = getFieldName("notNull_", sql);
                Map<String, Object> fieldProperties = getFieldProperties(fieldName2fieldProperties, fieldName);
                String aNull = (String) fieldProperties.get("Null");
                if(aNull.equalsIgnoreCase("NO")) {
                    sql = StringUtils.replace(sql, "${notNull_" + fieldName + "}", "NOT NULL");
                }else {
                    sql = StringUtils.replace(sql, "${notNull_" + fieldName + "}", "NULL");
                }
            }

            if(sql.contains("${DEFAULT_")) {
                String fieldName = getFieldName("DEFAULT_", sql);
                Map<String, Object> fieldProperties = getFieldProperties(fieldName2fieldProperties, fieldName);
                String aDefault = (String) fieldProperties.get("Default");
                if(aDefault != null) {
                    sql = StringUtils.replace(sql, "${DEFAULT_" + fieldName + "}", "DEFAULT '"+aDefault+"'");
                }else {
                    sql = StringUtils.replace(sql, "${DEFAULT_" + fieldName + "}", "");
                }
            }

            if(sql.contains("${COMMENT_")) {
                String fieldName = getFieldName("COMMENT_", sql);
                Map<String, Object> fieldProperties = getFieldProperties(fieldName2fieldProperties, fieldName);
                String comment = (String) fieldProperties.get("Comment");
                if(comment != null) {
                    sql = StringUtils.replace(sql, "${COMMENT_" + fieldName + "}", "COMMENT '"+comment+"'");
                }else {
                    sql = StringUtils.replace(sql, "${COMMENT_" + fieldName + "}", "");
                }
            }


            if(log.isDebugEnabled()) {
                log.debug("alterTable 执行修改的sql是: {}", sql);
            }

            String columnName = alterTableSqlInfo.getColumnName();
            boolean isNotExists = alterTableSqlInfo.isNotExists();
            if(isNotExists) {
                Map<String, Object> map = fieldName2fieldProperties.get(columnName);
                if(map == null) {
                    updateExecute(databaseConf, connect, sql, new Object[0]);
                }
                return new QueryMethodResult();
            }

            updateExecute(databaseConf, connect, sql, new Object[0]);
        }
        return new QueryMethodResult();
    }


    private Map<String, Object> getFieldProperties(Map<String, Map<String, Object>> fieldName2fieldProperties, String fieldName) {
        Map<String, Object> map = fieldName2fieldProperties.get(fieldName);
        if(map == null) {
            throw new RuntimeException("字段[" + fieldName + "]不存在");
        }
        return map;
    }

    private Map<String, Map<String, Object>> getFieldName2fieldProperties(S connect, Q queryEntity, D databaseConf, T tableConf) throws SQLException {
        Map<String, Map<String, Object>> fieldName2fieldProperties = new HashMap<>();
        List<Map<String, Object>> query = queryExecute(databaseConf, connect, "show full columns from `" + tableConf.getTableName() + "`", new MapListHandler(), new Object[0]);
        for (Map<String, Object> map : query) {
            String field = (String) map.get("Field");
            fieldName2fieldProperties.put(field, map);
        }

        return fieldName2fieldProperties;
    }

    private String getFieldName(String flag, String sql) {
        int start = sql.indexOf("${" + flag);
        int end = sql.indexOf("}", start);
        String fieldName = sql.substring(start + 2 + flag.length(), end);
        return fieldName;
    }


    @Override
    protected QueryMethodResult tableExistsMethod(S connect, D databaseConf, T tableConf) throws Exception {
        String sql = String.format(EXISTS_TABLE_SQL, databaseConf.getDatabaseName(), tableConf.getTableName());
        Map<String, Object> query = queryExecute(databaseConf, connect, sql, new MapHandler(), new Object[0]);
        Long total = Long.parseLong(query.get("count").toString());
        Map<String, Object> map = new HashMap<>(1);
        if (total > 0) {
            map.put(tableConf.getTableName(), true);
        } else {
            map.put(tableConf.getTableName(), false);
        }
        return new QueryMethodResult(1, CollectionUtil.newArrayList(map));
    }

    @Override
    protected QueryMethodResult queryTableInformationMethod(S connect, D databaseConf, T tableConf) throws Exception {
        // 拼接SQL
        String sql = String.format(TABLE_META, databaseConf.getDatabaseName(), tableConf.getTableName());
        // SQL 执行
        Map<String, Object> metaMap = queryExecute(databaseConf, connect, sql, new MapHandler(), new Object[0]);
        // 元数据 KEY 转换
        if (CollectionUtil.isNotEmpty(metaMap)) {
            Map<String, Object> result = new HashMap<>(metaMap.size());
            TableInformation<Object> tableInformation = MysqlTableInformation.builder().tableName(tableConf.getTableName()).extendMeta(result).build();
            for (Map.Entry<String, Object> entry : metaMap.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();
                key = StrUtil.toCamelCase(StrUtil.lowerFirst(key));
                result.put(key, value);
            }
            return new QueryMethodResult(1, CollectionUtil.newArrayList(JsonUtil.entityToMap(tableInformation)));
        } else {
            throw new BusinessException(ExceptionCode.NOT_FOUND_TABLE_EXCEPTION, tableConf.getTableName());
        }
    }

    @Override
    protected QueryMethodResult queryListDatabaseMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        List<Map<String, Object>> rows = queryExecute(databaseConf, connect, SHOW_DATABASE_SQL, new MapListHandler(), new Object[0]);
        if (StringUtils.isNotBlank(queryEntity.getListDatabase().getDatabaseMatch())) {
            rows = WildcardRegularConversion.matchFilter(queryEntity.getListDatabase().getDatabaseMatch(), rows, "Database");
        }
        QueryMethodResult queryMethodResult = new QueryMethodResult(rows.size(), rows);
        return queryMethodResult;
    }

    @Override
    protected QueryMethodResult queryDatabaseInformationMethod(S connect, Q queryEntity, D databaseConf, T tableConf) {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + ".queryDatabaseInformationMethod");
    }

    @Override
    protected SequenceHandler getSequenceHandler(D dataConf) {
        throw new RuntimeException("mysql暂不支持序列");
    }

    @Override
    protected QueryMethodResult saveOrUpdateMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        String sql = queryEntity.getAniUpsert().getSql();
        Object[] paramArray = queryEntity.getAniUpsert().getParamArray();
        int update = updateExecute(databaseConf, connect, sql, paramArray);
        return new QueryMethodResult(update, null);
    }

    protected ItemFieldTypeEnum getItemFieldTypeEnum(String dbFieldType, Integer fieldLength, boolean bit1isBoolean) {
        return BaseMySqlFieldTypeEnum.dbFieldType2FieldType(dbFieldType, fieldLength, bit1isBoolean);
    }

    @Override
    protected QueryMethodResult createDatabaseMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        AniHandler.AniCreateDatabase createDatabase = queryEntity.getCreatedatabase();
        String dbName = createDatabase.getDbName();
        String character = createDatabase.getCharacter();
        String collate = createDatabase.getCollate();
        String sql = String.format(CREATE_DATABASE, dbName);
        if(StringUtils.isNotBlank(character)) {
            sql += " CHARACTER SET " + character;
        }
        if(StringUtils.isNotBlank(collate)) {
            sql += " COLLATE " + collate;
        }

        int update = updateExecute(databaseConf, connect, sql, new Object[0]);
        return new QueryMethodResult(update, null);
    }

    @Override
    protected QueryMethodResult updateDatabaseMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        AniHandler.AniUpdateDatabase updateDatabase = queryEntity.getUpdatedatabase();
        String dbName = updateDatabase.getDbName();
        String newDbName = updateDatabase.getNewDbName();
        String sql = String.format(UPDATE_DATABASE, dbName, newDbName);
        int update = updateExecute(databaseConf, connect, sql, new Object[0]);
        return new QueryMethodResult(update, null);
    }

    @Override
    protected QueryMethodResult dropDatabaseMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        AniHandler.AniDropDatabase dropDatabase = queryEntity.getDropdatabase();
        String dbName = dropDatabase.getDbName();
        String sql = String.format(DROP_DATABASE, dbName);
        int update = updateExecute(databaseConf, connect, sql, new Object[0]);
        return new QueryMethodResult(update, null);
    }

    @Override
    protected QueryMethodResult queryPartitionInformationMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        List<Map<String, Object>> result = new ArrayList<>();
        String sql = "select PARTITION_NAME,PARTITION_EXPRESSION,PARTITION_DESCRIPTION,PARTITION_METHOD,TABLE_ROWS from  information_schema.PARTITIONS where TABLE_NAME= '" + tableConf.getTableName()
                +"' and TABLE_SCHEMA='"+databaseConf.getDbName()+"'";
        List<Map<String, Object>> rows = queryExecute(databaseConf, connect, sql, new MapListHandler(), new Object[0]);
        if (CollectionUtil.isNotEmpty(rows)) {
            for (Map<String, Object> row : rows) {
                if (MapUtil.isNotEmpty(row)) {
                    String partitionName = (String) row.get("PARTITION_NAME");
                    String partitionKey = (String) row.get("PARTITION_EXPRESSION");
                    String partitionType = (String) row.get("PARTITION_METHOD");
                    //partitionKey为空，说明不是分区表
                    if(StringUtils.isBlank(partitionKey)) {
                        continue;
                    }
                    TablePartitions tablePartitions = analyzePartition(partitionName, partitionKey, partitionType);
                    result.add(JsonUtil.entityToMap(tablePartitions));
                }
            }
        }
        return new QueryMethodResult(result.size(), result);
    }

    private TablePartitions analyzePartition(String partitionName, String partitionKey, String partitionType) {
        PartitionType type;
        String[] split;
        split = StringUtils.split(partitionKey, ",");
        TablePartitions tablePartitions = null;
        List<TablePartitions.TablePartition> partitions = new ArrayList<>();
        for (String key : split) {
            key = key.replaceAll("`","");
            TablePartitions.TablePartition tablePartition = TablePartitions.TablePartition.builder().partitionKey(key).build();
            partitions.add(tablePartition);
        }
        tablePartitions = TablePartitions.builder().partitionType(partitionType).partitionName(partitionName).partitions(partitions).build();
        return tablePartitions;
    }

    @Override
    public QueryMethodResult queryPartitionInformationMethodV2(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        List<Map<String, Object>> result = new ArrayList<>();
        Map<String, Type> columnTypes = getColumnType(connect,queryEntity,databaseConf,tableConf);
        String showPartitionsSql ="select * from  information_schema.PARTITIONS where TABLE_NAME= '" + tableConf.getTableName()
                +"' and TABLE_SCHEMA='"+databaseConf.getDbName()+"'";
        Connection connection =null;
        Statement statement =null;
        try {
            connection = connect.getDataSource().getConnection();
            ImmutableList.Builder<Partition> partitions = ImmutableList.builder();
             statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(showPartitionsSql);
            Map<String,Object> lastResultMap=null;
            while (resultSet.next()) {
                Partition partition =fromMysqlPartition(resultSet, columnTypes);
                if (partition == null){
                  continue;
                }
                Map<String,Object> resultMap= partition.toMap();
                if (resultMap.containsKey("upper")){
                    if (lastResultMap != null){
                        resultMap.put("lower",lastResultMap.get("upper"));
                    }
                    lastResultMap =resultMap;

                }
                result.add(resultMap);

            }
        } catch (Exception e) {
            log.error("queryPartitionInformationMethodV2 error:?,错误:?",tableConf.getTableName(),e.getMessage());
        }finally {
            if (statement !=null){
                statement.close();
            }
            if (connection!=null){
                connection.close();
            }
        }


        return new QueryMethodResult(result.size(), result);
    }
    private Partition fromMysqlPartition(
            ResultSet resultSet, Map<String, Type> columnTypes)
            throws SQLException {
        String partitionName = resultSet.getString("PARTITION_NAME");
        String partitionMethod = resultSet.getString("PARTITION_METHOD");
        String partitionValues = resultSet.getString("PARTITION_DESCRIPTION");
        String partitionExpression = resultSet.getString("PARTITION_EXPRESSION") == null?"":resultSet.getString("PARTITION_EXPRESSION");
        ImmutableMap.Builder<String, String> propertiesBuilder = ImmutableMap.builder();
        if (StringUtils.isBlank(partitionName) || StringUtils.isBlank(partitionMethod)){
              return null;
        }
        if ( StringUtils.isNotBlank(resultSet.getString("PARTITION_ORDINAL_POSITION"))) {
            propertiesBuilder.put("PARTITION_ORDINAL_POSITION", resultSet.getString("PARTITION_ORDINAL_POSITION"));
        }
        if ( StringUtils.isNotBlank(resultSet.getString("SUBPARTITION_NAME"))) {
            propertiesBuilder.put("SUBPARTITION_NAME", resultSet.getString("SUBPARTITION_NAME"));
        }
        if ( StringUtils.isNotBlank(resultSet.getString("SUBPARTITION_ORDINAL_POSITION"))) {
            propertiesBuilder.put("SUBPARTITION_ORDINAL_POSITION", resultSet.getString("SUBPARTITION_ORDINAL_POSITION"));
        }
        propertiesBuilder.put("PARTITION_METHOD", resultSet.getString("PARTITION_METHOD"));
        if ( StringUtils.isNotBlank(resultSet.getString("SUBPARTITION_METHOD"))) {
            propertiesBuilder.put("SUBPARTITION_METHOD", resultSet.getString("SUBPARTITION_METHOD"));
        }
        propertiesBuilder.put("PARTITION_EXPRESSION", resultSet.getString("PARTITION_EXPRESSION"));
        if ( StringUtils.isNotBlank(resultSet.getString("SUBPARTITION_EXPRESSION"))) {
            propertiesBuilder.put("SUBPARTITION_EXPRESSION", resultSet.getString("SUBPARTITION_EXPRESSION"));
        }
        if ( StringUtils.isNotBlank(resultSet.getString("PARTITION_DESCRIPTION"))) {
            propertiesBuilder.put("PARTITION_DESCRIPTION", resultSet.getString("PARTITION_DESCRIPTION"));
        }
        ImmutableMap<String, String> properties = propertiesBuilder.build();
        partitionExpression = partitionExpression.replaceAll("'","").replaceAll("`","");
        List<Expression> keys =getPartitionExpression(partitionExpression);
        if (partitionMethod.equals(PartitionTypeV2Constants.RANGE) ) {

            Literal<?> lower = Literals.NULL;
            Literal<?> upper = Literals.NULL;
            upper = Literals.of(partitionValues, Types.StringType.get().simpleString());

            return Partitions.range(partitionName, upper, lower, properties,keys);
        } else if (partitionMethod.equals(PartitionTypeV2Constants.LIST)) {
            ImmutableList.Builder<Literal<?>[]> lists = ImmutableList.builder();
            if (StringUtils.isNotBlank(partitionValues)){
                String[] partitionValueArr= partitionValues.split(",");
                ImmutableList.Builder<Literal<?>> literValues = ImmutableList.builder();
                for (String partitionValue : partitionValueArr) {
                    literValues.add(Literals.of(partitionValue, Types.StringType.get().simpleString()));
                }
                lists.add(literValues.build().toArray(new Literal<?>[0]));
            }
            return Partitions.list(
                    partitionName, lists.build().toArray(new Literal<?>[0][0]), properties,keys);
        } else if (partitionMethod.equals(PartitionTypeV2Constants.HASH)) {
            return Partitions.hash(
                    partitionName,  properties,keys);
        } else if (partitionMethod.equals(PartitionTypeV2Constants.KEY)) {
            ImmutableList.Builder<Literal<?>[]> lists = ImmutableList.builder();
            return Partitions.key(
                    partitionName, properties,keys);
        }else if (partitionMethod.equals(PartitionTypeV2Constants.LIST_COLUMNS)) {
            ImmutableList.Builder<Literal<?>[]> lists = ImmutableList.builder();
            if (StringUtils.isNotBlank(partitionValues)){
                String[] partitionValueArr= partitionValues.replace("(","").replace(")","").split(",");
                ImmutableList.Builder<Literal<?>> literValues = ImmutableList.builder();
                for (String partitionValue : partitionValueArr) {
                    literValues.add(Literals.of(partitionValue.replaceAll("'",""), Types.StringType.get().simpleString()));
                }
                lists.add(literValues.build().toArray(new Literal<?>[0]));
            }
            return Partitions.listColumns(
                    partitionName, lists.build().toArray(new Literal<?>[0][0]), properties,keys);
        }else if (partitionMethod.equals(PartitionTypeV2Constants.RANGE_COLUMNS) ) {

            Literal<?> lower = Literals.NULL;
            Literal<?> upper = Literals.NULL;
            upper = Literals.of(partitionValues, Types.StringType.get().simpleString());
            return Partitions.rangeColumns(partitionName, upper, lower, properties,keys);
        } else {
            throw new UnsupportedOperationException(
                    " is not a partitioned table");
        }
    }
    private List<Expression> getPartitionExpression(String expressionValue){
        List<Expression> parttionKeyExpression =CollectionUtil.newArrayList();
        String[]  expressionArr=  expressionValue.split(",");
        for (String value : expressionArr) {
            Matcher funcMatcher = FUNCTION_PATTERN.matcher(value);
            if (funcMatcher.matches()) {
                String function = funcMatcher.group(1);
                String field = funcMatcher.group(2);
                parttionKeyExpression.add(FunctionExpression.of(function,NamedReference.field(field)));
            }else{
                parttionKeyExpression.add(NamedReference.field(value)) ;
            }

        }

        return parttionKeyExpression;


    }

    private Map<String, Type> getColumnType(S connect, AniHandler queryEntity,  D databaseConf, T tableConf) throws Exception {
        Connection conn = null;
        try {
            conn = getConnection(databaseConf, connect);
            if(conn == null) {
                conn = connect.getDataSource().getConnection();

            }
            DatabaseMetaData metaData = conn.getMetaData();
            try (ResultSet result =
                         metaData.getColumns(
                                 conn.getCatalog(), conn.getSchema(), tableConf.getTableName(), null)) {
                ImmutableMap.Builder<String, Type> columnTypes = ImmutableMap.builder();
                while (result.next()) {
                    if (Objects.equals(result.getString("TABLE_NAME"), tableConf.getTableName())) {
                        JdbcTypeConverter.JdbcTypeBean typeBean =
                                new JdbcTypeConverter.JdbcTypeBean(result.getString("TYPE_NAME"));
                        typeBean.setColumnSize(result.getInt("COLUMN_SIZE"));
                        typeBean.setScale(result.getInt("DECIMAL_DIGITS"));
                        Type gravitinoType = typeConverter.toGravitino(typeBean);
                        String columnName = result.getString("COLUMN_NAME");
                        columnTypes.put(columnName, gravitinoType);
                    }
                }
                return columnTypes.build();
            }
        } finally {
             if (conn !=null){
                 conn.close();
             }
        }
    }
}
