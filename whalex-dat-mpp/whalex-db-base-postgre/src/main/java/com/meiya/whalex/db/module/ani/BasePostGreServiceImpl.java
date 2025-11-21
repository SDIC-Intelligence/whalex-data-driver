package com.meiya.whalex.db.module.ani;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.util.ArrayUtil;
import cn.hutool.core.util.StrUtil;
import com.meiya.whalex.annotation.Support;
import com.meiya.whalex.db.constant.SupportPower;
import com.meiya.whalex.db.entity.QueryCursorMethodResult;
import com.meiya.whalex.db.entity.QueryMethodResult;
import com.meiya.whalex.db.entity.ani.AniHandler;
import com.meiya.whalex.db.entity.ani.BasePostGreDatabaseInfo;
import com.meiya.whalex.db.entity.ani.BasePostGreFieldTypeEnum;
import com.meiya.whalex.db.entity.ani.BasePostGreTableInfo;
import com.meiya.whalex.db.entity.table.infomaration.TableInformation;
import com.meiya.whalex.db.error.PostgreFunctionErrorHandlerInfoTips;
import com.meiya.whalex.db.util.helper.impl.ani.BasePostGreConfigHelper;
import com.meiya.whalex.db.util.param.impl.ani.BasePostGreParamUtil;
import com.meiya.whalex.interior.db.search.condition.Sort;
import com.meiya.whalex.interior.db.search.in.Page;
import com.meiya.whalex.sql.ani.RdbmsCursorCache;
import com.meiya.whalex.sql.module.AbstractRdbmsModuleBaseService;
import com.meiya.whalex.db.util.param.impl.ani.BasePgRowProcessor;
import com.meiya.whalex.exception.BusinessException;
import com.meiya.whalex.exception.ExceptionCode;
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
import java.util.function.Consumer;
import java.util.function.Function;


/**
 * @author 黄河森
 * @date 2019/12/28
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
public class BasePostGreServiceImpl<S extends QueryRunner,
        Q extends AniHandler,
        D extends BasePostGreDatabaseInfo,
        T extends BasePostGreTableInfo,
        C extends RdbmsCursorCache> extends AbstractRdbmsModuleBaseService<S, Q, D, T, C> {

    /**
     * 查询库中表信息SQL
     */
    private static final String SHOW_TABLE_SQL = "select distinct tablename AS tableName,tableComment,schemaname AS schemaName from ( " +
            "select tablename,tableComment,schemaname from ( " +
            "select p.tablename,s.tableComment,p.schemaname from pg_tables p inner join (select relname as tabname,cast(obj_description(c.oid,'pg_class') as varchar) as tableComment from pg_class c inner join pg_namespace n on c.relnamespace = n.oid and n.nspname='${schema}') " +
            "s on p.tablename=s.tabname " +
            "where p.schemaname='${schema}' " +
            ") a " +
            "union all " +
            "select tablename,tableComment,schemaname from ( " +
            " select p.viewname as tablename,s.tableComment,p.schemaname from pg_views p inner join (select relname as tabname,cast(obj_description(c.oid,'pg_class') as varchar) as tableComment from pg_class c inner join pg_namespace n on c.relnamespace = n.oid and n.nspname='${schema}') " +
            "s on p.viewname=s.tabname " +
            " where p.schemaname='${schema}' order by p.viewname " +
            ") b " +
            ") c";

    /**
     * 查询表索引信息SQL
     */
    private static final String SHOW_INDEX_SQL = "SELECT indexname, indexdef FROM pg_indexes WHERE (tablename='%s' OR tablename='%s') AND schemaname = '%s'";

    /**
     * 删表SQL
     */
    private static final String DROP_TABLE_SQL = "DROP TABLE %s";

    /**
     * 判断表是否存在
     */
    private static final String EXISTS_TABLE_SQL = "SELECT DISTINCT " +
            "COUNT(*) " +
            "FROM " +
            "( " +
            "SELECT tablename " +
            "FROM ( " +
            "SELECT P.tablename FROM pg_tables P INNER JOIN ( SELECT relname AS tabname FROM pg_class C INNER JOIN pg_namespace n ON C.relnamespace = n.oid AND n.nspname = '${schema}' ) s " +
            "ON P.tablename = s.tabname " +
            "WHERE " +
            "P.schemaname = '${schema}' " +
            ") A " +
            "UNION ALL " +
            "SELECT " +
            "tablename " +
            "FROM " +
            "( " +
            "SELECT P.viewname AS tablename " +
            "FROM " +
            "pg_views P INNER JOIN ( SELECT relname AS tabname FROM pg_class C INNER JOIN pg_namespace n ON C.relnamespace = n.oid AND n.nspname = '${schema}' ) s ON P.viewname = s.tabname " +
            "WHERE " +
            "P.schemaname = '${schema}' " +
            "ORDER BY " +
            "P.viewname " +
            ") b " +
            ") C WHERE tablename = '${tableName_low}' OR tablename = '${tableName_up}' OR tablename= '${tableName}'";


    /**
     * 查看序列SQL
     */
    private static final String SHOW_SEQUENCE_SQL = "select sequence_name from  information_schema.sequences where sequence_schema = ? and sequence_name like ?";

    /**
     * 删除序列
     */
    private static final String DROP_SEQUENCE_SQL = "DROP SEQUENCE %s";

    /**
     *删除主键
     */
    private final static String DROP_PRIMARY_KEY = "ALTER TABLE %s DROP CONSTRAINT %s";


    /**
     * 查询数据库列表
     */
    private static final String SHOW_DATABASE_SQL = "SELECT datname as \"Database\" FROM pg_database";

    private static final String CREATE_DATABASE = "CREATE DATABASE %s";

    private static final String DROP_DATABASE = "DROP DATABASE %s";

    private static final String UPDATE_DATABASE = "ALTER DATABASE %s RENAME TO %s";

    /**
     * 获取行处理器
     * @return
     */
    @Override
    protected BasicRowProcessor getRowProcessor(D databaseConf) {
        return new BasePgRowProcessor();
    }

    @Override
    protected String formatSql(String sql, D databaseConf, T tableConf) {
        sql = StringUtils.replace(sql, "${tableName}", jointTable(databaseConf.getSchema(), tableConf.getTableName()));
        return sql;
    }

    @Override
    protected SqlParseHandler getSqlParseHandler(D database) {
        return new PostgreSqlParseHandler(database);
    }

    @Override
    protected QueryMethodResult showTablesMethod(S connect, Q queryEntity, D databaseConf) throws Exception {
        String sql = StringUtils.replace(SHOW_TABLE_SQL, "${schema}", databaseConf.getSchema());
        if (StringUtils.isNotBlank(queryEntity.getListTable().getTableMatch())) {
            sql = "select * from (" + sql + ") d where d.tableName like \'" + queryEntity.getListTable().getTableMatch() + "\'";
        }
        List<Map<String, Object>> query = queryExecute(databaseConf, connect, sql, new MapListHandler(), new Object[0]);
        for (Map<String, Object> map : query) {
            map.put("tableName", map.remove("tablename"));
            map.put("tableComment", map.remove("tablecomment"));
            map.put("schemaName", map.remove("schemaname"));
        }
        return new QueryMethodResult(query.size(), query);
    }

    @Override
    protected QueryMethodResult getIndexesMethod(S connect, D databaseConf, T tableConf) throws Exception {
        QueryMethodResult queryMethodResult = new QueryMethodResult();
        List<Map<String, Object>> dataList = new ArrayList<>();
        queryMethodResult.setRows(dataList);
        String showIndexSql = String.format(SHOW_INDEX_SQL, tableConf.getTableName(), tableConf.getTableName().toLowerCase(), databaseConf.getSchema());
        List<Map<String, Object>> queryList = queryExecute(databaseConf, connect, showIndexSql, new MapListHandler(), new Object[0]);
        if (CollectionUtil.isNotEmpty(queryList)) {
            for (int i = 0; i < queryList.size(); i++) {
                Map<String, Object> map = queryList.get(i);
                Map<String, Object> resultMap = new HashMap<>(1);
                dataList.add(resultMap);
                Object indexSql = map.get("indexdef");
                String indexName = (String) map.get("indexname");
                resultMap.put("indexName", indexName);
                if (indexSql != null) {
                    String columnNameStr = String.valueOf(indexSql);
                    columnNameStr = StringUtils.substringBetween(columnNameStr, "(", ")");

                    String[] indexColumns = StringUtils.split(columnNameStr, ",");
                    StrUtil.trim(indexColumns);
                    Map<String, String> columns = new LinkedHashMap<>(indexColumns.length);
                    for (String indexColumn : indexColumns) {
                        List<String> split = StrUtil.split(indexColumn, " ");
                        if (split.size() > 1) {
                            columns.put(split.get(0), split.get(1));
                        } else {
                            columns.put(split.get(0), Sort.ASC.name());
                        }
                    }
                    resultMap.put("column", CollectionUtil.join(columns.keySet(), ","));
                    resultMap.put("columns", columns);

                    if (StringUtils.endsWithIgnoreCase(indexName, "_pkey")) {
                        // 主键
                        resultMap.put("isUnique", true);
                        resultMap.put("isPrimaryKey", true);
                    } else {
                        resultMap.put("isUnique", StringUtils.startsWithIgnoreCase(String.valueOf(indexSql), "CREATE UNIQUE INDEX"));
                        resultMap.put("isPrimaryKey", false);
                    }

                }
            }
        }
        return queryMethodResult;
    }

    @Override
    protected QueryMethodResult createTableMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {

        boolean isNotExists = queryEntity.getAniCreateTable().isNotExists();
        if(isNotExists) {
            if(tableExists(connect, databaseConf, tableConf)) {
                return new QueryMethodResult();
            }
        }

        AniHandler.AniCreateTable aniCreateTable = queryEntity.getAniCreateTable();

        String sql = aniCreateTable.getSql();
        List<String> fieldCommentList = aniCreateTable.getFieldCommentList();
        String tableComment = aniCreateTable.getTableComment();
        List<String> sequenceSqlList = aniCreateTable.getSequenceSqlList();
        List<String> partitionSqlList = aniCreateTable.getPartitionSqlList();
        List<String> onUpdateFuncSqlList = aniCreateTable.getOnUpdateFuncSqlList();

        executeByConnection(connect, databaseConf, (connection)->{
            Statement statement = null;
            try {
                statement = connection.createStatement();
                //序列
                if(CollectionUtil.isNotEmpty(sequenceSqlList)) {
                    for (String sequenceSql : sequenceSqlList) {
                        recordExecuteStatementLog(sequenceSql, null);
                        statement.execute(sequenceSql);
                    }
                }
                if (log.isDebugEnabled()) {
                    log.debug("create table sql: {}", sql);
                }
                recordExecuteStatementLog(sql, null);
                statement.execute(sql);
                if (CollectionUtil.isNotEmpty(fieldCommentList)) {
                    for (int i = 0; i < fieldCommentList.size(); i++) {
                        String fieldCommentSql = fieldCommentList.get(i);
                        if (log.isDebugEnabled()) {
                            log.debug("create comment sql: {}", fieldCommentSql);
                        }
                        recordExecuteStatementLog(fieldCommentSql, null);
                        statement.execute(fieldCommentSql);
                    }
                }
                if (StringUtils.isNotBlank(tableComment)) {
                    if (log.isDebugEnabled()) {
                        log.debug("create comment sql: {}", tableComment);
                    }
                    recordExecuteStatementLog(tableComment, null);
                    statement.execute(tableComment);
                }

                if(CollectionUtil.isNotEmpty(partitionSqlList)) {
                    for (String partitionSql : partitionSqlList) {
                        if (log.isDebugEnabled()) {
                            log.debug("创建分区表相关sql: {}", partitionSql);
                        }
                        recordExecuteStatementLog(partitionSql, null);
                        statement.execute(partitionSql);
                    }
                }

                if(CollectionUtil.isNotEmpty(onUpdateFuncSqlList)) {
                    for (String funcSql : onUpdateFuncSqlList) {
                        if (log.isDebugEnabled()) {
                            log.debug("创建触发器相关sql: {}", funcSql);
                        }
                        recordExecuteStatementLog(funcSql, null);
                        statement.execute(funcSql);
                    }
                }

            }catch (Exception e) {
                throw new RuntimeException(e);
            }finally {
                if (statement != null) {
                    try {
                        statement.close();
                    } catch (SQLException e) {
                        log.error("postGre ResultSet 对象关闭失败!", e);
                    }
                }
            }
        });

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

        BasePostGreParamUtil paramUtil = (BasePostGreParamUtil) getDbModuleParamUtil();
        //是否忽略大小写
        boolean ignoreCase = databaseConf.isIgnoreCase();
        //表名
        String tableName = paramUtil.getTableName(databaseConf.getSchema(), tableConf.getTableName(), ignoreCase);

        String dropTableSQL = String.format(DROP_TABLE_SQL, tableName);
        int update = updateExecute(databaseConf, connect, dropTableSQL, new Object[0]);

        //查序列
        String schema = databaseConf.getSchema();
        String table = tableConf.getTableName();
        if(ignoreCase) {
            schema = schema.toLowerCase();
            table = table.toLowerCase();
        }
        List<Map<String, Object>> sequenceResult = queryExecute(databaseConf, connect, SHOW_SEQUENCE_SQL, new MapListHandler(), new Object[]{schema, table + "\\_%"});

        //删序列
        if(CollectionUtil.isNotEmpty(sequenceResult)) {
            for (Map<String, Object> sequence : sequenceResult) {
                String sequenceName = (String) sequence.values().iterator().next();
                sequenceName = paramUtil.getTableName(databaseConf.getSchema(), sequenceName, ignoreCase);
                String dropSequenceSQL = String.format(DROP_SEQUENCE_SQL, sequenceName);
                updateExecute(databaseConf, connect, dropSequenceSQL, new Object[0]);
            }
        }


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
        updateExecute(databaseConf, connect, sql, new Object[0]);
        return new QueryMethodResult();
    }

    @Override
    protected QueryMethodResult monitorStatusMethod(S connect, D databaseConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "PostGreServiceImpl.monitorStatusMethod");
    }

    @Override
    protected QueryMethodResult querySchemaMethod(S connect, D databaseConf, T tableConf) throws Exception {
        List<Map<String, Object>> schemaInfoList = new ArrayList<>();

        executeByConnection(connect, databaseConf, (connection)->{
            ResultSet primaryKeys = null;
            ResultSet columnSet = null;
            try {
                // 获取主键
                primaryKeys = connection.getMetaData().getPrimaryKeys(null, databaseConf.getSchema(), tableConf.getTableName());
                Map<String, String> primaryKeyMap = new HashMap<>();
                while (primaryKeys.next()) {
                    String primaryKey = primaryKeys.getString("COLUMN_NAME");
                    String pkName = primaryKeys.getString("pk_name");
                    primaryKeyMap.put(primaryKey, pkName);
                }
                columnSet = connection.getMetaData().getColumns(null, databaseConf.getSchema(), tableConf.getTableName(), "%");

                while (columnSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    String columnName = columnSet.getString("COLUMN_NAME");
                    map.put("col_name", columnName);
                    int isKey = 0;
                    if (primaryKeyMap.get(columnName) != null) {
                        //主键
                        isKey = 1;
                        map.put("pkName", primaryKeyMap.get(columnName));
                    }
                    map.put("isKey", isKey);
                    String columnDesc = columnSet.getString("REMARKS");
                    columnDesc = columnDesc == null ? "" : columnDesc;
                    map.put("comment", columnDesc);
                    String columnType = columnSet.getString("TYPE_NAME").toLowerCase();
                    // 自增情况下 int4 为 serial 类型
                    if (StringUtils.equalsIgnoreCase(columnType, "serial")) {
                        columnType = "int4";
                    } else if (StringUtils.startsWithIgnoreCase(columnType, "_")) {
                        // 表示当前类型为数组
                        columnType = StringUtils.substring(columnType, 1, columnType.length()) + "[]";
                    }
                    map.put("data_type", columnType);
                    map.put("std_data_type", BasePostGreFieldTypeEnum.dbFieldType2FieldType(columnType).getVal());
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
                    int nullableInt = columnSet.getInt("NULLABLE");
                    map.put("nullable", nullableInt);
                    String columnDef = columnSet.getString("COLUMN_DEF");
                    columnDef = columnDef == null ? "" : columnDef;
                    if (StringUtils.startsWithIgnoreCase(columnDef, "nextval(") && isKey == 1) {
                        map.put("autoincrement", 1);
                        columnDef = "";
                    } else {
                        map.put("autoincrement", 0);
                    }

                    map.put("columnDef", columnDef);
                    schemaInfoList.add(map);
                }
            }catch (Exception e) {
                e.printStackTrace();
                new RuntimeException(e.getCause());
            } finally {
                if (columnSet != null) {
                    try {
                        columnSet.close();
                    } catch (SQLException e) {
                        log.error("postGre ResultSet 对象关闭失败!", e);
                    }
                }

                if (primaryKeys != null) {
                    try {
                        primaryKeys.close();
                    } catch (SQLException e) {
                        log.error("postGre ResultSet 对象关闭失败!", e);

                    }
                }
            }
        });

        return new QueryMethodResult(schemaInfoList.size(), schemaInfoList);
    }

    @Override
    protected QueryMethodResult queryDatabaseSchemaMethod(S connect, D databaseConf, T tableConf) throws Exception {
        List<Map<String, Object>> schemasList = new ArrayList<>();
        executeByConnection(connect, databaseConf, (connection)->{
            ResultSet schemasSet = null;
            try {
                schemasSet = connection.getMetaData().getSchemas();
                while (schemasSet.next()) {
                    String tableSchem = schemasSet.getString("TABLE_SCHEM");
                    if(!("pg_catalog".equalsIgnoreCase(tableSchem)
                            || "information_schema".equalsIgnoreCase(tableSchem))) {
                        Map<String, Object> map = new HashMap<>();
                        map.put("schema", tableSchem);
                        map.put("catalog", schemasSet.getString("TABLE_CATALOG"));
                        schemasList.add(map);
                    }

                }
            } catch (SQLException e) {
                e.printStackTrace();
                throw new RuntimeException(e.getCause());
            }finally {
                if(schemasSet != null) {
                    try {
                        schemasSet.close();
                    } catch (SQLException throwables) {
                        throwables.printStackTrace();
                    }
                }
            }
        });
        
        return new QueryMethodResult(schemasList.size(), schemasList);
    }

    @Override
    protected QueryMethodResult saveOrUpdateMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        String sql = queryEntity.getAniUpsert().getSql();
        Object[] paramArray = queryEntity.getAniUpsert().getParamArray();
        int update = updateExecute(databaseConf, connect, sql, paramArray);
        return new QueryMethodResult(update, null);
    }

    @Override
    protected QueryMethodResult saveOrUpdateBatchMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        List<String> sqlList = queryEntity.getAniUpsertBatch().getSql();
        List<Object[]> paramArray = queryEntity.getAniUpsertBatch().getParamArray();
        int update = updateExecute(databaseConf, connect, sqlList.get(0), paramArray.get(0));
        return new QueryMethodResult(update, null);
    }


    private void executeByConnection(S connect, D databaseConf, Consumer<Connection> consumer) throws SQLException {
        Connection connection = getConnection(databaseConf, connect);
        boolean isClose = false;
        if(connection == null) {
            connection = connect.getDataSource().getConnection();
            connection.setAutoCommit(false);
            isClose = true;
        }

        try {
            consumer.accept(connection);
            if(isClose) {
                connection.commit();
            }
        }catch (Exception e) {
          if(isClose) {
              connection.rollback();
          }
          throw e;
        } finally {
            if(isClose) {
                connection.close();
            }
        }


    }

    @Override
    protected QueryMethodResult alterTableMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {


        List<String> sqlList = queryEntity.getAniAlterTable().getSqlList();
        boolean delPrimaryKey = queryEntity.getAniAlterTable().isDelPrimaryKey();

        executeByConnection(connect, databaseConf, (connection)->{
            Statement statement = null;
            try {
                statement = connection.createStatement();
                for (String sql : sqlList) {
                    sql = StringUtils.replace(sql, "${tableName}", jointTable(databaseConf.getSchema(), tableConf.getTableName()));
                    if (log.isDebugEnabled()) {
                        log.debug("alterTable 执行修改的sql是: {}", sql);
                    }
                    recordExecuteStatementLog(sql, null);
                    statement.execute(sql);
                }

                if(delPrimaryKey) {
                    String primaryKeySql = getPrimaryKeySql(connect, databaseConf, tableConf);
                    if(primaryKeySql != null) {
                        if (log.isDebugEnabled()) {
                            log.debug("alterTable 执行修改的sql是: {}", primaryKeySql);
                        }
                        recordExecuteStatementLog(primaryKeySql, null);
                        statement.execute(primaryKeySql);
                    }else {
                        log.warn("postGre 删除主键失败!, 没有设置主键");
                    }
                }
            } catch (Exception e) {
                throw new BusinessException(e);
            }finally {
                if (statement != null) {
                    try {
                        statement.close();
                    } catch (SQLException e) {
                        log.error("postGre ResultSet 对象关闭失败!", e);
                    }
                }
            }
        });

        return new QueryMethodResult();
    }

    private String getPrimaryKeySql(S connect, D databaseConf, T tableConf) throws Exception {
        BasePostGreParamUtil paramUtil = (BasePostGreParamUtil) getDbModuleParamUtil();
        QueryMethodResult queryMethodResult = querySchemaMethod(connect, databaseConf, tableConf);
        List<Map<String, Object>> rows = queryMethodResult.getRows();
        String primaryKeyName = "";
        for (Map<String, Object> row : rows) {
            if(row.get("isKey").equals(1)) {
                primaryKeyName = (String) row.get("pkName");
                break;
            }
        }
        if(StringUtils.isNotBlank(primaryKeyName)) {
            boolean ignoreCase = databaseConf.isIgnoreCase();
            String tableName = paramUtil.getTableName(databaseConf.getSchema(), tableConf.getTableName(), ignoreCase);
            String pkName = paramUtil.fieldHandler(primaryKeyName, ignoreCase);
            return String.format(DROP_PRIMARY_KEY, tableName, pkName);
        }
        return null;
    }

    @Override
    protected QueryMethodResult tableExistsMethod(S connect, D databaseConf, T tableConf) throws Exception {
        String sql = StringUtils.replaceEach(EXISTS_TABLE_SQL,
                new String[]{"${schema}", "${tableName_low}", "${tableName_up}", "${tableName}"},
                new String[]{databaseConf.getSchema(), tableConf.getTableName().toLowerCase(),
                        tableConf.getTableName().toUpperCase(), tableConf.getTableName()});
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

        Map<String, Object> dataMap = new HashMap<>();

        String sql = "select relkind, reltuples from pg_class where relname = '%s' and relnamespace in (select oid from pg_namespace where nspname = '%s')";
        Map<String, Object> query = queryExecute(databaseConf, connect, String.format(sql, tableConf.getTableName(), databaseConf.getSchema()) , new MapHandler(), new Object[0]);

        Object relkind = query.get("relkind");
        Object reltuples = query.get("reltuples");

        if("r".equals(relkind)) {
            relkind = "Normal";
        }else if("p".equals(relkind)){
            relkind = "Partitioned";
        }

        dataMap.put("rowCount", reltuples);
        dataMap.put("tableType", relkind);

        sql = "SELECT obj_description('%s'::regclass, 'pg_class')";
        query = queryExecute(databaseConf, connect, String.format(sql, "\"" + databaseConf.getSchema() + "\".\"" + tableConf.getTableName()  + "\""), new MapHandler(), new Object[0]);

        dataMap.put("tableComment", query.get("obj_description"));

        TableInformation<Object> tableInformation = TableInformation.builder().tableName(tableConf.getTableName()).extendMeta(dataMap).build();

        return new QueryMethodResult(1, CollectionUtil.newArrayList(JsonUtil.entityToMap(tableInformation)));
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

    /**
     * 拼接表名
     *
     * @param schema
     * @param tableName
     * @return
     */
    private String jointTable(String schema, String tableName) {
        if (StringUtils.containsIgnoreCase(tableName, "-") || StringUtils.isNumeric(StringUtils.substring(tableName, 0, 1))) {
            tableName = "\"" + tableName + "\"";
        }
        return schema + "." + tableName;
    }

    @Override
    public QueryMethodResult queryBySqlMethod(D databaseConf, S dbConnect, String sql, List<Object> params) throws Exception {
        try {
            return super.queryBySqlMethod(databaseConf, dbConnect, sql, params);
        }catch (Exception e) {
            PostgreFunctionErrorHandlerInfoTips.printTips(e.getMessage());
            throw e;
        }
    }

    @Override
    protected SequenceHandler getSequenceHandler(D dataConf) {
        return new PostGreSequenceHandler(dataConf);
    }


    @Override
    protected QueryMethodResult createDatabaseMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        AniHandler.AniCreateDatabase createDatabase = queryEntity.getCreatedatabase();
        String dbName = createDatabase.getDbName();
        if(!databaseConf.isIgnoreCase()) {
            dbName = "\"" + dbName + "\"";
        }
        String sql = String.format(CREATE_DATABASE, dbName);

        int update = updateExecute(databaseConf, connect, sql, new Object[0]);
        return new QueryMethodResult(update, null);
    }

    @Override
    protected QueryMethodResult dropDatabaseMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        AniHandler.AniDropDatabase dropDatabase = queryEntity.getDropdatabase();
        String dbName = dropDatabase.getDbName();
        if(!databaseConf.isIgnoreCase()) {
            dbName = "\"" + dbName + "\"";
        }
        String sql = String.format(DROP_DATABASE, dbName);
        int update = updateExecute(databaseConf, connect, sql, new Object[0]);
        return new QueryMethodResult(update, null);
    }

    @Override
    protected QueryMethodResult updateDatabaseMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        AniHandler.AniUpdateDatabase updateDatabase = queryEntity.getUpdatedatabase();
        String dbName = updateDatabase.getDbName();
        String newDbName = updateDatabase.getNewDbName();
        if(!databaseConf.isIgnoreCase()) {
            dbName = "\"" + dbName + "\"";
            newDbName = "\"" + newDbName + "\"";
        }
        String sql = String.format(UPDATE_DATABASE, dbName, newDbName);
        int update = updateExecute(databaseConf, connect, sql, new Object[0]);
        return new QueryMethodResult(update, null);
    }

    @Override
    protected Integer insertReturnValue(Connection connection, D databaseConf, T tableConf, Q queryEntity, String sql, List<Map<String, Object>> keys) throws Exception {

        String primaryKeyField = getPrimaryKeyField(databaseConf, tableConf);
        // 替换自增主键占位符
        if (queryEntity.getAniInsert().isReturnGeneratedKey()) {
            if (StringUtils.isNotBlank(primaryKeyField)) {
                BasePostGreParamUtil dbModuleParamUtil = (BasePostGreParamUtil) this.getDbModuleParamUtil();
                String field = dbModuleParamUtil.fieldHandler(primaryKeyField, databaseConf.isIgnoreCase());
                sql = StringUtils.replace(sql, "${@@IDENTITY}", field);
            } else {
                throw new BusinessException("未查询到 " + tableConf.getTableName() + " 对应的自增主键信息!");
            }
        }
        CallableStatement callStatement = null;
        Integer addTotal = queryEntity.getAniInsert().getAddTotal();
        List<String> returnFields = queryEntity.getAniInsert().getReturnFields();
        boolean returnGeneratedKey = queryEntity.getAniInsert().isReturnGeneratedKey();
        try {
            callStatement = connection.prepareCall(sql);
            //设置参数
            statementSetObject(queryEntity.getAniInsert().getParamArray(), callStatement);
            boolean execute = callStatement.execute();

            if (returnGeneratedKey) {
                if (addTotal == 1) {
                    // 单条
                    ResultSet resultSet = callStatement.getResultSet();
                    if(resultSet.next()){
                        Map<String, Object> columnMap = new LinkedHashMap<>();
                        keys.add(columnMap);
                        columnMap.put("GENERATED_KEY", resultSet.getObject(primaryKeyField));
                    }
                    resultSet.close();
                }
            } else {
                if (addTotal == 1) {
                    // 单条
                    ResultSet resultSet = callStatement.getResultSet();
                    if(resultSet.next()){
                        Map<String, Object> columnMap = new LinkedHashMap<>();
                        keys.add(columnMap);
                        for (String returnField : returnFields) {
                            columnMap.put(returnField, resultSet.getObject(returnField));
                        }
                    }
                    resultSet.close();
                }
            }

            return addTotal;
        } finally {
            if (callStatement != null) callStatement.close();
        }

    }


    /**
     * 获取字段类型映射信息
     *
     * @param databaseConf
     * @param tableConf
     * @return
     */
    private String getPrimaryKeyField(D databaseConf, T tableConf) {
        BasePostGreConfigHelper helper = (BasePostGreConfigHelper) this.getHelper();
        // 获取表字段映射
        return helper.getPrimaryKeyField(databaseConf, tableConf, new Function<String, String>() {
            @Override
            public String apply(String cacheKey) {
                S dbConnect = (S) helper.getDbConnect(cacheKey);
                try {
                    QueryMethodResult result = querySchemaMethod(dbConnect, databaseConf, tableConf);
                    List<Map<String, Object>> rows = result.getRows();
                    if (CollectionUtil.isNotEmpty(rows)) {
                        // 获取自增字段信息
                        for (Map<String, Object> row : rows) {
                            Integer autoincrement = row.get("autoincrement") == null ? null : (Integer) row.get("autoincrement");
                            Integer isKey = row.get("isKey") == null ? null : (Integer) row.get("isKey");
                            if (autoincrement != null && autoincrement.equals(1) && isKey != null && isKey.equals(1)) {
                                return (String) row.get("col_name");
                            }
                        }
                    }
                    return null;
                } catch (Exception e) {
                    throw new BusinessException("查询 " + tableConf.getTableName() + " 对应的自增主键信息异常!", e);
                }
            }
        });
    }

    @Override
    protected QueryCursorMethodResult queryCursorMethod(S connect, Q queryEntity, D databaseConf, T tableConf, C cursorCache, Consumer<Map<String, Object>> consumer, Boolean rollAllData) throws Exception {
        if (cursorCache == null) {
            AniHandler.AniQuery aniQuery = queryEntity.getAniQuery();
            String sql = aniQuery.getSql();
            String sqlCount = aniQuery.getSqlCount();
            Object[] paramArray = aniQuery.getParamArray();
            sql = formatSql(sql, databaseConf, tableConf);
            sqlCount = formatSql(sqlCount, databaseConf, tableConf);
            queryEntity.setQueryStr(transitionQueryStr(sql, paramArray));
            Long total = 0L;
            if (aniQuery.getCount()) {
                Map<String, Object> totalMap = connect.query(sqlCount, new MapHandler(), paramArray);
                total = Long.parseLong(totalMap.get("count").toString());
            }
            Connection conn = null;
            PreparedStatement stmt = null;
            ResultSet resultSet = null;
            try {
                // 根据游标遍历数据
                conn = connect.getDataSource().getConnection();
                conn.setAutoCommit(false);
                stmt = conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                if(aniQuery.getBatchSize() != null) {
                    stmt.setFetchSize(aniQuery.getBatchSize());
                }
                stmt.setFetchDirection(ResultSet.FETCH_FORWARD);
                // 游标超时时间
                stmt.setQueryTimeout(172800);
                // 若存在占位符，则设置参数
                if (ArrayUtil.isNotEmpty(paramArray)) {
                    connect.fillStatement(stmt, paramArray);
                }
                // 执行语句
                resultSet = stmt.executeQuery();
                // 若为游标全量查询，且带有位移，则设置游标直接滚动到指定位移
                if (aniQuery.getLimit().equals(Page.LIMIT_ALL_DATA) && aniQuery.getOffset() != null && aniQuery.getOffset() > 0) {
                    resultSet.absolute(aniQuery.getOffset());
                }
            } catch (Exception e) {
                if (conn != null) {
                    try {
                        conn.commit();
                    } catch (SQLException e1) {
                        log.error("执行事物提交失败!", e);
                    }
                }
                if (resultSet != null) {
                    try {
                        resultSet.close();
                    } catch (SQLException e2) {
                        log.error("ResultSet 对象关闭失败!", e);
                    }
                }
                if (stmt != null) {
                    try {
                        stmt.close();
                    } catch (SQLException e3) {
                        log.error("PreparedStatement 对象关闭失败!", e);
                    }
                }
                if (conn != null) {
                    try {
                        conn.close();
                    } catch (SQLException e4) {
                        log.error("conn 对象关闭失败!", e);
                    }
                }
                throw e;
            }
            cursorCache = (C) new RdbmsCursorCache(null, aniQuery.getBatchSize(), conn, stmt, resultSet);
            return getQueryCursorMethodResult(databaseConf, consumer, rollAllData, total, aniQuery.getBatchSize(), cursorCache);
        } else {
            return getQueryCursorMethodResult(databaseConf, consumer, rollAllData, 0L, cursorCache.getBatchSize(), cursorCache);
        }
    }
}
