package com.meiya.whalex.db.module.ani;

import cn.hutool.core.collection.CollectionUtil;
import com.meiya.whalex.db.entity.QueryMethodResult;
import com.meiya.whalex.db.entity.ani.*;
import com.meiya.whalex.db.error.DmFunctionErrorHandlerInfoTips;
import com.meiya.whalex.db.util.param.impl.ani.BaseDmParamUtil;
import com.meiya.whalex.exception.BusinessException;
import com.meiya.whalex.exception.ExceptionCode;
import com.meiya.whalex.sql.ani.RdbmsCursorCache;
import com.meiya.whalex.sql.module.AbstractRdbmsModuleBaseService;
import com.meiya.whalex.sql.module.SequenceHandler;
import com.meiya.whalex.sql.module.SqlParseHandler;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.dbutils.BasicRowProcessor;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.MapHandler;
import org.apache.commons.dbutils.handlers.MapListHandler;
import org.apache.commons.lang3.StringUtils;

import java.sql.*;
import java.util.*;
import java.util.function.Consumer;

/**
 * Dm 服务
 *
 * @author 黄河森
 * @date 2022/5/23
 * @project whale-cloud-platformX
 */
@Slf4j
public class BaseDmServiceImpl<S extends QueryRunner, Q extends AniHandler, D extends BaseDmDatabaseInfo, T extends BaseDmTableInfo, C extends RdbmsCursorCache>
        extends AbstractRdbmsModuleBaseService<S, Q, D, T, C> {

    /**
     * 查询库中表信息SQL
     */
    private static final String SHOW_TABLE_SQL = "select table_name,owner FROM all_tables where owner = ? and table_name like ?";

    /**
     * 查询表索引信息SQL
    **/
    private static final String SHOW_INDEX_SQL = "SELECT a.*, b.CONSTRAINT_TYPE, c.UNIQUENESS FROM (SELECT INDEX_NAME, COLUMN_NAME, DESCEND FROM DBA_IND_COLUMNS WHERE table_owner = '%s' AND table_name = '%s') a\n" +
            "LEFT JOIN DBA_CONSTRAINTS b ON b.INDEX_NAME = a.INDEX_NAME AND b.CONSTRAINT_TYPE = 'P' AND b.OWNER = '%s' AND b.TABLE_NAME = '%s'\n" +
            "LEFT JOIN DBA_INDEXES c ON c.INDEX_NAME = a.INDEX_NAME AND c.table_owner = '%s' AND c.table_name = '%s'";

    /**
     * 删表SQL
     */
    private static final String DROP_TABLE_SQL = "DROP TABLE %s";

    /**
     * 查看序列SQL
     */
    private static final String SHOW_SEQUENCE_SQL = "SELECT SEQUENCE_NAME FROM DBA_SEQUENCES WHERE  SEQUENCE_OWNER= ? AND SEQUENCE_NAME LIKE ?";

    /**
     * 删除序列
     */
    private static final String DROP_SEQUENCE_SQL = "DROP SEQUENCE %s";

    /**
     * 判断表是否存在
     */
    private static final String EXISTS_TABLE_SQL = "select count(1) as \"count\" FROM all_tables where owner = ? and table_name = ?";

    /**
     * 查询所有的数据库列表
     */
    private static final String SHOW_DATABASE_SQL = "SELECT NAME as \"Database\" FROM v$database";

    /**
     * 查询数据库信息
     */
    private static final String SHOW_DATABASE_INFO_SQL = "SELECT * FROM v$database where NAME = ?";

    @Override
    protected BasicRowProcessor getRowProcessor(D databaseConf) {
        return new BaseDmRowProcessor();
    }

    @Override
    protected String formatSql(String sql, D databaseConf, T tableConf) {
        return sql;
    }

    @Override
    protected SqlParseHandler getSqlParseHandler(D database) {
        return new DmSqlParseHandler(database);
    }

    @Override
    protected QueryMethodResult insertMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        QueryMethodResult queryMethodResult = super.insertMethod(connect, queryEntity, databaseConf, tableConf);
        Integer addTotal = queryEntity.getAniInsert().getAddTotal();
        queryMethodResult.setTotal(addTotal);
        return queryMethodResult;
    }

    @Override
    protected Integer insertReturnValue(Connection connection, D databaseConf, T tableConf, Q queryEntity, String sql, List<Map<String, Object>> keys) throws Exception {
        PreparedStatement statement = null;
        ResultSet generatedKeys = null;
        int update = 0;
        try {
            statement = connection.prepareStatement(sql);

            //设置参数
            statementSetObject(queryEntity.getAniInsert().getParamArray(), statement);

            update = statement.executeUpdate();
            generatedKeys = statement.getResultSet();
            ResultSetMetaData metaData = generatedKeys.getMetaData();
            int columnCount = metaData.getColumnCount();
            while (generatedKeys.next()) {
                Map<String, Object> columnMap = new LinkedHashMap<>();
                keys.add(columnMap);
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metaData.getColumnName(i);
                    Object object = generatedKeys.getObject(i);
                    columnMap.put(columnName, object);
                }
            }
            return update;
        }finally {
            if(generatedKeys != null) generatedKeys.close();
            if(statement != null) statement.close();
        }
    }

    @Override
    protected QueryMethodResult showTablesMethod(S connect, Q queryEntity, D databaseConf) throws Exception {
        //是否忽略大小写
        boolean ignoreCase = databaseConf.isIgnoreCase();
        String schema = databaseConf.getSchema();
        String tableMatch = queryEntity.getListTable().getTableMatch();
        if (StringUtils.isBlank(tableMatch)) {
            tableMatch = "%%";
        }

        if(ignoreCase) {
            schema  = schema.toUpperCase();
            tableMatch = tableMatch.toUpperCase();
        }

        List<Map<String, Object>> query = queryExecute(databaseConf, connect, SHOW_TABLE_SQL, new MapListHandler(), new Object[]{schema, tableMatch});
        for (Map<String, Object> map : query) {
            map.put("tableName", map.get("TABLE_NAME"));
            map.remove("TABLE_NAME");
            map.put("tableComment", map.get("TABLE_COMMENT"));
            map.remove("TABLE_COMMENT");
            map.put("schemaName", map.get("OWNER"));
        }
        return new QueryMethodResult(query.size(), query);
    }

    @Override
    protected QueryMethodResult getIndexesMethod(S connect, D databaseConf, T tableConf) throws Exception {

        QueryMethodResult queryMethodResult = new QueryMethodResult();
        List<Map<String, Object>> dataList = new ArrayList<>();
        queryMethodResult.setRows(dataList);

        //是否忽略大小写
        boolean ignoreCase = databaseConf.isIgnoreCase();
        String schema = databaseConf.getSchema();
        String tableName = tableConf.getTableName();
        if(ignoreCase) {
            schema = schema.toUpperCase();
            tableName = tableName.toUpperCase();
        }

        String showIndexSql = String.format(SHOW_INDEX_SQL, schema, tableName, schema, tableName, schema, tableName);
        List<Map<String, Object>> queryList = queryExecute(databaseConf, connect, showIndexSql, new MapListHandler(), new Object[0]);
        if (CollectionUtil.isNotEmpty(queryList)) {
            String primaryKey = null;
            Map<String, Map<String, Object>> indexesMap = new LinkedHashMap<>();
            Map<String, Boolean> isUniqueMap = new HashMap<>();
            for (int i = 0; i < queryList.size(); i++) {
                Map<String, Object> map = queryList.get(i);
                String columnName = (String) map.get("COLUMN_NAME");
                String indexName = (String) map.get("INDEX_NAME");
                Object sort = map.get("DESCEND");
                Object constraintType = map.get("CONSTRAINT_TYPE");
                if (constraintType != null) {
                    primaryKey = indexName;
                }
                String isUnique = (String) map.get("UNIQUENESS");
                isUniqueMap.put(indexName, StringUtils.equalsIgnoreCase(isUnique, "UNIQUE"));
                Map<String, Object> indexMap = indexesMap.get(indexName);
                if (indexMap == null) {
                    indexMap = new LinkedHashMap<>();
                    indexesMap.put(indexName, indexMap);
                }
                indexMap.put(columnName, sort);
            }

            for (Map.Entry<String, Map<String, Object>> entry : indexesMap.entrySet()) {
                Map<String, Object> resultMap = new HashMap<>(1);
                dataList.add(resultMap);
                resultMap.put("indexName", entry.getKey());
                Map<String, Object> value = entry.getValue();
                resultMap.put("column", CollectionUtil.join(value.keySet(), ","));
                resultMap.put("columns", value);
                if (StringUtils.isNotBlank(primaryKey) && StringUtils.equalsIgnoreCase(entry.getKey(), primaryKey)) {
                    // 主键
                    resultMap.put("isUnique", true);
                    resultMap.put("isPrimaryKey", true);
                } else {
                    resultMap.put("isUnique", isUniqueMap.get(entry.getKey()));
                    resultMap.put("isPrimaryKey", false);
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

        String sql = queryEntity.getAniCreateTable().getSql();
        //Dm 注释和建表不能同时完成，分开多条sql执行
        String[] split = sql.split("\n");
        for (String item : split){
            if(StringUtils.isBlank(item)){
                continue;
            }
            if (log.isDebugEnabled()) {
                log.debug("执行 sql: {}", item);
            }
            updateExecute(databaseConf, connect, item, new Object[0]);
        }
        return new QueryMethodResult();
    }

    @Override
    protected QueryMethodResult dropTableMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {

        //是否忽略大小写
        boolean ignoreCase = databaseConf.isIgnoreCase() && tableConf.isIgnoreCase();
        BaseDmParamUtil paramUtil = (BaseDmParamUtil) getDbModuleParamUtil();
        String tableName = paramUtil.getBaseDmParserUtil().getTableName(databaseConf.getSchema(), tableConf.getTableName(), ignoreCase);
        //删除表
        String dropTableSQL = String.format(DROP_TABLE_SQL, tableName);
        int update = updateExecute(databaseConf, connect, dropTableSQL, new Object[0]);

        //查序列
//        String schema = databaseConf.getSchema();
//        String table = tableConf.getTableName();
//        if(ignoreCase) {
//            schema = schema.toUpperCase();
//            table = table.toUpperCase();
//        }
//        List<Map<String, Object>> sequenceResult = queryExecute(databaseConf, connect, SHOW_SEQUENCE_SQL, new MapListHandler(), new Object[]{schema, table + "_%"});

        //删序列
//        if(CollectionUtil.isNotEmpty(sequenceResult)) {
//            for (Map<String, Object> sequence : sequenceResult) {
//                String sequenceName = (String) sequence.values().iterator().next();
//                sequenceName = paramUtil.getBaseDmParserUtil().getTableName(databaseConf.getSchema(), sequenceName, ignoreCase);
//                String dropSequenceSQL = String.format(DROP_SEQUENCE_SQL, sequenceName);
//                updateExecute(databaseConf, connect, dropSequenceSQL, new Object[0]);
//            }
//        }
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
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "DmServiceImpl.monitorStatusMethod");
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
    protected QueryMethodResult querySchemaMethod(S connect, D databaseConf, T tableConf) throws Exception {
        List<Map<String, Object>> schemaInfoList = new ArrayList<>();

        executeByConnection(connect, databaseConf, (connection)->{
            ResultSet primaryKeys = null;
            ResultSet columnSet = null;
            String tableName = tableConf.getTableName();
            try {

                // 获取主键
                primaryKeys = connection.getMetaData().getPrimaryKeys(databaseConf.getDatabaseName(), databaseConf.getSchema(), tableName);
                Map<String, String> primaryKeyMap = new HashMap<>();
                while (primaryKeys.next()) {
                    String primaryKey = primaryKeys.getString("COLUMN_NAME");
                    String pkName = primaryKeys.getString("pk_name");
                    primaryKeyMap.put(primaryKey, pkName);
                }
                columnSet = connection.getMetaData().getColumns(databaseConf.getDatabaseName(), databaseConf.getSchema(), tableName, "%");

                while (columnSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    String columnName = columnSet.getString("COLUMN_NAME");
                    map.put("col_name", columnName);
                    int isKey = 0;
                    if (primaryKeyMap.containsKey(columnName)) {
                        //主键
                        isKey = 1;
                        map.put("pkName", primaryKeyMap.get(columnName));
                    }
                    map.put("isKey", isKey);
                    String columnDesc = columnSet.getString("REMARKS");
                    columnDesc = columnDesc == null ? "" : columnDesc;
                    map.put("comment", columnDesc);
                    String columnType = columnSet.getString("TYPE_NAME").toLowerCase();
                    map.put("data_type", columnType);
                    map.put("std_data_type", BaseDmFieldTypeEnum.dbFieldType2FieldType(columnType).getVal());
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

                    if (StringUtils.endsWithIgnoreCase(columnDef, ".NEXTVAL") && isKey == 1) {
                        map.put("autoincrement", 1);
                        columnDef = "";
                    } else {
                        map.put("autoincrement", 0);
                    }

                    map.put("columnDef", columnDef);
                    schemaInfoList.add(map);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }finally {
                if (columnSet != null) {
                    try {
                        columnSet.close();
                    } catch (SQLException e) {
                        log.error("Dm ResultSet 对象关闭失败!", e);
                    }
                }

                if (primaryKeys != null) {
                    try {
                        primaryKeys.close();
                    } catch (SQLException e) {
                        log.error("Dm ResultSet 对象关闭失败!", e);

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
    protected QueryMethodResult saveOrUpdateBatchMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        Connection connection = this.getConnection(databaseConf, connect);
        AniHandler.AniUpsertBatch aniUpsertBatch = queryEntity.getAniUpsertBatch();
        List<String> sqlList = aniUpsertBatch.getSql();
        List<Object[]> paramList = aniUpsertBatch.getParamArray();
        int update = 0;
        if (Objects.isNull(connection)) {
            try {
                connection = connect.getDataSource().getConnection();
                connection.setAutoCommit(false);
                for (int i = 0; i < sqlList.size(); i++) {
                    String sql = sqlList.get(i);
                    Object[] params = paramList.get(i);
                    recordExecuteStatementLog(sql, params);
                    update = update +  connect.update(connection, sql, params);
                }
                connection.commit();
            } finally {
                if (connection != null) {
                    connection.close();
                }
            }
        } else {
            for (int i = 0; i < sqlList.size(); i++) {
                String sql = sqlList.get(i);
                Object[] params = paramList.get(i);
                recordExecuteStatementLog(sql, params);
                update = update +  connect.update(connection, sql, params);
            }
        }
        return new QueryMethodResult(update, null);
    }

    @Override
    protected QueryMethodResult alterTableMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        List<String> sqlList = queryEntity.getAniAlterTable().getSqlList();

        executeByConnection(connect, databaseConf, (connection)->{
            Statement statement = null;
            try {
                statement = connection.createStatement();
                for (String sql : sqlList) {
                    if (log.isDebugEnabled()) {
                        log.debug("alterTable 执行修改的sql是: {}", sql);
                    }
                    recordExecuteStatementLog(sql, new Object[0]);
                    statement.execute(sql);
                }
            }catch (Exception e)  {
                e.printStackTrace();
                throw new RuntimeException(e.getCause());
            }finally {
                if (statement != null) {
                    try {
                        statement.close();
                    } catch (SQLException e) {
                        log.error("MD ResultSet 对象关闭失败!", e);
                    }
                }
            }
        });



        return new QueryMethodResult();
    }

    @Override
    protected QueryMethodResult tableExistsMethod(S connect, D databaseConf, T tableConf) throws Exception {
        //是否忽略大小写
        boolean ignoreCase = databaseConf.isIgnoreCase();
        String schema = databaseConf.getSchema();
        String tableName = tableConf.getTableName();
        if(ignoreCase) {
            schema = schema.toUpperCase();
            tableName = tableName.toUpperCase();
        }
        Map<String, Object> query = queryExecute(databaseConf, connect, EXISTS_TABLE_SQL, new MapHandler(), new Object[] {schema, tableName});
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
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "BaseDmServiceImpl.queryTableInformationMethod");
    }

    @Override
    protected QueryMethodResult queryListDatabaseMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        AniHandler.AniListDatabase listDatabase = queryEntity.getListDatabase();
        String sql = SHOW_DATABASE_SQL;
        Object[] paramArray = new Object[0];
        if(StringUtils.isNotBlank(listDatabase.getDatabaseMatch())) {
            sql = sql + " WHERE NAME LIKE ?";
            paramArray = new Object[1];
            paramArray[0] = listDatabase.getDatabaseMatch();
        }

        List<Map<String, Object>> rows =  queryExecute(databaseConf, connect, sql, new MapListHandler(), paramArray);

        return new QueryMethodResult(rows.size(), rows);
    }

    @Override
    protected QueryMethodResult queryDatabaseInformationMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws SQLException {
        Object[] paramArray = new Object[1];
        paramArray[0] = databaseConf.getDatabaseName();

        List<Map<String, Object>> rows = queryExecute(databaseConf, connect, SHOW_DATABASE_INFO_SQL, new MapListHandler(), paramArray);
        return new QueryMethodResult(rows.size(), rows);
    }


    @Override
    public QueryMethodResult queryBySqlMethod(D databaseConf, S connect, String sql, List<Object> params) throws Exception {
        // 处理当值为 '' 时，达梦数据库会认为是一个 NULL，必须使用 IS NULL 进行筛选
        Iterator<Object> iterator = params.iterator();
        int i = 0;
        while (iterator.hasNext()) {
            i++;
            Object param = iterator.next();
            if (param instanceof String && StringUtils.isBlank((String) param)) {
                int index = StringUtils.ordinalIndexOf(sql, "?", i);
                String operation = StringUtils.substring(sql, index - 2, index - 1);
                if (StringUtils.equalsIgnoreCase(StringUtils.trim(operation), "=")) {
                    StringBuilder sqlBuilder = new StringBuilder(sql);
                    sql = sqlBuilder.replace(index - 2, index + 1, "IS NULL").toString();
                    iterator.remove();
                }
            }
        }
        try {
            List<Map<String, Object>> rows = queryExecute(databaseConf, connect, sql, new MapListHandler(new BaseDmRowProcessor()), params.toArray());
            return new QueryMethodResult(rows.size(), rows);
        }catch (Exception e) {
            String errorMessage = e.getMessage();
            DmFunctionErrorHandlerInfoTips.printTips(errorMessage);
            throw e;
        }
    }

    @Override
    protected SequenceHandler getSequenceHandler(D database) {
        return new DmSequenceHandler(database);
    }

    @Override
    protected QueryMethodResult saveOrUpdateMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        String sql = queryEntity.getAniUpsert().getSql();
        Object[] paramArray = queryEntity.getAniUpsert().getParamArray();
        int update = updateExecute(databaseConf, connect, sql, paramArray);

        return new QueryMethodResult(update, null);
    }
}
