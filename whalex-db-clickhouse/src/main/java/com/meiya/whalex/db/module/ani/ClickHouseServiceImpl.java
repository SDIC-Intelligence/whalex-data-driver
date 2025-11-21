package com.meiya.whalex.db.module.ani;

import cn.hutool.core.collection.CollectionUtil;
import com.meiya.whalex.annotation.DbService;
import com.meiya.whalex.annotation.Support;
import com.meiya.whalex.db.constant.SupportPower;
import com.meiya.whalex.db.entity.QueryMethodResult;
import com.meiya.whalex.db.entity.ani.*;
import com.meiya.whalex.sql.ani.RdbmsCursorCache;
import com.meiya.whalex.sql.module.AbstractRdbmsModuleBaseService;
import com.meiya.whalex.db.util.param.impl.ani.ClickHouseParamUtil;
import com.meiya.whalex.exception.BusinessException;
import com.meiya.whalex.exception.ExceptionCode;
import com.meiya.whalex.interior.db.constant.CloudVendorsEnum;
import com.meiya.whalex.interior.db.constant.DbResourceEnum;
import com.meiya.whalex.interior.db.constant.DbVersionEnum;
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

/**
 * @author 黄河森
 * @date 2022/7/6
 * @package com.meiya.whalex.db.module.ani
 * @project whalex-data-driver
 */
@DbService(dbType = DbResourceEnum.clickhouse, version = DbVersionEnum.CLICKHOUSE_22_2_2_1, cloudVendors = CloudVendorsEnum.OPEN)
@Support(value = {
        SupportPower.TEST_CONNECTION,
        SupportPower.CREATE,
        SupportPower.SEARCH,
        SupportPower.SHOW_SCHEMA,
        SupportPower.CREATE_TABLE,
        SupportPower.DROP_TABLE,
        SupportPower.MODIFY_TABLE,
        SupportPower.SHOW_TABLE_LIST
})
@Slf4j
public class ClickHouseServiceImpl extends AbstractRdbmsModuleBaseService<QueryRunner, AniHandler, ClickHouseDatabaseInfo, ClickHouseTableInfo, RdbmsCursorCache> {
    /**
     * 查询库中表信息SQL
     */
    private static final String SHOW_TABLE_SQL = "SELECT database, name, engine, storage_policy, comment FROM system.tables WHERE database = '${database}'";

    /**
     * 查询表索引信息SQL
     */
    private static final String SHOW_INDEX_SQL = "SELECT indexdef FROM pg_indexes WHERE (tablename='%s' OR tablename='%s') AND schemaname = '%s'";

    /**
     * 删表SQL
     */
    private static final String DROP_TABLE_SQL_CLUSTER = "DROP TABLE %s ON CLUSTER %s";
    private static final String DROP_TABLE_SQL = "DROP TABLE %s";

    /**
     * 判断表是否存在
     */
    private static final String EXISTS_TABLE_SQL = "SELECT count(*) AS count FROM system.tables WHERE database = '${database}' AND (name = '${tableName_low}' OR name = '${tableName_up}')";


    @Override
    protected QueryMethodResult showTablesMethod(QueryRunner connect, AniHandler queryEntity, ClickHouseDatabaseInfo databaseConf) throws Exception {
        String sql = StringUtils.replace(SHOW_TABLE_SQL, "${database}", databaseConf.getDatabase());
        if (StringUtils.isNotBlank(queryEntity.getListTable().getTableMatch())) {
            sql = sql + "AND name LIKE '" + queryEntity.getListTable().getTableMatch() + "'";
        }
        recordExecuteStatementLog(sql, null);
        List<Map<String, Object>> query = connect.query(sql, new MapListHandler());
        for (Map<String, Object> map : query) {
            map.put("tableName", map.remove("name"));
            map.put("tableComment", map.remove("comment"));
            map.put("storagePolicy", map.remove("storage_policy"));
        }
        return new QueryMethodResult(query.size(), query);
    }

    @Override
    protected QueryMethodResult getIndexesMethod(QueryRunner connect, ClickHouseDatabaseInfo databaseConf, ClickHouseTableInfo tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + ".getIndexesMethod");
    }

    @Override
    protected QueryMethodResult createTableMethod(QueryRunner connect, AniHandler queryEntity, ClickHouseDatabaseInfo databaseConf, ClickHouseTableInfo tableConf) throws Exception {

        boolean isNotExists = queryEntity.getAniCreateTable().isNotExists();
        if(isNotExists) {
            if(tableExists(connect, databaseConf, tableConf)) {
                return new QueryMethodResult();
            }
        }

        String createTableFieldSql = queryEntity.getAniCreateTable().getSql();
        String tableComment = queryEntity.getAniCreateTable().getTableComment();
        if (StringUtils.isBlank(tableComment)) {
            tableComment = tableConf.getTableName();
        }
        AniHandler.DistributedField distributedField = queryEntity.getAniCreateTable().getDistributedField();
        List<String> sqlList = new ArrayList<>();
        if (!tableConf.isOpenDistributed() && !tableConf.isOpenReplica()) {
            String engineSQL = EngineType.parserEngineSQL(tableConf.getEngine(), tableConf.getEngineParamMap());
            String sql = StringUtils.replaceEach(ClickHouseParamUtil.CREATE_TABLE_STAND_ALONE_TEMPLATE
                    , new String[]{"${database}", "${tableName}", "${fieldSet}", "${engine}", "${tableComment}"}
                    , new String[]{databaseConf.getDatabase(), tableConf.getTableName(), createTableFieldSql, engineSQL, tableComment});
            sqlList.add(sql);
        } else if (tableConf.isOpenDistributed() && !tableConf.isOpenReplica()) {
            String engineSQL = EngineType.parserEngineSQL(tableConf.getEngine(), tableConf.getEngineParamMap());
            String sql = StringUtils.replaceEach(ClickHouseParamUtil.CREATE_TABLE_CLUSTER_TEMPLATE
                    , new String[]{"${database}", "${tableName}", "${fieldSet}", "${engine}", "${clusterName}", "${tableComment}"}
                    , new String[]{databaseConf.getDatabase(), tableConf.getTableName() + "_shard", createTableFieldSql, engineSQL, databaseConf.getClusterName(), tableComment + "_SHARD"});
            sqlList.add(sql);
            String distributeKey = "rand()";
            if (distributedField != null) {
                distributeKey = distributedField.getFieldName();
            }
            String distributeSql = StringUtils.replaceEach(ClickHouseParamUtil.CREATE_TABLE_CLUSTER_DISTRIBUTE_TEMPLATE
                    , new String[]{"${database}", "${tableName}", "${fieldSet}",  "${clusterName}", "${shardTableName}", "${distributeField}", "${tableComment}"}
                    , new String[]{databaseConf.getDatabase(), tableConf.getTableName(), createTableFieldSql, databaseConf.getClusterName(), tableConf.getTableName() + "_shard", distributeKey, tableComment});
            sqlList.add(distributeSql);
        } else if (!tableConf.isOpenDistributed() && tableConf.isOpenReplica()) {
            String sql = StringUtils.replaceEach(ClickHouseParamUtil.CREATE_TABLE_REPLICATED_TEMPLATE
                    , new String[]{"${database}", "${tableName}", "${fieldSet}", "${engine}", "${tableComment}"}
                    , new String[]{databaseConf.getDatabase(), tableConf.getTableName(), createTableFieldSql, tableConf.getEngine().getEngineName(), tableComment + "_REPLICA"});
            sqlList.add(sql);
        } else if (tableConf.isOpenDistributed() && tableConf.isOpenReplica()) {
            String sql = StringUtils.replaceEach(ClickHouseParamUtil.CREATE_TABLE_CLUSTER_REPLICATED_TEMPLATE
                    , new String[]{"${database}", "${tableName}", "${fieldSet}", "${engine}", "${clusterName}", "${tableComment}"}
                    , new String[]{databaseConf.getDatabase(), tableConf.getTableName() + "_shard", createTableFieldSql, tableConf.getEngine().getEngineName(), databaseConf.getClusterName(), tableComment + "_REPLICA"});
            sqlList.add(sql);
            String distributeKey = "rand()";
            if (distributedField != null) {
                distributeKey = distributedField.getFieldName();
            }
            String distributeSql = StringUtils.replaceEach(ClickHouseParamUtil.CREATE_TABLE_CLUSTER_DISTRIBUTE_TEMPLATE
                    , new String[]{"${database}", "${tableName}", "${fieldSet}", "${clusterName}", "${shardTableName}", "${distributeField}", "${tableComment}"}
                    , new String[]{databaseConf.getDatabase(), tableConf.getTableName(), createTableFieldSql, databaseConf.getClusterName(), tableConf.getTableName() + "_shard", distributeKey, tableComment});
            sqlList.add(distributeSql);
        }
        for (String sql : sqlList) {
            if (log.isDebugEnabled()) {
                log.debug("create table sql: {}", sql);
            }
            recordExecuteStatementLog(sql, null);
            connect.update(sql);
        }
        return new QueryMethodResult();
    }

    @Override
    protected QueryMethodResult dropTableMethod(QueryRunner connect, AniHandler queryEntity, ClickHouseDatabaseInfo databaseConf, ClickHouseTableInfo tableConf) throws Exception {
        String dropTableSQL;
        if (tableConf.isOpenDistributed()) {
            dropTableSQL = String.format(DROP_TABLE_SQL_CLUSTER, tableConf.getTableName(), databaseConf.getClusterName());
            String dropShardTable = String.format(DROP_TABLE_SQL_CLUSTER, tableConf.getTableName() + "_shard", databaseConf.getClusterName());
            recordExecuteStatementLog(dropShardTable, null);
            connect.update(dropShardTable);
        } else {
            dropTableSQL = String.format(DROP_TABLE_SQL, tableConf.getTableName());
        }
        recordExecuteStatementLog(dropTableSQL, null);
        int update = connect.update(dropTableSQL);
        return new QueryMethodResult(update, null);
    }

    @Override
    protected QueryMethodResult deleteIndexMethod(QueryRunner connect, AniHandler queryEntity, ClickHouseDatabaseInfo databaseConf, ClickHouseTableInfo tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + ".deleteIndexMethod");
    }

    @Override
    protected QueryMethodResult createIndexMethod(QueryRunner connect, AniHandler queryEntity, ClickHouseDatabaseInfo databaseConf, ClickHouseTableInfo tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + ".createIndexMethod");
    }


    @Override
    protected QueryMethodResult updateMethod(QueryRunner connect, AniHandler queryEntity, ClickHouseDatabaseInfo databaseConf, ClickHouseTableInfo tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + ".updateMethod");
    }

    @Override
    protected QueryMethodResult delMethod(QueryRunner connect, AniHandler queryEntity, ClickHouseDatabaseInfo databaseConf, ClickHouseTableInfo tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + ".delMethod");
    }

    @Override
    protected QueryMethodResult monitorStatusMethod(QueryRunner connect, ClickHouseDatabaseInfo databaseConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "PostGreServiceImpl.monitorStatusMethod");
    }

    @Override
    protected QueryMethodResult querySchemaMethod(QueryRunner connect, ClickHouseDatabaseInfo databaseConf, ClickHouseTableInfo tableConf) throws Exception {
        List<Map<String, Object>> schemaInfoList = new ArrayList<Map<String, Object>>();
        Connection conn = null;
        ResultSet primaryKeys = null;
        ResultSet columnSet = null;
        try {
            conn = connect.getDataSource().getConnection();
            // 获取主键
            primaryKeys = conn.getMetaData().getPrimaryKeys(null, null,tableConf.getTableName());
            List<String> primaryKeyList = new ArrayList<>();
            while (primaryKeys.next()) {
                primaryKeyList.add(primaryKeys.getString("COLUMN_NAME"));
            }
            columnSet = conn.getMetaData().getColumns(null, null, tableConf.getTableName(), "%");

            while (columnSet.next()) {
                Map<String, Object> map = new HashMap<>();
                String columnName = columnSet.getString("COLUMN_NAME");
                map.put("col_name", columnName);
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
                map.put("data_type", columnType);
                String columnSize = columnSet.getString("COLUMN_SIZE");
                if (StringUtils.isEmpty(columnSize)) {
                    map.put("columnSize", "");
                } else {
                    map.put("columnSize", columnSize);
                }
                int nullableInt = columnSet.getInt("NULLABLE");
                map.put("nullable", nullableInt);
                String columnDef = columnSet.getString("COLUMN_DEF");
                columnDef = columnDef == null ? "" : columnDef;
                map.put("columnDef", columnDef);
                schemaInfoList.add(map);
            }
            return new QueryMethodResult(schemaInfoList.size(), schemaInfoList);
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

            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    log.error("postGre conn 对象关闭失败!", e);
                }
            }
        }
    }

    @Override
    protected QueryMethodResult queryDatabaseSchemaMethod(QueryRunner connect, ClickHouseDatabaseInfo databaseConf, ClickHouseTableInfo tableConf) throws Exception {
        ResultSet schemasSet = null;
        List<Map<String, Object>> schemasList = new ArrayList<>();
        try (Connection conn = connect.getDataSource().getConnection()) {
            schemasSet = conn.getMetaData().getSchemas();
            while (schemasSet.next()) {
                Map<String, Object> map = new HashMap<>();
                map.put("schema", schemasSet.getString("TABLE_SCHEM"));
                map.put("catalog", schemasSet.getString("TABLE_CATALOG"));
                schemasList.add(map);
            }
        }
        return new QueryMethodResult(schemasList.size(), schemasList);
    }

    @Override
    protected QueryMethodResult saveOrUpdateMethod(QueryRunner connect, AniHandler queryEntity, ClickHouseDatabaseInfo databaseConf, ClickHouseTableInfo tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + ".saveOrUpdateMethod");
    }

    @Override
    protected QueryMethodResult saveOrUpdateBatchMethod(QueryRunner connect, AniHandler queryEntity, ClickHouseDatabaseInfo databaseConf, ClickHouseTableInfo tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + ".saveOrUpdateBatchMethod");
    }

    @Override
    protected QueryMethodResult alterTableMethod(QueryRunner connect, AniHandler queryEntity, ClickHouseDatabaseInfo databaseConf, ClickHouseTableInfo tableConf) throws Exception {
        String addSql = queryEntity.getAniAlterTable().getAddSql();
        String delSql = queryEntity.getAniAlterTable().getDelSql();
        List<String> fieldCommentList = queryEntity.getAniAlterTable().getFieldCommentList();

        if (log.isDebugEnabled()) {
            log.debug("ClickHouse修改表addSql语句:{}",addSql);
            log.debug("ClickHouse修改表delSql语句:{}",delSql);
        }
        if(StringUtils.isNotBlank(addSql)){
            addSql = StringUtils.replace(addSql, "${tableName}", tableConf.getTableName());
        }
        if(StringUtils.isNotBlank(delSql)){
            delSql = StringUtils.replace(delSql, "${tableName}", tableConf.getTableName());
        }
        if (CollectionUtil.isNotEmpty(fieldCommentList)) {
            Connection connection = null;
            Statement statement = null;
            try {
                connection = connect.getDataSource().getConnection();
                connection.setAutoCommit(false);
                statement = connection.createStatement();
                statement.execute(addSql);
                for (int i = 0; i < fieldCommentList.size(); i++) {
                    String fieldCommentSql = fieldCommentList.get(i);
                    String replaceSql = StringUtils.replace(fieldCommentSql, "${tableName}", tableConf.getTableName());
                    recordExecuteStatementLog(replaceSql, null);
                    statement.execute(replaceSql);
                }
                connection.commit();
            } finally {
                if (statement != null) {
                    try {
                        statement.close();
                    } catch (SQLException e) {
                        log.error("ClickHouse ResultSet 对象关闭失败!", e);
                    }
                }
                if (connection != null) {
                    try {
                        connection.close();
                    } catch (SQLException e) {
                        log.error("ClickHouse ResultSet 对象关闭失败!", e);
                    }
                }
            }
        } else {
            recordExecuteStatementLog(addSql, null);
            connect.update(addSql);
        }

        if(StringUtils.isNotBlank(delSql)){
            recordExecuteStatementLog(delSql, null);
            connect.update(delSql);
        }
        return new QueryMethodResult();
    }

    @Override
    protected QueryMethodResult tableExistsMethod(QueryRunner connect, ClickHouseDatabaseInfo databaseConf, ClickHouseTableInfo tableConf) throws Exception {
        String sql = StringUtils.replaceEach(EXISTS_TABLE_SQL, new String[]{"${database}", "${tableName_low}", "${tableName_up}"}, new String[]{databaseConf.getDatabase(), tableConf.getTableName().toLowerCase(), tableConf.getTableName().toUpperCase()});
        recordExecuteStatementLog(sql, null);
        Map<String, Object> query = connect.query(sql, new MapHandler());
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
    protected QueryMethodResult queryTableInformationMethod(QueryRunner connect, ClickHouseDatabaseInfo databaseConf, ClickHouseTableInfo tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "BasePostGreServiceImpl.queryTableInformationMethod");
    }

    @Override
    protected QueryMethodResult queryListDatabaseMethod(QueryRunner connect, AniHandler queryEntity, ClickHouseDatabaseInfo databaseConf, ClickHouseTableInfo tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + ".queryListDatabaseMethod");
    }

    @Override
    protected QueryMethodResult queryDatabaseInformationMethod(QueryRunner connect, AniHandler queryEntity, ClickHouseDatabaseInfo databaseConf, ClickHouseTableInfo tableConf) throws SQLException {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + ".queryDatabaseInformationMethod");
    }


    @Override
    protected BasicRowProcessor getRowProcessor(ClickHouseDatabaseInfo databaseConf) {
        return new BasicRowProcessor();
    }

    @Override
    protected String formatSql(String sql, ClickHouseDatabaseInfo databaseConf, ClickHouseTableInfo tableConf) {
        sql = String.format(sql, tableConf.getTableName());
        return StringUtils.replace(sql, "${tableName}", tableConf.getTableName());
    }

    @Override
    protected SqlParseHandler getSqlParseHandler(ClickHouseDatabaseInfo database) {
        throw new RuntimeException("还没实现....请期待");
    }

    @Override
    protected SequenceHandler getSequenceHandler(ClickHouseDatabaseInfo database) {
        throw new RuntimeException("ClickHouse暂不支持序列");
    }

}
