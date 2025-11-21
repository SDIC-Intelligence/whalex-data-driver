package com.meiya.whalex.db.module.dwh;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.map.MapUtil;
import cn.hutool.core.util.StrUtil;
import com.meiya.whalex.annotation.Support;
import com.meiya.whalex.db.constant.SupportPower;
import com.meiya.whalex.db.entity.QueryCursorMethodResult;
import com.meiya.whalex.db.entity.QueryMethodResult;
import com.meiya.whalex.db.entity.ani.AniHandler;
import com.meiya.whalex.db.entity.dwh.HiveClient;
import com.meiya.whalex.db.entity.dwh.HiveDatabaseInfo;
import com.meiya.whalex.db.entity.dwh.HiveFieldTypeEnum;
import com.meiya.whalex.db.entity.dwh.HiveTableInfo;
import com.meiya.whalex.db.entity.table.infomaration.HiveTableInformation;
import com.meiya.whalex.db.entity.table.partition.TablePartitions;
import com.meiya.whalex.exception.BusinessException;
import com.meiya.whalex.exception.ExceptionCode;
import com.meiya.whalex.interior.db.constant.PartitionType;
import com.meiya.whalex.sql.ani.RdbDataResult;
import com.meiya.whalex.sql.ani.RdbmsCursorCache;
import com.meiya.whalex.sql.module.AbstractRdbmsModuleBaseService;
import com.meiya.whalex.sql.module.SequenceHandler;
import com.meiya.whalex.sql.module.SqlParseHandler;
import com.meiya.whalex.util.AggResultTranslateUtil;
import com.meiya.whalex.util.JsonUtil;
import com.meiya.whalex.util.WildcardRegularConversion;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.dbutils.BasicRowProcessor;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.lang3.StringUtils;

import java.sql.*;
import java.util.*;
import java.util.Date;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static com.meiya.whalex.db.util.param.impl.dwh.BaseHiveParamUtil.PARTITION_INFO;

/**
 * Hive 服务类
 *
 * @author 黄河森
 * @date 2019/12/26
 * @project whale-cloud-platformX
 */
@Slf4j
@Support(value = {
        SupportPower.TEST_CONNECTION,
        SupportPower.SEARCH,
        SupportPower.SHOW_SCHEMA,
        SupportPower.CREATE_TABLE,
        SupportPower.DROP_TABLE,
        SupportPower.SHOW_TABLE_LIST,
        SupportPower.SHOW_DATABASE_LIST
})
public class BaseHiveServiceImpl<S extends HiveClient,
        Q extends AniHandler,
        D extends HiveDatabaseInfo,
        T extends HiveTableInfo,
        C extends RdbmsCursorCache> extends AbstractRdbmsModuleBaseService<S, Q, D, T, C> {

    /**
     * 查询库中表信息SQL
     */
    private static final String SHOW_TABLE_SQL = "show tables";

    /**
     * 删除表SQL
     */
    private static final String DROP_TABLE_SQL = "drop table %s";

    /**
     * 删除分区SQL
     */
    private static final String DROP_PARTITION_SQL = "alter table %s drop if exists partition(%s='%s')";

    /**
     * 删除指定范围分区SQL
     */
    private static final String DROP_RANGE_PARTITION_SQL = "alter table %s drop if exists partition(%s>='%s',%s<='%s')";

    /**
     * 查询表具体分区信息
     */
    private static final String QUERY_PARTITION_DETAIL_SQL = "DESCRIBE FORMATTED %s PARTITION (%s)";

    /**
     * 查询表详细信息
     */
    private static final String QUERY_SCHEMA_DETAIL_SQL = "desc formatted %s";

    /**
     * 查询数据库列表
     */
    private static final String SHOW_DATABASE_SQL = "show databases";


    /**
     * 查询数据库信息
     */
    private static final String SHOW_DATABASE_DETAIL_SQL = "describe database %s";

    private static final String CREATE_DATABASE = "CREATE DATABASE `%s`";

    private static final String DROP_DATABASE = "DROP DATABASE `%s`";

    @Override
    protected QueryMethodResult queryMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        AniHandler.AniQuery aniQuery = queryEntity.getAniQuery();
        String sql = aniQuery.getSql();
        String sqlCount = aniQuery.getSqlCount();
        sql = sql.replace("${tableName}", tableConf.getTableName());
        if(log.isDebugEnabled()) {
            log.debug("hive sql查询语句 : " + sql);
        }
//        sql = String.format(sql, tableConf.getTableName());
        Long total = 0L;
        queryEntity.setQueryStr(sql);
        List<Map<String, Object>> rows = new ArrayList<>();
        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;
        ResultSet resultSetTotal = null;
        try {
            connection = connect.getDataSource().getConnection();
            statement = connection.createStatement();
            // 设置环境
            setEnv(databaseConf, tableConf, statement);
            if (aniQuery.getCount() && !aniQuery.isAgg()) {
                sqlCount = sqlCount.replace("${tableName}", tableConf.getTableName());
                if(log.isDebugEnabled()) {
                    log.debug("hive sql查询语句 : " + sqlCount);
                }
//                sqlCount = String.format(sqlCount, tableConf.getTableName());
                recordExecuteStatementLog(sqlCount, null);
                resultSetTotal = statement.executeQuery(sqlCount);
                if (resultSetTotal.next()) {
                    total = resultSetTotal.getLong(1);
                }
                if (total == 0) {
                    return new QueryMethodResult(total, rows);
                }
            }
            recordExecuteStatementLog(sql, null);
            resultSet = statement.executeQuery(sql);
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();
            while (resultSet.next()) {
                Map<String, Object> map = new HashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    String columnLabel = metaData.getColumnLabel(i);
                    if (StringUtils.startsWithIgnoreCase(columnLabel, tableConf.getTableName())) {
                        columnLabel = StringUtils.substring(columnLabel, columnLabel.indexOf(".") + 1, columnLabel.length());
                    }
                    map.put(columnLabel, resultSet.getString(i));
                }
                rows.add(map);
            }
        } finally {
            /*if (connection != null) {
                try {
                    connection.commit();
                } catch (SQLException e) {
                    log.error("Hive 查询事务提交失败!", e);
                }
            }*/
            close(connection, statement, resultSet, resultSetTotal);
        }
        if(aniQuery.isAgg()){
            return new QueryMethodResult(total, AggResultTranslateUtil.getRelationDataBaseTreeAggResult(rows, aniQuery.getAggNames()));
        }
        return new QueryMethodResult(total, rows);
    }

    @Override
    public <T> T queryExecute(D databaseConf, S connect, String sql, ResultSetHandler<T> rsh, Object[] paramArray) throws SQLException {

        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        try {
            connection = connect.getDataSource().getConnection();
            statement = connection.prepareStatement(sql);
            if(paramArray != null) {
                for (int i = 0; i < paramArray.length; i++) {
                    statement.setObject(i + 1, paramArray[i]);
                }
            }
            recordExecuteStatementLog(sql, paramArray);
            resultSet = statement.executeQuery(sql);
            return rsh.handle(resultSet);
        } finally {
            close(connection, statement, resultSet, null);
        }
    }

    /**
     * 设置环境
     *
     * @param databaseConf
     * @param tableConf
     * @param statement
     * @throws SQLException
     */
    private void setEnv(D databaseConf, T tableConf, Statement statement) throws SQLException {
        // 设置资源队列
        if (StringUtils.isNotBlank(databaseConf.getQueueName())) {
            if (HiveDatabaseInfo.TEZ_JOB.equalsIgnoreCase(databaseConf.getJobEngine())) {
                statement.execute("set tez.queue.name=" + databaseConf.getQueueName());
            } else {
                statement.execute("set mapreduce.job.queuename=" + databaseConf.getQueueName());
            }
        }
        // 设置资源
        if (StringUtils.isNotBlank(databaseConf.getJobEngine()) && tableConf.getRandomMemory() != null) {
            if (HiveDatabaseInfo.TEZ_JOB.equalsIgnoreCase(databaseConf.getJobEngine())) {
                statement.execute(String.format(HiveDatabaseInfo.TEZ_CONTAINER_SIZE, tableConf.getRandomMemory()));
            } else if (HiveDatabaseInfo.MR_JOB.equalsIgnoreCase(databaseConf.getJobEngine())) {
                statement.execute(String.format(HiveDatabaseInfo.YARN_MAP_MEMORY, tableConf.getRandomMemory()));
                statement.execute(String.format(HiveDatabaseInfo.YARN_REDUCE_MEMORY, tableConf.getRandomMemory()));
            }
        }
    }

    @Override
    protected QueryMethodResult countMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        // 判断表是否存在
        try {
            boolean exists = tableExists(connect, databaseConf, tableConf);
            if (!exists) {
                throw new BusinessException(ExceptionCode.NOT_FOUND_TABLE_EXCEPTION, tableConf.getTableName());
            }
        } catch (Exception e) {
            if (e instanceof BusinessException
                    &&
                    ((BusinessException) e).getCode() == ExceptionCode.NOT_FOUND_TABLE_EXCEPTION.getCode()) {
                throw e;
            }
            log.error("Hive 统计接口判断目标表是否存在失败 tableName: [{}]!", tableConf.getTableName(), e);
        }

        AniHandler.AniQuery aniQuery = queryEntity.getAniQuery();
        String sqlCount = aniQuery.getSqlCount();
        sqlCount = sqlCount.replace("${tableName}", tableConf.getTableName());
        Long total = 0L;
        queryEntity.setQueryStr(sqlCount);
        Connection connection = null;
        Statement statement = null;
        ResultSet resultSetTotal = null;
        try {
            connection = connect.getDataSource().getConnection();
            statement = connection.createStatement();
            // 设置环境
            setEnv(databaseConf, tableConf, statement);
            recordExecuteStatementLog(sqlCount, null);
            resultSetTotal = statement.executeQuery(sqlCount);
            if (resultSetTotal.next()) {
                total = resultSetTotal.getLong(1);
            }
        } finally {
            close(connection, statement, null, resultSetTotal);
        }
        return new QueryMethodResult(total, null);
    }

    @Override
    protected QueryMethodResult testConnectMethod(S connect, D databaseConf) throws Exception {
        Connection connection = null;
        try {
            connection = connect.getDataSource().getConnection();
        } finally {
            close(connection, null, null, null);
        }
        return new QueryMethodResult();
    }

    @Override
    protected QueryMethodResult showTablesMethod(S connect, Q queryEntity, D databaseConf) throws Exception {
        if(connect.metaStoreIsAvailable()) {
            List<Map<String, Object>> rows = connect.listTable(databaseConf.getDatabaseName());
            rows = WildcardRegularConversion.matchFilter(queryEntity.getListTable().getTableMatch(), rows);
            return new QueryMethodResult(rows.size(), rows);
        }
        String sql = SHOW_TABLE_SQL;
        AniHandler.AniListTable listTable = queryEntity.getListTable();
        if(StringUtils.isNotBlank(listTable.getDatabaseName())) {
            sql += " in " + listTable.getDatabaseName();
        }
        QueryMethodResult queryMethodResult = getQueryMethodResult(connect, sql);
        if (StringUtils.isNotBlank(listTable.getTableMatch())) {
            if (queryMethodResult.isSuccess()) {
                List<Map<String, Object>> rows = queryMethodResult.getRows();
                rows = WildcardRegularConversion.matchFilter(queryEntity.getListTable().getTableMatch(), rows);
                queryMethodResult.setRows(rows);
                queryMethodResult.setTotal(rows.size());
            }
        }
        return queryMethodResult;
    }

    @Override
    protected QueryMethodResult getIndexesMethod(S connect, D databaseConf, T tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "HiveService.getIndexesMethod");
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
        // 若为修复分区表操作
        if (aniCreateTable.isRepair()) {
            Connection connection = null;
            Statement statement = null;
            try {
                connection = connect.getDataSource().getConnection();
                statement = connection.createStatement();
                recordExecuteStatementLog("msck repair table " + tableConf.getTableName(), null);
                statement.execute("msck repair table " + tableConf.getTableName());
            } finally {
                close(connection, statement, null, null);
            }
            return new QueryMethodResult();
        }
        // 建表操作
        String sql = aniCreateTable.getSql();

        boolean partition = aniCreateTable.isPartition();

        if (log.isDebugEnabled()) {
            log.debug("hive create table sql: [{}]", sql);
        }
        Connection connection = null;
        Statement statement = null;
        try {
            connection = connect.getDataSource().getConnection();
            statement = connection.createStatement();
            recordExecuteStatementLog(sql, null);
            statement.execute(sql);
            // 外部表执行修复分区操作
            if (partition && (!StrUtil.isAllBlank(tableConf.getFileType(), tableConf.getPath()))) {
                recordExecuteStatementLog("msck repair table " + tableConf.getTableName(), null);
                try {
                    statement.execute("msck repair table " + tableConf.getTableName());
                } catch (SQLException e) {
                    log.error("repair table: {} fail! path: {}.", tableConf.getTableName(), tableConf.getPath(), e);
                }
            }
        } finally {
            close(connection, statement, null, null);
        }
        return new QueryMethodResult();
    }

    @Override
    protected QueryMethodResult dropTableMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        AniHandler.AniDropTable aniDropTable = queryEntity.getAniDropTable();
        Date startTime = aniDropTable.getStartTime();
        Date stopTime = aniDropTable.getStopTime();
        // 判断是否分区表，删除分区操作
        String sql;
        if (StringUtils.isNotBlank(tableConf.getFormat()) && startTime != null && stopTime != null) {
            String format = tableConf.getFormat();
            // 解析字段名
            String fieldName = StringUtils.substring(format, 1, StringUtils.indexOf(format, "="));
            // 解析格式
            String formatType = StringUtils.substring(format, StringUtils.indexOf(format, "#") + 1, StringUtils.indexOf(format, "}"));
            // 转换开始时间
            String startTimeFormat = DateUtil.format(startTime, formatType);
            // 转换结束时间
            String stopTimeFormat = DateUtil.format(stopTime, formatType);
            // 拼接SQL
            if (startTimeFormat.equalsIgnoreCase(stopTimeFormat)) {
                sql = String.format(DROP_PARTITION_SQL, tableConf.getTableName(), fieldName, stopTimeFormat);
            } else {
                sql = String.format(DROP_RANGE_PARTITION_SQL, tableConf.getTableName(), fieldName, startTimeFormat, fieldName, stopTimeFormat);
            }
        } else {
            // 全表删除
            sql = String.format(DROP_TABLE_SQL, tableConf.getTableName());
        }
        Connection connection = null;
        Statement statement = null;
        try {
            connection = connect.getDataSource().getConnection();
            statement = connection.createStatement();
            recordExecuteStatementLog(sql, null);
            statement.execute(sql);
        } finally {
            close(connection, statement, null, null);
        }
        return new QueryMethodResult();
    }

    @Override
    protected QueryMethodResult deleteIndexMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "HiveService.deleteIndexMethod");
    }

    @Override
    protected QueryMethodResult createIndexMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "HiveService.createIndexMethod");
    }

    @Override
    protected QueryMethodResult updateMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "HiveService.updateMethod");
    }

    @Override
    protected QueryMethodResult delMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "HiveService.delMethod");
    }

    @Override
    protected QueryMethodResult monitorStatusMethod(S connect, D databaseConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "HiveService.monitorStatusMethod");
    }

    @Override
    protected QueryMethodResult querySchemaMethod(S connect, D databaseConf, T tableConf) throws Exception {

        String databaseName = databaseConf.getDatabaseName();
        String tableName = tableConf.getTableName();
        if(tableName.contains(".")) {
            String[] split = tableName.split("\\.");
            tableName = split[1];
            databaseName = split[0];
        }

        if(connect.metaStoreIsAvailable()) {
            List<Map<String, Object>> schemaList = connect.getTable(databaseName, tableName);
            return new QueryMethodResult(schemaList.size(), schemaList);
        }

        List<Map<String, Object>> schemaInfoList = new ArrayList<>();
        Connection conn = null;
        ResultSet primaryKeys = null;
        ResultSet columnSet = null;

        try {
            conn = connect.getDataSource().getConnection();
            // 获取主键
            primaryKeys = conn.getMetaData().getPrimaryKeys(databaseName, databaseName, tableName);
            List<String> primaryKeyList = new ArrayList<>();
            while (primaryKeys.next()) {
                primaryKeyList.add(primaryKeys.getString("COLUMN_NAME"));
            }
            columnSet = conn.getMetaData().getColumns(databaseName, null, tableName, "%");

            while (columnSet.next()) {
                Map<String, Object> map = new HashMap<>();
                String columnName = columnSet.getString("COLUMN_NAME");
                map.put("col_name", columnName);
                String isAutoincrement = columnSet.getString("IS_AUTO_INCREMENT");
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
                map.put("std_data_type", HiveFieldTypeEnum.dbFieldType2FieldType(columnType, StringUtils.isNotBlank(columnSize) ? Integer.valueOf(columnSize) : null).getVal());
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

            if (conn != null) {
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
    protected QueryCursorMethodResult queryCursorMethod(S connect, Q queryEntity, D databaseConf, T tableConf, C cursorCache, Consumer<Map<String, Object>> consumer, Boolean rollAllData) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "HiveServiceImpl.queryCursorMethod");
    }

    @Override
    protected BasicRowProcessor getRowProcessor(D databaseConf) {
        return new BasicRowProcessor();
    }

    @Override
    protected String formatSql(String sql, D databaseConf, T tableConf) {
        sql = String.format(sql, tableConf.getTableName());
        sql = StringUtils.replace(sql, "${tableName}", tableConf.getTableName());
        return StringUtils.replace(sql, "${localTable}", tableConf.getTableName());
    }

    @Override
    protected SqlParseHandler getSqlParseHandler(D database) {
        return new HiveSqlParseHandler();
    }

    @Override
    protected SequenceHandler getSequenceHandler(D database) {
        throw new RuntimeException("hive 暂不支持序列");
    }

    @Override
    protected QueryMethodResult saveOrUpdateMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "HiveServiceImpl.saveOrUpdateMethod");
    }

    @Override
    protected QueryMethodResult saveOrUpdateBatchMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "HiveServiceImpl.saveOrUpdateBatchMethod");
    }

    @Override
    protected QueryMethodResult alterTableMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        AniHandler.AniAlterTable aniAlterTable = queryEntity.getAniAlterTable();
        List<String> sqlList = aniAlterTable.getSqlList();
        if(CollectionUtils.isNotEmpty(sqlList)) {
            Connection connection = null;
            Statement statement = null;
            try {
                connection = connect.getDataSource().getConnection();
                statement = connection.createStatement();
                for (String sql : sqlList) {
                    if (log.isDebugEnabled()) {
                        log.debug("hive 执行 sql: [{}]", sql);
                    }
                    recordExecuteStatementLog(sql, null);
                    statement.execute(sql);
                }
            } finally {
                close(connection, statement, null, null);
            }
        }
        return new QueryMethodResult();
    }

    @Override
    protected QueryMethodResult tableExistsMethod(S connect, D databaseConf, T tableConf) throws Exception {
        if(connect.metaStoreIsAvailable()) {
            List<Map<String, Object>> rows = connect.listTable(databaseConf.getDatabaseName());
            Map<String, Object> existsMap = new HashMap<>(1);
            for (Map<String, Object> row : rows) {
                String tableName = (String) row.get("tableName");
                if(tableName.equalsIgnoreCase(tableConf.getTableName())) {
                    existsMap.put(tableConf.getTableName(), true);
                    break;
                }
            }
            if(existsMap.isEmpty()) {
                existsMap.put(tableConf.getTableName(), false);
            }
            return new QueryMethodResult(1, CollectionUtil.newArrayList(existsMap));
        }
        String sql = String.format("show tables like '%s'", tableConf.getTableName());
        QueryMethodResult queryMethodResult = getQueryMethodResult(connect, sql);
        Map<String, Object> existsMap = new HashMap<>(1);
        if (CollectionUtil.isNotEmpty(queryMethodResult.getRows())) {
            existsMap.put(tableConf.getTableName(), true);
        } else {
            existsMap.put(tableConf.getTableName(), false);
        }
        return new QueryMethodResult(1, CollectionUtil.newArrayList(existsMap));
    }

    @Override
    protected QueryMethodResult queryTableInformationMethod(S connect, D databaseConf, T tableConf) throws Exception {
        String sql = String.format(QUERY_SCHEMA_DETAIL_SQL, tableConf.getTableName());
        QueryMethodResult queryMethodResult = getQueryMethodResult(connect, sql);
        List<Map<String, Object>> rows = queryMethodResult.getRows();
        Map<String, Object> result = resultBuild(rows);

        List<Map<String, Object>> partitionInformation = (List<Map<String, Object>>) result.get("partitionInformation");

        String partitionKey = "";

        if(CollectionUtils.isNotEmpty(partitionInformation)) {
            for (Map<String, Object> stringObjectMap : partitionInformation) {
                if(StringUtils.isNotBlank(partitionKey)) {
                    partitionKey += ",";
                }
                partitionKey += stringObjectMap.get("col_name");
            }
        }

        Map<String, Object> storageInformation = (Map<String, Object>) result.get("storageInformation");
        Map<String, Object> tableInformation = (Map<String, Object>) result.get("tableInformation");
        Map<String, Object> extendMeta = new HashMap<>();
        extendMeta.putAll(storageInformation);
        extendMeta.putAll(tableInformation);

        HiveTableInformation hiveTableInformation = new HiveTableInformation(tableConf.getTableName(), null, null, null, partitionKey, extendMeta);


        return new QueryMethodResult(1, CollectionUtil.newArrayList(JsonUtil.entityToMap(hiveTableInformation)));

    }

    @Override
    protected QueryMethodResult queryPartitionInformationMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        List<Map<String, Object>> result = new ArrayList<>();
        String sql = "SHOW partitions " + tableConf.getTableName();
        QueryMethodResult queryMethodResult = getQueryMethodResult(connect, sql);
        List<Map<String, Object>> rows = queryMethodResult.getRows();
        if (CollectionUtil.isNotEmpty(rows)) {
            for (Map<String, Object> row : rows) {
                if (MapUtil.isNotEmpty(row)) {
                    List<TablePartitions.TablePartition> tablePartitionList = new ArrayList<>();
                    Map<String, Object> extendsMap = new HashMap<>();
                    Object partitionObj = row.get("partition");
                    if (partitionObj != null) {
                        String partition = String.valueOf(partitionObj);
                        String[] split = StringUtils.split(partition, "/");
                        for (String partitionStr : split) {
                            String[] partitionSpl = StringUtils.split(partitionStr, "=");
                            String partitionKey = partitionSpl[0];
                            String partitionValue = partitionSpl[1];
                            TablePartitions.TablePartition tablePartition = TablePartitions.TablePartition.builder().partitionKey(partitionKey).partitionValue(partitionValue).build();
                            tablePartitionList.add(tablePartition);
                        }
                        // 查询分区存储路径
                        String location = queryPartitionLocation(connect, tableConf, tablePartitionList);
                        extendsMap.put("location", location);
                    }
                    TablePartitions tablePartitions = TablePartitions.builder().partitions(tablePartitionList).extendInfos(extendsMap).partitionType(PartitionType.EQ.name()).build();
                    result.add(JsonUtil.entityToMap(tablePartitions));
                }
            }
        }
        return new QueryMethodResult(result.size(), result);
    }

    /**
     * 查询分区存储路径
     *
     * @param connect
     * @param tableConf
     * @param tablePartitionList
     * @throws SQLException
     */
    private String queryPartitionLocation(S connect, T tableConf, List<TablePartitions.TablePartition> tablePartitionList) throws SQLException {
        StringBuilder sb = new StringBuilder();
        for (TablePartitions.TablePartition tablePartition : tablePartitionList) {
            String partitionField = tablePartition.getPartitionKey();
            Object partitionValue = tablePartition.getPartitionValue();
            if (sb.length() > 0) {
                sb.append(",");
            }
            sb.append(String.format(PARTITION_INFO, partitionField, partitionValue));
        }
        String queryPartitionLocationSql = String.format(QUERY_PARTITION_DETAIL_SQL, tableConf.getTableName(), sb.toString());
        QueryMethodResult queryPartitionLocationResult = getQueryMethodResult(connect, queryPartitionLocationSql);
        List<Map<String, Object>> partitionLocationResultRows = queryPartitionLocationResult.getRows();
        Object location = partitionLocationResultRows.stream().filter(partitionLocationResultRow -> {
            Object o = partitionLocationResultRow.get("col_name");
            if (o != null) {
                return StringUtils.equalsIgnoreCase("Location:           ", o.toString());
            }
            return false;
        }).flatMap(map -> Stream.of(map.get("data_type"))).findFirst().orElse(null);
        if (location != null) {
            return (String) location;
        }
        return null;
    }

    private Map<String, Object> resultBuild(List<Map<String, Object>> rows) {
        Map<String, Object> result = new HashMap<>();
        Iterator<Map<String, Object>> iterator = rows.iterator();
        //buildSchema
        List<Map<String, Object>> schema = buildSchema(iterator);
        List<Map<String, Object>> partitionInformation = new ArrayList<>();
        Map<String, Object> next = iterator.next();
        Object col_name = next.get("col_name");
        if("# Partition Information".equals(col_name)) {
            //Partition Information
            partitionInformation = buildSchema(iterator);
        }
        //Detailed Table Information
        Map<String, Object> tableInformation = buildTableInformation(iterator);
        //Storage Information
        Map<String, Object> storageInformation = buildStorageInformation(iterator);

        result.put("schema", schema);
        result.put("partitionInformation", partitionInformation);
        result.put("tableInformation", tableInformation);
        result.put("storageInformation", storageInformation);
        return result;
    }

    private Map<String, Object> buildStorageInformation(Iterator<Map<String, Object>> iterator) {
        Map<String, Object>  storageInformation = new HashMap<>();
        while (iterator.hasNext()) {
            Map<String, Object> next = iterator.next();
            Object col_name = next.get("col_name");
            if("# Storage Information".equals(col_name)) {
                continue;
            }
            col_name =  col_name == null ?  "" : col_name;
            if(StringUtils.isNotBlank(col_name.toString())) {

                String colName =  getColName(col_name.toString());
                String dataType = getValue(next.get("data_type"));

                if("StorageDescParams".equals(colName)) {
                    while (iterator.hasNext()) {
                        Map<String, Object> objectMap = iterator.next();
                        String comment = getValue(objectMap.get("comment"));
                        dataType = getValue(objectMap.get("data_type"));
                        if(comment == null && dataType == null) {
                            break;
                        }
                        storageInformation.put(dataType, comment);
                    }
                    return storageInformation;
                }else {
                    storageInformation.put(colName, dataType);
                }
            } else {
                return storageInformation;
            }
        }
        return storageInformation;
    }

    private String getValue(Object value){
        if(value == null) {
            return null;
        }
        String trim = value.toString().trim();
        return trim;
    }

    private Map<String, Object> buildTableInformation(Iterator<Map<String, Object>> iterator) {
        Map<String, Object>  tableInformation = new HashMap<>();
        while (iterator.hasNext()) {
            Map<String, Object> next = iterator.next();
            Object col_name = next.get("col_name");
            if("# Detailed Table Information".equals(col_name)) {
                continue;
            }

            if(StringUtils.isNotBlank(col_name.toString())) {

                String colName =  getColName(col_name.toString());
                String dataType = getValue(next.get("data_type"));

                if("TableParameters".equals(colName)) {
                    while (iterator.hasNext()) {
                        Map<String, Object> objectMap = iterator.next();
                        String comment = getValue(objectMap.get("comment"));
                        dataType = getValue(objectMap.get("data_type"));
                        if(comment == null && dataType == null) {
                             break;
                        }
                        tableInformation.put(dataType, comment);
                    }
                    return tableInformation;
                }else {
                    tableInformation.put(colName, dataType);
                }
            } else {
                return tableInformation;
            }
        }
        return tableInformation;
    }

    private List<Map<String, Object>> buildSchema(Iterator<Map<String, Object>> iterator) {
        List<Map<String, Object>>  schema = new ArrayList<>();
        while (iterator.hasNext()) {
            Map<String, Object> next = iterator.next();
            Object col_name = next.get("col_name");
            if("# col_name".equals(col_name)) {
                continue;
            }
            col_name =  col_name == null ?  "" : col_name;
            if(StringUtils.isNotBlank(col_name.toString())) {
                next.put("col_name", getColName(col_name.toString()));
                schema.add(next);
            }else {
                break;
            }
        }
        return schema;
    }

    private String getColName(String colName) {
        String trim = colName.replace(":", "").trim();
        String[] s = trim.split(" ");
        trim  = StringUtils.joinWith("", s);
        return trim;
    }

    @Override
    protected QueryMethodResult queryListDatabaseMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        if(connect.metaStoreIsAvailable()) {
            List<Map<String, Object>> rows = connect.listDatabase();
            rows = WildcardRegularConversion.matchFilterDatabaseName(queryEntity.getListDatabase().getDatabaseMatch(), rows);
            return new QueryMethodResult(rows.size(), rows);
        }
        QueryMethodResult queryMethodResult = getQueryMethodResult(connect, SHOW_DATABASE_SQL);
        if (StringUtils.isNotBlank(queryEntity.getListDatabase().getDatabaseMatch())) {
            if (queryMethodResult.isSuccess()) {
                List<Map<String, Object>> rows = queryMethodResult.getRows();
                rows = WildcardRegularConversion.matchFilterDatabaseName(queryEntity.getListDatabase().getDatabaseMatch(), rows);
                queryMethodResult.setRows(rows);
                queryMethodResult.setTotal(rows.size());
            }
        }
        return queryMethodResult;
    }

    @Override
    protected QueryMethodResult queryDatabaseInformationMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws SQLException {
        String sql = String.format(SHOW_DATABASE_DETAIL_SQL, databaseConf.getDatabaseName());
        return getQueryMethodResult(connect, sql);
    }

    /**
     * 获取查询结果
     *
     * @param connect
     * @param sql
     * @return
     * @throws SQLException
     */
    private QueryMethodResult getQueryMethodResult(QueryRunner connect, String sql) throws SQLException {
        List<Map<String, Object>> rows = new ArrayList<>();
        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;
        try {
            connection = connect.getDataSource().getConnection();
            statement = connection.createStatement();
            recordExecuteStatementLog(sql, null);
            resultSet = statement.executeQuery(sql);
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();
            while (resultSet.next()) {
                Map<String, Object> map = new HashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    String columnLabel = metaData.getColumnLabel(i);
                    if (StringUtils.equalsIgnoreCase(columnLabel, "tab_name")
                            || StringUtils.equalsIgnoreCase(columnLabel, "table_name")
                            || StringUtils.equalsIgnoreCase(columnLabel, "tablename")
                            || StringUtils.equalsIgnoreCase(columnLabel, "tabname")) {
                        columnLabel = "tableName";
                    } else if(StringUtils.equalsIgnoreCase(columnLabel, "database_name")
                            || StringUtils.equalsIgnoreCase(columnLabel, "databasename")) {
                        columnLabel = "Database";
                    }
                    map.put(columnLabel, resultSet.getString(i));
                }
                rows.add(map);
            }
        } finally {
            close(connection, statement, resultSet, null);
        }
        return new QueryMethodResult(rows.size(), rows);
    }

    /**
     * 关闭相关连接
     *
     * @param connection
     * @param statement
     * @param resultSet
     * @param resultSetTotal
     */
    private void close(Connection connection, Statement statement, ResultSet resultSet, ResultSet resultSetTotal) {
        if (resultSetTotal != null) {
            try {
                resultSetTotal.close();
            } catch (SQLException e) {
                log.error("关闭失败!", e);
            }
        }

        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException e) {
                log.error("关闭失败!", e);
            }
        }

        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException e) {
                log.error("关闭失败!", e);
            }
        }

        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                log.error("关闭失败!", e);
            }
        }
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
    public RdbDataResult updateCallableExecute(D databaseConf, Connection connection, String sql, Object[] paramArray, boolean isClose) throws Exception {
        recordExecuteStatementLog(sql, paramArray);

        try {
            try (PreparedStatement statement = connection.prepareStatement(sql)){
                statementSetObject(paramArray, statement);
                int update = statement.executeUpdate();

                RdbDataResult rdbDataResult = new RdbDataResult();
                rdbDataResult.setTotal(update);
                return rdbDataResult;
            }
        } finally {
            if (isClose) {
                if (connection != null) {
                    connection.close();
                }
            }
        }
    }
}
