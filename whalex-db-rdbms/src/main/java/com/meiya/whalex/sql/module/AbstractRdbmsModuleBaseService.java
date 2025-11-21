package com.meiya.whalex.sql.module;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.util.ArrayUtil;
import com.github.benmanes.caffeine.cache.Cache;
import com.meiya.whalex.annotation.Support;
import com.meiya.whalex.business.entity.DatabaseConf;
import com.meiya.whalex.cache.DatCaffeine;
import com.meiya.whalex.db.constant.IsolationLevel;
import com.meiya.whalex.db.constant.SupportPower;
import com.meiya.whalex.db.entity.AbstractDatabaseInfo;
import com.meiya.whalex.db.entity.AbstractDbTableInfo;
import com.meiya.whalex.db.entity.DatabaseSetting;
import com.meiya.whalex.db.entity.PageResult;
import com.meiya.whalex.db.entity.QueryCursorMethodResult;
import com.meiya.whalex.db.entity.QueryMethodResult;
import com.meiya.whalex.db.entity.ani.AniHandler;
import com.meiya.whalex.db.module.DbTransactionModuleService;
import com.meiya.whalex.exception.BusinessException;
import com.meiya.whalex.exception.ExceptionCode;
import com.meiya.whalex.interior.db.constant.ReturnCodeEnum;
import com.meiya.whalex.interior.db.search.in.Page;
import com.meiya.whalex.db.entity.CreateSequenceBean;
import com.meiya.whalex.db.entity.DropSequenceBean;
import com.meiya.whalex.sql.ani.RdbmsCursorCache;
import com.meiya.whalex.util.AggResultTranslateUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.dbutils.BasicRowProcessor;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.dbutils.handlers.MapHandler;
import org.apache.commons.dbutils.handlers.MapListHandler;
import org.apache.commons.lang3.StringUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * 数据库组件复杂sql查询
 *
 * @author 蔡荣桂
 * @date 2011/06/28
 * @project whale-cloud-platformX
 */

@Support(value = {SupportPower.CREATE, SupportPower.DELETE, SupportPower.UPDATE, SupportPower.SEARCH, SupportPower.ASSOCIATED_QUERY, SupportPower.TRANSACTION, SupportPower.TEST_CONNECTION})
@Slf4j
public abstract class AbstractRdbmsModuleBaseService<S extends QueryRunner,
        Q extends AniHandler,
        D extends AbstractDatabaseInfo,
        T extends AbstractDbTableInfo,
        C extends RdbmsCursorCache> extends RdbmsRawStatementModuleImpl<S, Q, D, T, C> implements RdbmsModuleService {

    /**
     * sql 预编译缓存
     */
    private Cache<String, PrecompileSqlStatement> sqlCache = DatCaffeine.newBuilder().build();

    /**
     * SQL查询方法
     *
     * @param dbConnect
     * @param sql
     * @param params
     * @return
     * @throws Exception
     */
    public QueryMethodResult queryBySqlMethod(D databaseConf, S dbConnect, String sql, List<Object> params) throws Exception {
        List<Map<String, Object>> rows = queryExecute(databaseConf, dbConnect, sql, new MapListHandler(getRowProcessor(databaseConf)), params.toArray());
        return new QueryMethodResult(rows.size(), rows);
    }

    /**
     * SQL更新方法
     *
     * @param dbConnect
     * @param sql
     * @param params
     * @return
     * @throws Exception
     */
    public QueryMethodResult updateBySqlMethod(D databaseConf, S dbConnect, String sql, List<Object> params) throws Exception {
        int update = updateExecute(databaseConf, dbConnect, sql, params.toArray());
        return new QueryMethodResult(update, null);
    }

    private PageResult execute(DatabaseSetting databaseSetting, String sql, List<Object> params, ExecuteSqlMethod<D, S> executeSqlMethod) throws Exception {
        return execute(databaseSetting, sql, params, this::sqlParse, executeSqlMethod);
    }

    private PageResult execute(DatabaseSetting databaseSetting, String sql, List<Object> params, CustomSqlParser<D> customSqlParse, ExecuteSqlMethod<D, S> executeSqlMethod) throws Exception {

        QueryMethodResult queryResult = _execute(databaseSetting, sql, params, customSqlParse, executeSqlMethod);

        if (queryResult.isSuccess()) {
            // 无查询结果返回
            if (CollectionUtils.isEmpty(queryResult.getRows())) {
                return new PageResult(queryResult.getTotal(), queryResult.getRows(), true, ReturnCodeEnum.CODE_SUCCESS_QUERY_RESULT_NULL);
            } else {
                return new PageResult(queryResult.getTotal(), queryResult.getRows(), true, ReturnCodeEnum.CODE_SUCCESS);
            }
        } else {
            PageResult pageResult = new PageResult(queryResult.getTotal(), queryResult.getRows(), false, ReturnCodeEnum.CODE_BUSINESS_FAIL);
            return pageResult;
        }
    }

    @Override
    public PageResult queryBySql(DatabaseSetting databaseSetting, String sql, List<Object> params) throws Exception {
       return execute(
               databaseSetting,
               sql,
               params,
               new ExecuteSqlMethod<D, S>() {
                   @Override
                   public <T> T execute(D databaseConf, S dbConnect, String sql, List<Object> params) throws Exception {
                       return (T) queryBySqlMethod(databaseConf, dbConnect, sql, params);
                   }
               }
       );
    }


    @Override
    public PageResult updateBySql(DatabaseSetting databaseSetting, String sql, List<Object> params) throws Exception {
        return execute(
                databaseSetting,
                sql,
                params,
                new ExecuteSqlMethod<D, S>() {
                    @Override
                    public <T> T execute(D databaseConf, S dbConnect, String sql, List<Object> params) throws Exception {
                        return (T) updateBySqlMethod(databaseConf, dbConnect, sql, params);
                    }
                }
        );
    }

    @Override
    public PageResult createSequence(DatabaseSetting databaseSetting, CreateSequenceBean createSequenceBean) throws Exception {
        SequenceHandler sequenceHandler = getSequenceHandler(getDataConf(databaseSetting));
        String sql = sequenceHandler.parseCreateSequenceBean(createSequenceBean);
        return execute(
                databaseSetting,
                sql,
                new ArrayList<>(0),
                this::sqlRawParse,
                new ExecuteSqlMethod<D, S>() {
                    @Override
                    public <T> T execute(D databaseConf, S dbConnect, String sql, List<Object> params) throws Exception {
                        return (T) updateBySqlMethod(databaseConf, dbConnect, sql, params);
                    }
                }
        );
    }

    @Override
    public PageResult dropSequence(DatabaseSetting databaseSetting, DropSequenceBean dropSequenceBean) throws Exception {
        SequenceHandler sequenceHandler = getSequenceHandler(getDataConf(databaseSetting));
        String sql = sequenceHandler.parseDropSequenceBean(dropSequenceBean);
        System.out.println(sql);
        return execute(
                databaseSetting,
                sql,
                new ArrayList<>(0),
                this::sqlRawParse,
                new ExecuteSqlMethod<D, S>() {
                    @Override
                    public <T> T execute(D databaseConf, S dbConnect, String sql, List<Object> params) throws Exception {
                        return (T) updateBySqlMethod(databaseConf, dbConnect, sql, params);
                    }
                }
        );
    }

    /**
     * SQL 解析
     *
     * @param database
     * @param params
     * @param sql
     * @return
     * @throws SqlParseException
     */
    protected String sqlParse(D database, List<Object> params, String sql) {
        String key = helper.getCacheKey(database, null) + sql;
        PrecompileSqlStatement precompileSqlStatement = sqlCache.get(key.hashCode() + "", s -> {

            SqlParseHandler sqlParseHandler = getSqlParseHandler(database);

            return sqlParseHandler.executeSqlParse(sql);
        });

        //参数预处理
        precompileSqlStatement.paramHandle(params);
        return precompileSqlStatement.getSql();
    }

    @Override
    protected QueryMethodResult testConnectMethod(S connect, D databaseConf) throws Exception {
        Connection connection = null;
        try {
            connection = connect.getDataSource().getConnection();
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
        return new QueryMethodResult();
    }

    @Override
    protected QueryMethodResult insertMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        List<Map<String, Object>> keys;
        int update = 0;
        boolean isTransaction = false;
        Connection connection = null;
        try {
            String sql = queryEntity.getAniInsert().getSql();
            sql = formatSql(sql, databaseConf, tableConf);

            recordExecuteStatementLog(sql, queryEntity.getAniInsert().getParamArray());

            keys = null;
            connection = this.getConnection(databaseConf, connect);
            if (!Objects.isNull(connection)) {
                isTransaction = true;
            }else {
                connection = connect.getDataSource().getConnection();
            }
            // 7.执行入库SQL
            if (CollectionUtil.isNotEmpty(queryEntity.getAniInsert().getReturnFields()) || queryEntity.getAniInsert().isReturnGeneratedKey()) {
                keys = new ArrayList<>();
                update = insertReturnValue(connection, databaseConf, tableConf, queryEntity, sql, keys);
            } else {
                try (PreparedStatement statement = connection.prepareStatement(sql)) {
                    //设置参数
                    statementSetObject(queryEntity.getAniInsert().getParamArray(), statement);
                    update = statement.executeUpdate();
                }
            }
            // 8.返回
            return new QueryMethodResult(update, keys);
        } finally {
            //没有开启事务，需要手动关闭连接
            if(!isTransaction) {
                if (connection != null) {
                    connection.close();
                }
            }
        }
    }

    /**
     * 新增返回自增值
     *
     * @param connection
     * @param queryEntity
     * @param sql
     * @param keys
     * @return
     * @throws Exception
     */
    protected Integer insertReturnValue(Connection connection, D databaseConf, T tableConf, Q queryEntity, String sql, List<Map<String, Object>> keys) throws Exception {
        PreparedStatement statement = null;
        ResultSet generatedKeys = null;
        int update = 0;
        try {
            if (CollectionUtil.isNotEmpty(queryEntity.getAniInsert().getReturnFields())) {
                statement = connection.prepareStatement(sql, queryEntity.getAniInsert().getReturnFields().toArray(new String[queryEntity.getAniInsert().getReturnFields().size()]));
            } else {
                statement = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            }

            //设置参数
            statementSetObject(queryEntity.getAniInsert().getParamArray(), statement);

            update = statement.executeUpdate();
            generatedKeys = statement.getGeneratedKeys();
            ResultSetMetaData metaData = generatedKeys.getMetaData();
            int columnCount = metaData.getColumnCount();
            while (generatedKeys.next()) {
                Map<String, Object> columnMap = new LinkedHashMap<>();
                keys.add(columnMap);
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metaData.getColumnName(i);
                    Object object = generatedKeys.getObject(i);
                    // GENERATED_KEY
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
    protected QueryMethodResult delMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        String sql = queryEntity.getAniDel().getSql();
        sql = formatSql(sql, databaseConf, tableConf);
        Object[] paramArray = queryEntity.getAniDel().getParamArray();
        int update = updateExecute(databaseConf, connect, sql, paramArray);
        return new QueryMethodResult(update, null);
    }

    @Override
    protected QueryMethodResult updateMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        String sql = queryEntity.getAniUpdate().getSql();
        sql = formatSql(sql, databaseConf, tableConf);
        Object[] paramArray = queryEntity.getAniUpdate().getParamArray();
        List<String> subSQLList = queryEntity.getAniUpdate().getSubSQL();
        List<Object[]> subParamArray = queryEntity.getAniUpdate().getSubParamArray();
        int update;
        if(CollectionUtil.isEmpty(subSQLList)) {
            update = updateExecute(databaseConf, connect, sql, paramArray);
            return new QueryMethodResult(update, null);
        }

        //subSQLList 不为空时
        subSQLList = subSQLList.stream().map(subSql-> {return formatSql(subSql, databaseConf, tableConf);}).collect(Collectors.toList());
        // 多 SQL 执行
        Connection connection = this.getConnection(databaseConf, connect);
        if (Objects.isNull(connection)) {
            try {
                connection = connect.getDataSource().getConnection();
                connection.setAutoCommit(false);
                update = updateExecute(connection, sql, paramArray, false);
                for (int i = 0; i < subSQLList.size(); i++) {
                    String subSql = subSQLList.get(i);
                    Object[] params = subParamArray.get(i);
                    updateExecute(connection, subSql, params, false);
                }
            }finally {
                if(connection != null)  {
                    connection.commit();
                    connection.setAutoCommit(true);
                    connection.close();
                }
            }

        }else {
            update = updateExecute(connection, sql, paramArray, false);
            for (int i = 0; i < subSQLList.size(); i++) {
                String subSql = subSQLList.get(i);
                Object[] params = subParamArray.get(i);
                updateExecute(connection, subSql, params, false);
            }
        }
        return new QueryMethodResult(update, null);
    }

    @Override
    protected QueryMethodResult queryMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        AniHandler.AniQuery aniQuery = queryEntity.getAniQuery();
        String sql = aniQuery.getSql();
        String sqlCount = aniQuery.getSqlCount();
        Object[] paramArray = aniQuery.getParamArray();

        sql = formatSql(sql, databaseConf, tableConf);
        queryEntity.setQueryStr(transitionQueryStr(sql, paramArray));
        Long total = 0L;
        List<Map<String, Object>> rows = new ArrayList<>();
        if (aniQuery.getCount() && !aniQuery.isAgg()) {
            sqlCount = formatSql(sqlCount, databaseConf, tableConf);
            Map<String, Object> totalMap = queryExecute(databaseConf, connect, sqlCount, new MapHandler(), paramArray);
            total = Long.parseLong(totalMap.get("count").toString());
            if (total > 0) {
                rows = queryExecute(databaseConf, connect, sql, new MapListHandler(getRowProcessor(databaseConf)), paramArray);
            }
        } else {
            rows = queryExecute(databaseConf, connect, sql, new MapListHandler(getRowProcessor(databaseConf)), paramArray);
        }
        if(aniQuery.isAgg()){
            return new QueryMethodResult(total, AggResultTranslateUtil.getRelationDataBaseTreeAggResult(rows, aniQuery.getAggNames()));
        }
        return new QueryMethodResult(total, rows);
    }

    protected boolean tableExists(S connect, D databaseConf, T tableConf) throws Exception {
        QueryMethodResult queryMethodResult = tableExistsMethod(connect, databaseConf, tableConf);
        List<Map<String, Object>> rows = queryMethodResult.getRows();
        Map<String, Object> existsMap = rows.get(0);
        boolean exists = (boolean) existsMap.get(tableConf.getTableName());
        return exists;
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
            log.error("统计接口判断目标表是否存在失败 dbId: [{}]!", tableConf.getTableName(), e);
        }
        AniHandler.AniQuery aniQuery = queryEntity.getAniQuery();
        String sqlCount = aniQuery.getSqlCount();
        sqlCount = formatSql(sqlCount, databaseConf, tableConf);
        Object[] paramArray = aniQuery.getParamArray();
        queryEntity.setQueryStr(transitionQueryStr(sqlCount, paramArray));
        Long total = 0L;
        Map<String, Object> totalMap = queryExecute(databaseConf, connect, sqlCount, new MapHandler(), paramArray);
        total = Long.parseLong(totalMap.get("count").toString());
        return new QueryMethodResult(total, null);
    }

    @Override
    protected QueryMethodResult saveOrUpdateMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        String updateSql = queryEntity.getAniUpsert().getUpdateSql();
        updateSql = formatSql(updateSql, databaseConf, tableConf);
        Object[] updateParamArray = queryEntity.getAniUpsert().getUpdateParamArray();

        int update = updateExecute(databaseConf, connect, updateSql,updateParamArray);

        if (update > 0) {
            // 若更新结果大于 0 说明更新成功，则不进行插入
            return new QueryMethodResult(update, null);
        }

        // 执行插入
        String insertSql = queryEntity.getAniUpsert().getInsertSql();

        Object[] paramArray = queryEntity.getAniUpsert().getParamArray();
        if(paramArray == null) paramArray = new Object[0];

        insertSql = formatSql(insertSql, databaseConf, tableConf);
        update = updateExecute(databaseConf, connect, insertSql, paramArray);
        return new QueryMethodResult(update, null);
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
                stmt.setFetchSize(Integer.MIN_VALUE);
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


    /**
     * 滚动游标获取数据
     *
     * @param consumer
     * @param rollAllData
     * @param total
     * @param batchSize
     * @param cursor
     * @return
     */
    protected QueryCursorMethodResult getQueryCursorMethodResult(D databaseConf, Consumer<Map<String, Object>> consumer, Boolean rollAllData, Long total, Integer batchSize, RdbmsCursorCache cursor) throws SQLException {
        try {
            ResultSet resultSet = cursor.getResultSet();
            // 滚动数据量
            long rollTotal = 0;
            // 遍历游标
            while (resultSet.next()) {
                // 调用行处理器解析数据
                BasicRowProcessor rowProcessor = getRowProcessor(databaseConf);
                Map<String, Object> rowMap = rowProcessor.toMap(resultSet);
                rollTotal++;
                consumer.accept(rowMap);
                // 非一次滚完所有数据，并且当前滚动数据量大于等于批量数即可返回
                if (!rollAllData && rollTotal >= batchSize) {
                    break;
                }
            }
            // 判断是否当前游标滚动完所有数据才结束
            if (!rollAllData) {
                // 当前只滚动批量数即返回，则判断是否还有数据，是否需要保存游标
                if (!resultSet.isAfterLast() && rollTotal != 0) {
                    return new QueryCursorMethodResult(total, cursor);
                } else {
                    rollAllData = Boolean.TRUE;
                    return new QueryCursorMethodResult(total, null);
                }
            } else {
                // 当次滚动完所有数据
                return new QueryCursorMethodResult(total, null);
            }
        } catch (Exception e) {
            rollAllData = Boolean.TRUE;
            throw e;
        } finally {
            if (rollAllData) {
                cursor.closeCursor();
            }
        }
    }

    /**
     * 查询语句（用于日志输出）
     *
     * @param sql
     * @param paramArray
     * @return
     */
    protected String transitionQueryStr(String sql, Object[] paramArray) {
        try {
            List<Object> paramList = new ArrayList<>(0);
            if(paramArray != null) {
                paramList = Arrays.asList(paramArray);
            }
            return "sql语句：[" + sql + "]，参数：" + paramList;
        } catch (Exception e) {
            log.error("sql: [{}] parser queryStr fail!", sql, e);
        }
        return null;
    }


    @Deprecated
    public int updateExecute(S connect, String sql, Object[] paramArray) throws Exception {
        return updateExecute(null, connect, sql, paramArray);
    }


    @Deprecated
    public  <T> T queryExecute(S connect, String sql, ResultSetHandler<T> rsh, Object[] paramArray) throws SQLException {
        return queryExecute(null, connect, sql, rsh, paramArray);
    }


    @Override
    protected DbTransactionModuleService getDbTransactionModuleService(DatabaseSetting databaseSetting,
                                                                       String transactionIdKey,
                                                                       String transactionFirstKey,
                                                                       String isolationLevelKey,
                                                                       String transactionId,
                                                                       IsolationLevel isolationLevel) {
        return new RdbmsTransactionModuleService(
                databaseSetting, this, transactionId, isolationLevel, transactionIdKey, transactionFirstKey, isolationLevelKey
        );
    }

    /**
     * 格式化sql, 替换字符串
     * @return
     */
    protected abstract String formatSql(String sql, D databaseConf, T tableConf);

    protected abstract SqlParseHandler getSqlParseHandler(D database);

    protected abstract SequenceHandler getSequenceHandler(D database);
}
