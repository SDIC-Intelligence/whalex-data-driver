package com.meiya.whalex.sql.module;

import com.meiya.whalex.db.constant.IsolationLevel;
import com.meiya.whalex.db.constant.TransactionConstant;
import com.meiya.whalex.db.entity.AbstractDatabaseInfo;
import com.meiya.whalex.db.entity.AbstractDbTableInfo;
import com.meiya.whalex.db.entity.DataResult;
import com.meiya.whalex.db.entity.DatabaseSetting;
import com.meiya.whalex.db.entity.ani.AniBinaryStreamParameter;
import com.meiya.whalex.db.entity.ani.AniHandler;
import com.meiya.whalex.db.module.AbstractDbRawStatementModule;
import com.meiya.whalex.db.module.DatabaseExecuteStatementLog;
import com.meiya.whalex.exception.BusinessException;
import com.meiya.whalex.exception.ExceptionCode;
import com.meiya.whalex.sql.ani.Cell;
import com.meiya.whalex.sql.ani.DatMapListHandler;
import com.meiya.whalex.sql.ani.RdbDataResult;
import com.meiya.whalex.sql.ani.RdbmsCursorCache;
import com.meiya.whalex.util.concurrent.ThreadLocalUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.dbutils.BasicRowProcessor;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.dbutils.handlers.MapListHandler;
import org.apache.commons.lang3.StringUtils;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

@Slf4j
public abstract class RdbmsRawStatementModuleImpl<S extends QueryRunner,
        Q extends AniHandler,
        D extends AbstractDatabaseInfo,
        T extends AbstractDbTableInfo,
        C extends RdbmsCursorCache> extends AbstractDbRawStatementModule<S, Q, D, T, C> {


    /**
     * 获取自定义处理器
     *
     * @param databaseConf
     * @return
     */
    protected abstract BasicRowProcessor getRowProcessor(D databaseConf);

    /**
     * 判断是否是更新操作
     * @param rawStatement
     * @return
     */
    private boolean isUpdate(String rawStatement) {

        String[] split = rawStatement.trim().split("\\s+");

        //insert
        if(split[0].equalsIgnoreCase("insert")) {
            return true;
        }

        //update
        if(split[0].equalsIgnoreCase("update")) {
            return true;
        }

        //delete
        if(split[0].equalsIgnoreCase("delete")) {
            return true;
        }

        //create
        if(split[0].equalsIgnoreCase("create")) {
            return true;
        }

        //drop
        if(split[0].equalsIgnoreCase("drop")) {
            return true;
        }

        //alter
        if(split[0].equalsIgnoreCase("alter")) {
            return true;
        }

        //comment
        if(split[0].equalsIgnoreCase("comment")) {
            return true;
        }

        //truncate
        if(split[0].equalsIgnoreCase("truncate")) {
            return true;
        }

        return false;

    }

    protected DataResult _updateCallableExecute(DatabaseSetting databaseSetting, String rawStatement, List<Object> params, CustomSqlParser<D> customSqlParser) throws Exception {

        return _execute(databaseSetting, rawStatement, params, customSqlParser, new ExecuteSqlMethod<D, S>() {
            @Override
            public <T> T execute(D databaseConf, S dbConnect, String sql, List<Object> params) throws Exception {
                return (T) updateCallableExecute(databaseConf, dbConnect, sql, params.toArray());
            }
        });
    }

    protected <T> T _queryExecute(DatabaseSetting databaseSetting, String rawStatement, List<Object> params, CustomSqlParser<D> customSqlParser, ResultSetHandler<T> rsh) throws Exception {
        return _execute(databaseSetting, rawStatement, params, customSqlParser, new ExecuteSqlMethod<D, S>() {
            @Override
            public <T> T execute(D databaseConf, S dbConnect, String sql, List<Object> params) throws Exception {
                return (T) queryExecute(databaseConf, dbConnect, sql, rsh, params.toArray());
            }
        });
    }

    protected <T> T _queryExecute(DatabaseSetting databaseSetting, String rawStatement, List<Object> params, CustomSqlParser<D> customSqlParser) throws Exception {
        return _execute(databaseSetting, rawStatement, params, customSqlParser, new ExecuteSqlMethod<D, S>() {
            @Override
            public <T> T execute(D databaseConf, S dbConnect, String sql, List<Object> params) throws Exception {
                return (T) queryExecute(databaseConf, dbConnect, sql, new MapListHandler(getRowProcessor(databaseConf)), params.toArray());
            }
        });
    }

    @Override
    public DataResult rawStatementExecute(DatabaseSetting databaseSetting, String rawStatement) throws Exception {

        if(isUpdate(rawStatement)) {
            return _updateCallableExecute(databaseSetting, rawStatement, new ArrayList<>(), this::sqlRawParse);
        }

        final DatMapListHandler[] datMapListHandlerArr = {null};

        List<Map<String, Object>> rows = _execute(databaseSetting, rawStatement, new ArrayList<>(), this::sqlRawParse, new ExecuteSqlMethod<D, S>() {
            @Override
            public <T> T execute(D databaseConf, S dbConnect, String sql, List<Object> params) throws Exception {

                datMapListHandlerArr[0] = new DatMapListHandler(getRowProcessor(databaseConf));

                return (T) queryExecute(databaseConf, dbConnect, sql, datMapListHandlerArr[0], params.toArray());
            }
        });

        RdbDataResult rdbDataResult = new RdbDataResult();

        Map<String, Object> data = new HashMap<>();
        data.put("data", rows);
        DatMapListHandler datMapListHandler = datMapListHandlerArr[0];
        if(datMapListHandlerArr != null) {
            data.put("head", datMapListHandler.getHeadList());
        }
        rdbDataResult.setData(data);
        rdbDataResult.setTotal(rows.size());



        return rdbDataResult;

    }


    protected String sqlRawParse(D database, List<Object> params, String sql) {
        return sql;
    }



    public interface CustomSqlParser<D extends AbstractDatabaseInfo>{
        String sqlParse(D database, List<Object> params, String sql);
    }

    class RdbmsParamHandler implements Function<D, String>{

        CustomSqlParser customSqlParser;
        List<Object> params;
        String sql;

        public RdbmsParamHandler(CustomSqlParser customSqlParser, List<Object> params, String sql) {
            this.customSqlParser = customSqlParser;
            this.params = params;
            this.sql = sql;
        }

        @Override
        public String apply(D dataConf) {
            if (customSqlParser != null) {
                return customSqlParser.sqlParse(dataConf, params, sql);
            } else {
                throw new RuntimeException("sql解析器不能为空");
            }
        }
    }

    class RdbmsExecutor implements Executor<S, D>{

        ExecuteSqlMethod<D, S> executeSqlMethod;

        List<Object> params;

        public RdbmsExecutor(ExecuteSqlMethod<D, S> executeSqlMethod, List<Object> params) {
            this.executeSqlMethod = executeSqlMethod;
            this.params = params;
        }

        @Override
        public <R, P> R execute(D dataConf, S dbConnect, P sql) {
            try {
                return executeSqlMethod.execute(dataConf, dbConnect, (String) sql, params);
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }
    }

    protected  <T> T _execute(DatabaseSetting databaseSetting, String sql, List<Object> params, CustomSqlParser<D> customSqlParser, ExecuteSqlMethod<D, S> executeSqlMethod) throws Exception {

        if(params == null) {
            params = new ArrayList<>();
        }

        RdbmsParamHandler rdbmsParamHandler = new RdbmsParamHandler(customSqlParser, params, sql);
        RdbmsExecutor rdbmsExecutor = new RdbmsExecutor(executeSqlMethod, params);

        return _execute(databaseSetting, rdbmsParamHandler, rdbmsExecutor);
    }

    /**
     * 设置参数
     *
     * @param paramArray
     * @param statement
     * @throws SQLException
     */
    protected void statementSetObject(Object[] paramArray, PreparedStatement statement) throws SQLException {
        if (paramArray != null && paramArray.length > 0) {
            for (int i = 0; i < paramArray.length; i++) {
                Object param = paramArray[i];
                if (param instanceof AniBinaryStreamParameter) {
                    AniBinaryStreamParameter binaryStreamParameter = (AniBinaryStreamParameter) param;
                    statement.setBinaryStream(i + i, binaryStreamParameter.getInputStream(), binaryStreamParameter.getLength());
                } else {
                    statement.setObject(i + 1, param);
                }
            }
        }
    }

    /**
     * 执行非 插入与查询的 SQL
     *
     * @param databaseConf
     * @param connect
     * @param sql
     * @param paramArray
     * @return
     * @throws Exception
     */
    public int updateExecute(D databaseConf, S connect, String sql, Object[] paramArray) throws Exception {
        Connection connection = null;
        boolean isTransaction = false;
        connection = this.getConnection(databaseConf, connect);
        if (Objects.isNull(connection)) {
            connection = connect.getDataSource().getConnection();
        } else {
            // 开启事务
            isTransaction = true;
        }
        return updateExecute(connection, sql, paramArray, !isTransaction);
    }

    /**
     * 执行非 插入与查询的 SQL
     *
     * @param connection
     * @param sql
     * @param paramArray
     * @param isClose
     * @return
     * @throws Exception
     */
    public int updateExecute(Connection connection, String sql, Object[] paramArray, boolean isClose) throws Exception {

        recordExecuteStatementLog(sql, paramArray);

        try {
            try (PreparedStatement statement = connection.prepareStatement(sql)){
                statementSetObject(paramArray, statement);
                return statement.executeUpdate();
            }
        } finally {
            if (isClose) {
                if (connection != null) {
                    connection.close();
                }
            }
        }
    }

    public RdbDataResult updateCallableExecute(D databaseConf, S connect, String sql, Object[] paramArray) throws Exception {
        Connection connection = null;
        boolean isTransaction = false;
        connection = this.getConnection(databaseConf, connect);
        if (Objects.isNull(connection)) {
            connection = connect.getDataSource().getConnection();
        } else {
            // 开启事务
            isTransaction = true;
        }
        RdbDataResult rdbDataResult = updateCallableExecute(databaseConf, connection, sql, paramArray, !isTransaction);
        return rdbDataResult;
    }

    public RdbDataResult updateCallableExecute(D databaseConf, Connection connection, String sql, Object[] paramArray, boolean isClose) throws Exception {

        recordExecuteStatementLog(sql, paramArray);

        try {
            try (CallableStatement statement = connection.prepareCall(sql)){
                statementSetObject(paramArray, statement);
                int update = statement.executeUpdate();
                ResultSet resultSet = statement.getResultSet();

                DatMapListHandler datMapListHandler = new DatMapListHandler(getRowProcessor(databaseConf));
                Map<String, Object> data = new HashMap<>();
                if(resultSet != null) {
                    List<Map<String, Object>> rows = datMapListHandler.handle(resultSet);
                    resultSet.close();
                    data.put("data", rows);
                    data.put("head", datMapListHandler.getHeadList());
                }
                RdbDataResult rdbDataResult = new RdbDataResult();
                rdbDataResult.setTotal(update);
                rdbDataResult.setData(data);
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

    public  <T> T queryExecute(D databaseConf, S connect, String sql, ResultSetHandler<T> rsh, Object[] paramArray) throws SQLException {
        recordExecuteStatementLog(sql, paramArray);
        // 判断是否开启事务
        Connection connection = this.getConnection(databaseConf, connect);
        if (connection != null) {
            return connect.query(connection, sql, rsh, paramArray);
        } else {
            return connect.query(sql, rsh, paramArray);
        }
    }

    /**
     * 获取连接
     *
     * @param connect
     * @return Connection 连接对象
     * @throws Exception
     */
    protected Connection getConnection(D databaseConf, S connect) throws SQLException {

        Connection connection = null;

        if(databaseConf != null) {
            // 1.判断 ThreadLocal 中是否存在 TransactionId
            String databaseKey = this.helper.getCacheKey(databaseConf, null);
            String transactionId = (String) ThreadLocalUtil.get(databaseKey.hashCode() + "_" + TransactionConstant.ID);
            if (StringUtils.isNotBlank(transactionId)) {
                //2.判断时候第一次开启事务
                String first = (String) ThreadLocalUtil.get(databaseKey.hashCode() + "_" + TransactionConstant.FIRST);
                RdbmsTransactionManager transactionManager;
                if (StringUtils.isBlank(first)) {
                    // 3.不是第一次则判断 this.getHelper().getTransactionManager(TransactionId) 是否获取得到TransactionManager
                    transactionManager = (RdbmsTransactionManager) this.getHelper().getTransactionManager(transactionId);
                    if (Objects.isNull(transactionManager)) {
                        throw new BusinessException(ExceptionCode.TRANSACTION_NULL_EXCEPTION);
                    }
                    // 4.若 从第三步中获取到 TransactionManager，则调用 TransactionManager.getTransactionClient 获取 connection
                    connection = transactionManager.getTransactionClient();
                } else {
                    // 5.是第一次则第一次创建事务对象 将 connection.setAutoCommit(false) 以及 事务级别 并将 connection 作为事务管理对象放到 TransactionManager 中
                    transactionManager = (RdbmsTransactionManager) this.getHelper().getTransactionManager(transactionId);
                    if (!Objects.isNull(transactionManager)) {
                        throw new BusinessException(ExceptionCode.TRANSACTION_ID_EXIST_EXCEPTION);
                    }
                    transactionManager = new RdbmsTransactionManager();
                    connection = connect.getDataSource().getConnection();
                    // 设置事务隔离级别
                    Integer isolationLevel = (Integer) ThreadLocalUtil.get(databaseKey.hashCode() + "_" + TransactionConstant.ISOLATION_LEVEL);
                    if (isolationLevel != null && isolationLevel != IsolationLevel.DEFAULT.value()) {
                        // 判断是否与当前默认事务级别相同，不相同才设置
                        int transactionIsolation = connection.getTransactionIsolation();
                        if (transactionIsolation != isolationLevel) {
                            connection.setTransactionIsolation(isolationLevel);
                        }
                    }
                    connection.setAutoCommit(false);
                    transactionManager.setConnection(connection);
                    this.getHelper().createTransactionManager(transactionId, transactionManager);
                    //6.删除第一次开始事务标志位
                    ThreadLocalUtil.set(databaseKey.hashCode() + "_" + TransactionConstant.FIRST, null);
                }

            }
        }

        if(connection == null) {
            return getConnection(connect);
        }

        return connection;
    }

    private Connection getConnection(S connect) throws SQLException {
        Connection connection = null;
        // 1.判断 ThreadLocal 中是否存在 TransactionId
        String transactionId = (String) ThreadLocalUtil.get(TransactionConstant.ID);
        if (StringUtils.isNotBlank(transactionId)) {
            //2.判断时候第一次开启事务
            String first = (String) ThreadLocalUtil.get(TransactionConstant.FIRST);
            RdbmsTransactionManager transactionManager;
            if (StringUtils.isBlank(first)) {
                // 3.不是第一次则判断 this.getHelper().getTransactionManager(TransactionId) 是否获取得到TransactionManager
                transactionManager = (RdbmsTransactionManager) this.getHelper().getTransactionManager(transactionId);
                if (Objects.isNull(transactionManager)) {
                    throw new BusinessException(ExceptionCode.TRANSACTION_NULL_EXCEPTION);
                }
                // 4.若 从第三步中获取到 TransactionManager，则调用 TransactionManager.getTransactionClient 获取 connection
                connection = transactionManager.getTransactionClient();
            } else {
                // 5.是第一次则第一次创建事务对象 将 connection.setAutoCommit(false) 并将 connection 作为事务管理对象放到 TransactionManager 中
                transactionManager = (RdbmsTransactionManager) this.getHelper().getTransactionManager(transactionId);
                if (!Objects.isNull(transactionManager)) {
                    throw new BusinessException(ExceptionCode.TRANSACTION_ID_EXIST_EXCEPTION);
                }
                transactionManager = new RdbmsTransactionManager();
                connection = connect.getDataSource().getConnection();
                // 设置事务隔离级别
                Integer isolationLevel = (Integer) ThreadLocalUtil.get(TransactionConstant.ISOLATION_LEVEL);
                if (isolationLevel != null && isolationLevel != IsolationLevel.DEFAULT.value()) {
                    // 判断是否与当前默认事务级别相同，不相同才设置
                    int transactionIsolation = connection.getTransactionIsolation();
                    if (transactionIsolation != isolationLevel) {
                        connection.setTransactionIsolation(isolationLevel);
                    }
                }
                connection.setAutoCommit(false);
                transactionManager.setConnection(connection);
                this.getHelper().createTransactionManager(transactionId, transactionManager);
                //6.删除第一次开始事务标志位
                ThreadLocalUtil.set(TransactionConstant.FIRST, null);
            }

        }
        return connection;
    }


    protected void recordExecuteStatementLog(String sql, Object[] paramArray) {
        StringBuilder builder = new StringBuilder();
        if (paramArray == null) {
            paramArray = new Object[0];
        }
        builder.append("sql:[").append(sql).append("]").append(", 参数：").append(Arrays.asList(paramArray));
        DatabaseExecuteStatementLog.set(builder.toString());
        if(log.isDebugEnabled()) {
            log.debug(builder.toString());
        }
    }

}
