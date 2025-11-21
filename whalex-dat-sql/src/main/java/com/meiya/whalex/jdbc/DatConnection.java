package com.meiya.whalex.jdbc;

import com.meiya.whalex.db.builder.DatabaseConfigurationBuilder;
import com.meiya.whalex.db.constant.IsolationLevel;
import com.meiya.whalex.db.constant.SupportPower;
import com.meiya.whalex.db.entity.DatabaseSetting;
import com.meiya.whalex.db.entity.PageResult;
import com.meiya.whalex.db.entity.TableSetting;
import com.meiya.whalex.db.module.DbModuleService;
import com.meiya.whalex.db.template.BaseDbConfTemplate;
import com.meiya.whalex.interior.db.constant.DbResourceEnum;
import com.meiya.whalex.util.DbBeanManagerUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.lang3.StringUtils;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;


/**
 * dat连接对象
 *
 * @author 蔡荣桂
 * @date 2022/06/27
 * @project whalex-dat-sql
 */
@Slf4j
public class DatConnection implements Connection {

    private BaseDbConfTemplate baseDbConfTemplate;

    private DatabaseSetting databaseSetting;

    private DbModuleService service;

    private DbModuleService oldService;

    private DbResourceEnum dbType;

    private boolean autoCommit = true;

    private String url;

    private String userName;

    private String databaseName;

    /**
     * 事务级别
     */
    private IsolationLevel isolationLevel;

    public DatConnection(String url, String userName, String databaseName, BaseDbConfTemplate baseDbConfTemplate, DbModuleService service, DbResourceEnum dbType) {
        this.url = url;
        this.userName = userName;
        this.databaseName = databaseName;
        this.baseDbConfTemplate = baseDbConfTemplate;
        this.service = service;
        this.dbType = dbType;
        this.databaseSetting = DatabaseConfigurationBuilder.builder()
                .type(dbType).database(baseDbConfTemplate).build();
    }

    @Override
    public Statement createStatement() throws SQLException {
        return new DatStatement(this, baseDbConfTemplate, dbType, this.databaseSetting);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        if(StringUtils.isBlank(sql)) {
            throw new SQLException("sql不能为空");
        }
        return new DatPreparedStatement(this, sql, baseDbConfTemplate, dbType, databaseSetting);
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        return null;
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        return sql;
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        if (this.autoCommit && !autoCommit) {
            this.autoCommit = false;
            oldService = service;
            service = service.newTransaction(databaseSetting, isolationLevel);
        } else if (!this.autoCommit && autoCommit) {
            //提交事务
            try {
                service.commit();
                service = oldService;
                this.autoCommit = true;
                oldService = null;
            } catch (Exception e) {
                throw new RuntimeException("设置 autoCommit 为 true，提交已存在的事务发生异常!", e);
            }
        }
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return autoCommit;
    }

    @Override
    public void commit() throws SQLException {
        try {
            if (!autoCommit) {
                service.commit();
                service = oldService;
                oldService = null;
                this.autoCommit = true;
            }
        } catch (Exception e) {
            service = oldService;
            this.autoCommit = true;
            oldService = null;
            throw new RuntimeException("事务提交失败", e);
        }
    }

    @Override
    public void rollback() throws SQLException {
        try {
            if (!autoCommit) {
                service.rollback();
                service = oldService;
                oldService = null;
                this.autoCommit = true;
            }
        } catch (Exception e) {
            service = oldService;
            this.autoCommit = true;
            oldService = null;
            throw new RuntimeException("事务回滚失败", e);
        }
    }

    @Override
    public void close() throws SQLException {

    }

    @Override
    public boolean isClosed() throws SQLException {
        return false;
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        List<SupportPower> supportList = service.getSupportList();
        if(supportList.contains(SupportPower.ASSOCIATED_QUERY)) {

            try {
                QueryRunner dataSource = DbBeanManagerUtil.getDataSource(dbType, databaseSetting, new TableSetting(), QueryRunner.class);
                return new DatDatabaseMetaData(url, userName, supportList, dataSource.getDataSource(), dbType);
            } catch (Exception e) {
                e.printStackTrace();
            }

        }

        return new DatDatabaseMetaData(url, userName, supportList, dbType);
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        log.error(this.getClass().getName() + "#" + "setReadOnly 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return false;
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        log.error(this.getClass().getName() + "#" + "setCatalog 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public String getCatalog() throws SQLException {
        return databaseName;
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        this.isolationLevel = IsolationLevel.parser(level);
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        return Connection.TRANSACTION_REPEATABLE_READ;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {

    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        log.error(this.getClass().getName() + "#" + "createStatement 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        log.error(this.getClass().getName() + "#" + "prepareStatement 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        log.error(this.getClass().getName() + "#" + "prepareCall 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        log.error(this.getClass().getName() + "#" + "getTypeMap 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        log.error(this.getClass().getName() + "#" + "setTypeMap 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        log.error(this.getClass().getName() + "#" + "setHoldability 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public int getHoldability() throws SQLException {
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        log.error(this.getClass().getName() + "#" + "setSavepoint 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        log.error(this.getClass().getName() + "#" + "setSavepoint 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        log.error(this.getClass().getName() + "#" + "rollback 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        log.error(this.getClass().getName() + "#" + "releaseSavepoint 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        log.error(this.getClass().getName() + "#" + "createStatement 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        log.error(this.getClass().getName() + "#" + "prepareStatement 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        log.error(this.getClass().getName() + "#" + "prepareCall 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        if(StringUtils.isBlank(sql)) {
            throw new SQLException("sql不能为空");
        }
        return new DatPreparedStatement(this, sql, baseDbConfTemplate, dbType, databaseSetting, autoGeneratedKeys);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        log.error(this.getClass().getName() + "#" + "prepareStatement 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        if(StringUtils.isBlank(sql)) {
            throw new SQLException("sql不能为空");
        }
        return new DatPreparedStatement(this, sql, baseDbConfTemplate, dbType, databaseSetting, columnNames);
    }

    @Override
    public Clob createClob() throws SQLException {
        log.error(this.getClass().getName() + "#" + "createClob 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public Blob createBlob() throws SQLException {
        log.error(this.getClass().getName() + "#" + "createBlob 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public NClob createNClob() throws SQLException {
        log.error(this.getClass().getName() + "#" + "createNClob 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        log.error(this.getClass().getName() + "#" + "createSQLXML 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        try {
            PageResult pageResult = service.testConnection(databaseSetting);
            return pageResult.getSuccess();
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        log.error(this.getClass().getName() + "#" + "setClientInfo 方法未实现");
        throw new RuntimeException("方法未实现");
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        log.error(this.getClass().getName() + "#" + "setClientInfo 方法未实现");
        throw new RuntimeException("方法未实现");
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        log.error(this.getClass().getName() + "#" + "getClientInfo 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        log.error(this.getClass().getName() + "#" + "getClientInfo 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        log.error(this.getClass().getName() + "#" + "createArrayOf 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        log.error(this.getClass().getName() + "#" + "createStruct 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        log.error(this.getClass().getName() + "#" + "setSchema 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public String getSchema() throws SQLException {
        log.error(this.getClass().getName() + "#" + "getSchema 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        log.error(this.getClass().getName() + "#" + "abort 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        log.error(this.getClass().getName() + "#" + "setNetworkTimeout 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        log.error(this.getClass().getName() + "#" + "getNetworkTimeout 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        log.error(this.getClass().getName() + "#" + "unwrap 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        log.error(this.getClass().getName() + "#" + "isWrapperFor 方法未实现");
        throw new SQLException("方法未实现");
    }

    public DbModuleService getService() {
        return service;
    }
}
