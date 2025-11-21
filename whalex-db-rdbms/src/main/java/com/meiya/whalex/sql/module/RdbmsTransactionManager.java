package com.meiya.whalex.sql.module;

import com.meiya.whalex.db.entity.TransactionManager;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * 关系型数据库事务管理器
 *
 * @author zhangwuqing
 * @date 2022/4/30 下午 5:30
 */
public class RdbmsTransactionManager implements TransactionManager<Connection> {

    private Connection connection;

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    @Override
    public Connection getTransactionClient() {
        return connection;
    }

    @Override
    public void rollback() throws Exception {
        try {
            connection.rollback();
        } catch (SQLException e) {
            throw new SQLException(e);
        } finally {
            connection.close();
        }
    }

    @Override
    public void commit() throws Exception {
        try {
            connection.commit();
        } catch (SQLException e) {
            throw new SQLException(e);
        } finally {
            connection.close();
        }
    }

    @Override
    public void transactionTimeOut() throws Exception {
        rollback();
    }
}
