package com.meiya.whalex.db.module.graph;

import com.meiya.whalex.exception.BusinessException;
import org.neo4j.driver.*;
import org.neo4j.driver.async.AsyncTransaction;
import org.neo4j.driver.async.ResultCursor;

import java.util.Map;
import java.util.concurrent.CompletionStage;

/**
 * @author 黄河森
 * @date 2023/3/16
 * @package com.meiya.whalex.db.module.graph
 * @project whalex-data-driver
 */
public class Neo4jTransaction implements Transaction, AsyncTransaction {

    /**
     * 同步事务
     */
    private Transaction transaction;

    /**
     * 异步事务
     */
    private AsyncTransaction asyncTransaction;

    public Neo4jTransaction(Transaction transaction) {
        this.transaction = transaction;
    }

    public Neo4jTransaction(AsyncTransaction asyncTransaction) {
        this.asyncTransaction = asyncTransaction;
    }

    @Override
    public void commit() {
        if (transaction == null) {
            throw new BusinessException("当前 Neo4J 未开启同步事务!");
        }
        transaction.commit();
    }

    @Override
    public void rollback() {
        if (transaction == null) {
            throw new BusinessException("当前 Neo4J 未开启同步事务!");
        }
        transaction.rollback();
    }

    @Override
    public void close() {
        if (transaction == null) {
            throw new BusinessException("当前 Neo4J 未开启同步事务!");
        }
        transaction.close();
    }

    @Override
    public Result run(String s, Value value) {
        if (transaction == null) {
            throw new BusinessException("当前 Neo4J 未开启同步事务!");
        }
        return transaction.run(s, value);
    }

    @Override
    public Result run(String s, Map<String, Object> map) {
        if (transaction == null) {
            throw new BusinessException("当前 Neo4J 未开启同步事务!");
        }
        return transaction.run(s, map);
    }

    @Override
    public Result run(String s, Record record) {
        if (transaction == null) {
            throw new BusinessException("当前 Neo4J 未开启同步事务!");
        }
        return transaction.run(s, record);
    }

    @Override
    public Result run(String s) {
        if (transaction == null) {
            throw new BusinessException("当前 Neo4J 未开启同步事务!");
        }
        return transaction.run(s);
    }

    @Override
    public Result run(Query query) {
        if (transaction == null) {
            throw new BusinessException("当前 Neo4J 未开启同步事务!");
        }
        return transaction.run(query);
    }

    @Override
    public CompletionStage<Void> commitAsync() {
        if (asyncTransaction == null) {
            throw new BusinessException("当前 Neo4J 未开启异步事务!");
        }
        return asyncTransaction.commitAsync();
    }

    @Override
    public CompletionStage<Void> rollbackAsync() {
        if (asyncTransaction == null) {
            throw new BusinessException("当前 Neo4J 未开启异步事务!");
        }
        return asyncTransaction.rollbackAsync();
    }

    @Override
    public CompletionStage<Void> closeAsync() {
        if (asyncTransaction == null) {
            throw new BusinessException("当前 Neo4J 未开启异步事务!");
        }
        return asyncTransaction.closeAsync();
    }

    @Override
    public CompletionStage<ResultCursor> runAsync(String s, Value value) {
        if (asyncTransaction == null) {
            throw new BusinessException("当前 Neo4J 未开启异步事务!");
        }
        return asyncTransaction.runAsync(s, value);
    }

    @Override
    public CompletionStage<ResultCursor> runAsync(String s, Map<String, Object> map) {
        if (asyncTransaction == null) {
            throw new BusinessException("当前 Neo4J 未开启异步事务!");
        }
        return asyncTransaction.runAsync(s, map);
    }

    @Override
    public CompletionStage<ResultCursor> runAsync(String s, Record record) {
        if (asyncTransaction == null) {
            throw new BusinessException("当前 Neo4J 未开启异步事务!");
        }
        return asyncTransaction.runAsync(s, record);
    }

    @Override
    public CompletionStage<ResultCursor> runAsync(String s) {
        if (asyncTransaction == null) {
            throw new BusinessException("当前 Neo4J 未开启异步事务!");
        }
        return asyncTransaction.runAsync(s);
    }

    @Override
    public CompletionStage<ResultCursor> runAsync(Query query) {
        if (asyncTransaction == null) {
            throw new BusinessException("当前 Neo4J 未开启异步事务!");
        }
        return asyncTransaction.runAsync(query);
    }

    @Override
    public boolean isOpen() {
        if (transaction != null) {
            return transaction.isOpen();
        } else {
            throw new BusinessException("当前 Neo4J 未开启同步事务, 并且异步事务不支持该操作!");
        }
    }
}
