package com.meiya.whalex.db.module.graph;

import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.async.AsyncSession;

/**
 * @author 黄河森
 * @date 2023/1/3
 * @package com.meiya.whalex.db.module.graph
 * @project whalex-data-driver
 */
public interface Neo4jExecuteCallback<SYNC_RESULT, ASYNC_RESULT> {

    /**
     * 事务执行
     *
     * @param transaction
     * @return
     * @throws Exception
     */
    SYNC_RESULT doWithTransaction(Neo4jTransaction transaction) throws Exception;

    /**
     * 异步调用
     *
     * @param transaction
     * @return
     * @throws Exception
     */
    ASYNC_RESULT doAsyncWithTransaction(Neo4jTransaction transaction) throws Exception;

    /**
     * 会话执行
     *
     * @param session
     * @return
     * @throws Exception
     */
    SYNC_RESULT doWithSession(Session session) throws Exception;

    /**
     * 异步执行
     *
     * @param session
     * @return
     * @throws Exception
     */
    ASYNC_RESULT doAsyncWithSession(AsyncSession session) throws Exception;

}
