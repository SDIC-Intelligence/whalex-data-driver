package com.meiya.whalex.db.module.graph;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.map.MapUtil;
import com.meiya.whalex.annotation.DbService;
import com.meiya.whalex.annotation.Support;
import com.meiya.whalex.db.constant.SupportPower;
import com.meiya.whalex.db.constant.TransactionConstant;
import com.meiya.whalex.db.entity.*;
import com.meiya.whalex.db.entity.graph.*;
import com.meiya.whalex.exception.BusinessException;
import com.meiya.whalex.exception.ExceptionCode;
import com.meiya.whalex.graph.constant.OperationField;
import com.meiya.whalex.graph.entity.GraphResultSet;
import com.meiya.whalex.graph.entity.QlResult;
import com.meiya.whalex.graph.module.AbstractGraphModuleBaseService;
import com.meiya.whalex.interior.db.constant.CloudVendorsEnum;
import com.meiya.whalex.interior.db.constant.DbResourceEnum;
import com.meiya.whalex.interior.db.constant.DbVersionEnum;
import com.meiya.whalex.util.concurrent.ThreadLocalUtil;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.driver.*;
import org.neo4j.driver.async.AsyncSession;
import org.neo4j.driver.async.AsyncTransaction;
import org.neo4j.driver.async.ResultCursor;
import org.neo4j.driver.internal.InternalNode;
import org.neo4j.driver.internal.InternalRelationship;
import org.neo4j.driver.internal.value.NodeValue;

import java.util.*;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * @author chenjp
 * @date 2020/9/27
 */
@Slf4j
@DbService(dbType = DbResourceEnum.neo4j, version = DbVersionEnum.NEO4J_4_4_11, cloudVendors = CloudVendorsEnum.OPEN)
@Support(value = {
        SupportPower.TEST_CONNECTION,
        SupportPower.CREATE,
        SupportPower.UPDATE,
        SupportPower.DELETE,
        SupportPower.SEARCH
})
public class Neo4jServiceImpl extends AbstractGraphModuleBaseService<Driver, Neo4jHandler, Neo4jDatabaseInfo, Neo4jTableInfo, AbstractCursorCache> {

    @Override
    protected QueryMethodResult queryMethod(Driver connect, Neo4jHandler neo4jHandler, Neo4jDatabaseInfo databaseConf, Neo4jTableInfo tableConf) throws Exception {
        QueryMethodResult queryMethodResult = new QueryMethodResult();
        Neo4jHandler.Neo4jQuery neo4jQuery = neo4jHandler.getNeo4jQuery();
        queryMethodResult.setRows(runToMap(connect, databaseConf.getDatabase(), neo4jQuery.getCql()));
        return queryMethodResult;
    }

    @Override
    protected QueryMethodResult countMethod(Driver connect, Neo4jHandler neo4jHandler, Neo4jDatabaseInfo databaseConf, Neo4jTableInfo tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "Neo4jServiceImpl.countMethod");
    }

    @Override
    protected QueryMethodResult testConnectMethod(Driver connect, Neo4jDatabaseInfo databaseConf) throws Exception {
        Session session = connect.session();
        boolean open = session.isOpen();
        if (!open) {
            session.close();
            throw new BusinessException(ExceptionCode.CONNECT_FAILURE_EXCEPTION);
        }
        return new QueryMethodResult();
    }

    @Override
    protected QueryMethodResult showTablesMethod(Driver connect, Neo4jHandler neo4jHandler, Neo4jDatabaseInfo databaseConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "Neo4jServiceImpl.showTablesMethod");
    }

    @Override
    protected QueryMethodResult getIndexesMethod(Driver connect, Neo4jDatabaseInfo databaseConf, Neo4jTableInfo tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "Neo4jServiceImpl.getIndexesMethod");
    }

    @Override
    protected QueryMethodResult createTableMethod(Driver connect, Neo4jHandler neo4jHandler, Neo4jDatabaseInfo databaseConf, Neo4jTableInfo tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "Neo4jServiceImpl.createTableMethod");
    }

    @Override
    protected QueryMethodResult dropTableMethod(Driver connect, Neo4jHandler neo4jHandler, Neo4jDatabaseInfo databaseConf, Neo4jTableInfo tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "Neo4jServiceImpl.dropTableMethod");
    }

    @Override
    protected QueryMethodResult deleteIndexMethod(Driver connect, Neo4jHandler neo4jHandler, Neo4jDatabaseInfo databaseConf, Neo4jTableInfo tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "Neo4jServiceImpl.deleteIndexMethod");
    }

    @Override
    protected QueryMethodResult createIndexMethod(Driver connect, Neo4jHandler neo4jHandler, Neo4jDatabaseInfo databaseConf, Neo4jTableInfo tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "Neo4jServiceImpl.createIndexMethod");
    }

    @Override
    protected QueryMethodResult insertMethod(Driver connect, Neo4jHandler neo4jHandler, Neo4jDatabaseInfo databaseConf, Neo4jTableInfo tableConf) throws Exception {
        Neo4jHandler.Neo4jInsert neo4jInsert = neo4jHandler.getNeo4jInsert();
        List<Map<String, Object>> executeSync = executeSync(connect, databaseConf.getDatabase(), new Neo4jExecuteCallback<List<Map<String, Object>>, List<Map<String, Object>>>() {

            /**
             * 组织返回的自增ID，与入参List顺序一致
             *
             * @param idIndex
             * @param vertexId
             * @param edgeId
             * @return
             */
            private List<Map<String, Object>> getIdentities(List<String> idIndex, List<String> vertexId, List<String> edgeId) {
                List<Map<String, Object>> identityResult = new ArrayList<>(idIndex.size());
                for (String index : idIndex) {
                    if (OperationField.VERTEX.equals(index)) {
                        identityResult.add(MapUtil.builder("id", (Object) vertexId.remove(0)).build());
                    } else {
                        identityResult.add(MapUtil.builder("id", (Object) edgeId.remove(0)).build());
                    }
                }
                return identityResult;
            }

            @Override
            public List<Map<String, Object>> doWithTransaction(Neo4jTransaction transaction) throws Exception {
                String vertexCql = neo4jInsert.getVertexCql();
                List<String> edgeCql = neo4jInsert.getEdgeCql();
                List<String> idIndex = neo4jInsert.getIdIndex();
                List<String> vertexId = new ArrayList<>();
                // 创建节点
                if (StringUtils.isNotBlank(vertexCql)) {
                    Result result = transaction.run(vertexCql);
                    while (result.hasNext()) {
                        Record next = result.next();
                        Collection<Object> values = next.asMap().values();
                        for (Object value : values) {
                            String identity = String.valueOf(((InternalNode) value).id());
                            vertexId.add(identity);
                        }
                    }
                }
                // 创建关系
                List<String> edgeId = new ArrayList<>();
                for (String edge : edgeCql) {
                    Result run = transaction.run(edge);
                    while (run.hasNext()) {
                        Record next = run.next();
                        Collection<Object> values = next.asMap().values();
                        for (Object value : values) {
                            String identity = String.valueOf(((InternalRelationship) value).id());
                            edgeId.add(identity);
                        }
                    }
                }

                // 组织返回的自增ID，与入参List顺序一致
                List<Map<String, Object>> identityResult = getIdentities(idIndex, vertexId, edgeId);
                return identityResult;
            }

            @Override
            public List<Map<String, Object>> doAsyncWithTransaction(Neo4jTransaction transaction) throws Exception {
                String vertexCql = neo4jInsert.getVertexCql();
                List<String> edgeCql = neo4jInsert.getEdgeCql();
                List<String> idIndex = neo4jInsert.getIdIndex();
                List<String> vertexId = new ArrayList<>();
                // 创建节点
                if (StringUtils.isNotBlank(vertexCql)) {
                    CompletionStage<ResultCursor> resultCursorCompletionStage = transaction.runAsync(vertexCql);
                    ResultCursor resultCursor = resultCursorCompletionStage.toCompletableFuture().get();
                    List<Record> records = resultCursor.listAsync().toCompletableFuture().get();
                    for (Record next : records) {
                        Collection<Object> values = next.asMap().values();
                        for (Object value : values) {
                            String identity = String.valueOf(((InternalNode) value).id());
                            vertexId.add(identity);
                        }
                    }
                }
                // 创建关系
                List<String> edgeId = new ArrayList<>();
                for (String edge : edgeCql) {
                    ResultCursor cursor = transaction.runAsync(edge).toCompletableFuture().get();
                    List<Record> list = cursor.listAsync().toCompletableFuture().get();
                    for (Record next : list) {
                        Collection<Object> values = next.asMap().values();
                        for (Object value : values) {
                            String identity = String.valueOf(((InternalRelationship) value).id());
                            edgeId.add(identity);
                        }
                    }
                }

                // 组织返回的自增ID，与入参List顺序一致
                List<Map<String, Object>> identityResult = getIdentities(idIndex, vertexId, edgeId);
                return identityResult;
            }

            @Override
            public List<Map<String, Object>> doWithSession(Session session) throws Exception {
                String vertexCql = neo4jInsert.getVertexCql();
                List<String> edgeCql = neo4jInsert.getEdgeCql();
                List<String> idIndex = neo4jInsert.getIdIndex();
                List<String> vertexId = new ArrayList<>();
                // 创建节点
                if (StringUtils.isNotBlank(vertexCql)) {
                    Result result = session.run(vertexCql);
                    while (result.hasNext()) {
                        Record next = result.next();
                        Collection<Object> values = next.asMap().values();
                        for (Object value : values) {
                            String identity = String.valueOf(((InternalNode) value).id());
                            vertexId.add(identity);
                        }
                    }
                }
                // 创建关系
                List<String> edgeId = new ArrayList<>();
                for (String edge : edgeCql) {
                    Result run = session.run(edge);
                    while (run.hasNext()) {
                        Record next = run.next();
                        Collection<Object> values = next.asMap().values();
                        for (Object value : values) {
                            String identity = String.valueOf(((InternalRelationship) value).id());
                            edgeId.add(identity);
                        }
                    }
                }

                // 组织返回的自增ID，与入参List顺序一致
                List<Map<String, Object>> identityResult = getIdentities(idIndex, vertexId, edgeId);
                return identityResult;
            }

            @Override
            public List<Map<String, Object>> doAsyncWithSession(AsyncSession session) throws Exception {
                String vertexCql = neo4jInsert.getVertexCql();
                List<String> edgeCql = neo4jInsert.getEdgeCql();
                List<String> idIndex = neo4jInsert.getIdIndex();
                List<String> vertexId = new ArrayList<>();
                // 创建节点
                if (StringUtils.isNotBlank(vertexCql)) {
                    CompletionStage<ResultCursor> resultCursorCompletionStage = session.runAsync(vertexCql);
                    ResultCursor resultCursor = resultCursorCompletionStage.toCompletableFuture().get();
                    List<Record> records = resultCursor.listAsync().toCompletableFuture().get();
                    for (Record next : records) {
                        Collection<Object> values = next.asMap().values();
                        for (Object value : values) {
                            String identity = String.valueOf(((InternalNode) value).id());
                            vertexId.add(identity);
                        }
                    }
                }
                // 创建关系
                List<String> edgeId = new ArrayList<>();
                for (String edge : edgeCql) {
                    ResultCursor cursor = session.runAsync(edge).toCompletableFuture().get();
                    List<Record> list = cursor.listAsync().toCompletableFuture().get();
                    for (Record next : list) {
                        Collection<Object> values = next.asMap().values();
                        for (Object value : values) {
                            String identity = String.valueOf(((InternalRelationship) value).id());
                            edgeId.add(identity);
                        }
                    }
                }

                // 组织返回的自增ID，与入参List顺序一致
                List<Map<String, Object>> identityResult = getIdentities(idIndex, vertexId, edgeId);
                return identityResult;
            }
        }, true);
        return new QueryMethodResult(executeSync.size(), executeSync);
    }

    @Override
    protected QueryMethodResult updateMethod(Driver connect, Neo4jHandler neo4jHandler, Neo4jDatabaseInfo databaseConf, Neo4jTableInfo tableConf) throws Exception {
        QueryMethodResult queryMethodResult = new QueryMethodResult();
        Neo4jHandler.Neo4jUpdate neo4jUpdate = neo4jHandler.getNeo4jUpdate();
        queryMethodResult.setRows(runToMap(connect, databaseConf.getDatabase(), neo4jUpdate.getCql()));
        return queryMethodResult;
    }

    @Override
    protected QueryMethodResult delMethod(Driver connect, Neo4jHandler neo4jHandler, Neo4jDatabaseInfo databaseConf, Neo4jTableInfo tableConf) throws Exception {
        QueryMethodResult queryMethodResult = new QueryMethodResult();
        Neo4jHandler.Neo4jDelete neo4jDelete = neo4jHandler.getNeo4jDelete();
        queryMethodResult.setRows(runToMap(connect, databaseConf.getDatabase(), neo4jDelete.getCql()));
        return queryMethodResult;
    }

    @Override
    protected QueryMethodResult monitorStatusMethod(Driver connect, Neo4jDatabaseInfo databaseConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "Neo4jServiceImpl.monitorStatusMethod");
    }

    @Override
    protected QueryMethodResult querySchemaMethod(Driver connect, Neo4jDatabaseInfo databaseConf, Neo4jTableInfo tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "Neo4jServiceImpl.querySchemaMethod");
    }

    @Override
    protected QueryMethodResult queryDatabaseSchemaMethod(Driver connect, Neo4jDatabaseInfo databaseConf, Neo4jTableInfo tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "Neo4jServiceImpl.queryDatabaseSchemaMethod");
    }

    @Override
    protected QueryCursorMethodResult queryCursorMethod(Driver connect, Neo4jHandler neo4jHandler, Neo4jDatabaseInfo databaseConf, Neo4jTableInfo tableConf, AbstractCursorCache cursorCache, Consumer<Map<String, Object>> consumer, Boolean rollAllData) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "Neo4jServiceImpl.queryCursorMethod");
    }

    @Override
    protected QueryMethodResult saveOrUpdateMethod(Driver connect, Neo4jHandler neo4jHandler, Neo4jDatabaseInfo databaseConf, Neo4jTableInfo tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "Neo4jServiceImpl.saveOrUpdateMethod");
    }

    @Override
    protected QueryMethodResult saveOrUpdateBatchMethod(Driver connect, Neo4jHandler queryEntity, Neo4jDatabaseInfo databaseConf, Neo4jTableInfo tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "Neo4jServiceImpl.saveOrUpdateBatchMethod");
    }

    @Override
    protected QueryMethodResult alterTableMethod(Driver connect, Neo4jHandler queryEntity, Neo4jDatabaseInfo databaseConf, Neo4jTableInfo tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "Neo4jServiceImpl.alterTableMethod");
    }

    @Override
    protected QueryMethodResult tableExistsMethod(Driver connect, Neo4jDatabaseInfo databaseConf, Neo4jTableInfo tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "Neo4jServiceImpl.tableExistsMethod");
    }

    @Override
    protected QueryMethodResult queryTableInformationMethod(Driver connect, Neo4jDatabaseInfo databaseConf, Neo4jTableInfo tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + ".queryTableInformationMethod");
    }

    @Override
    protected QueryMethodResult queryListDatabaseMethod(Driver connect, Neo4jHandler queryEntity, Neo4jDatabaseInfo databaseConf, Neo4jTableInfo tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + ".queryListDatabaseMethod");
    }

    @Override
    protected QueryMethodResult queryDatabaseInformationMethod(Driver connect, Neo4jHandler queryEntity, Neo4jDatabaseInfo databaseConf, Neo4jTableInfo tableConf) {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + ".queryDatabaseInformationMethod");
    }

    /**
     * 同步执行
     *
     * @param connect
     * @param cql
     * @param database
     * @return
     * @throws Exception
     */
    private List<Record> run(Driver connect, String database, String cql) throws Exception {
        return executeSync(connect, database, new Neo4jExecuteBasicImpl(cql), true);
    }

    private List<Map<String, Object>> runToMap(Driver driver, String database, String cql) throws Exception {
        List<Map<String, Object>> list = new ArrayList<>();
        List<Record> statementResult = run(driver, database, cql);
        for (Record record : statementResult) {
            List<Value> values = record.values();
            for (Value value : values) {
                Map<String, Object> row = new HashMap<>();
                if (value instanceof NodeValue) {
                    NodeValue nodeValue = (NodeValue) value;
                    InternalNode node = (InternalNode) nodeValue.asNode();
                    Collection<String> labels = node.labels();
                    long id = node.id();
                    row.put("id", String.valueOf(id));
                    if (CollectionUtil.isNotEmpty(labels)) {
                        if (labels.size() > 1) {
                            StringBuilder sb = new StringBuilder();
                            Iterator<String> iterator = labels.iterator();
                            while (iterator.hasNext()) {
                                String next = iterator.next();
                                sb.append(next).append(",");
                            }
                            sb = sb.delete(sb.length() - 1, sb.length());
                            row.put("label", sb.toString());
                        } else {
                            row.put("label", labels.iterator().next());
                        }
                    }
                    Map<String, Object> properties = node.asMap();
                    if (MapUtil.isNotEmpty(properties)) {
                        row.putAll(properties);
                    }
                }
                list.add(row);
            }
        }
        return list;
    }

    @Override
    protected GraphResultSet gremlinMethod(Driver graph, Neo4jHandler queryEntity, Neo4jDatabaseInfo database, Neo4jTableInfo table) throws Exception {
        Neo4jHandler.Noe4jGremlinQuery gremlinQuery = queryEntity.getGremlinQuery();
        if (log.isDebugEnabled()) {
            log.debug("cypher query [{}]", gremlinQuery.getCql());
        }
        GraphResultSet graphResults = executeSync(graph, database.getDatabase(), new Neo4jExecuteGremlinImpl(gremlinQuery), false);
        return graphResults;
    }

    @Override
    protected Future<GraphResultSet> gremlinAsyncMethod(Driver graph, Neo4jHandler queryEntity, Neo4jDatabaseInfo database, Neo4jTableInfo table) throws Exception {
        Neo4jHandler.Noe4jGremlinQuery gremlinQuery = queryEntity.getGremlinQuery();
        if (log.isDebugEnabled()) {
            log.debug("cypher query [{}]", gremlinQuery.getCql());
        }
        Future<GraphResultSet> future = executeAsync(graph, database.getDatabase(), new Neo4jExecuteGremlinImpl(gremlinQuery), false);
        return future;
    }

    @Override
    protected QlResult qlMethod(Driver graph, Neo4jHandler queryEntity, Neo4jDatabaseInfo database, Neo4jTableInfo table) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + ".qlMethod");
    }

    /**
     * 异步执行
     *
     * @param connect
     * @param callback
     * @param <SYNC_RESULT>
     * @param <ASYNC_RESULT>
     * @return
     * @throws Exception
     */
    private <SYNC_RESULT, ASYNC_RESULT> ASYNC_RESULT executeAsync(Driver connect, String database, Neo4jExecuteCallback<SYNC_RESULT, ASYNC_RESULT> callback, boolean closeSession) throws Exception {
        return execute(connect, database, callback, true, closeSession).getAsyncResult();
    }

    /**
     * 同步执行
     *
     * @param connect
     * @param callback
     * @param <SYNC_RESULT>
     * @param <ASYNC_RESULT>
     * @param database
     * @return
     * @throws Exception
     */
    private <SYNC_RESULT, ASYNC_RESULT> SYNC_RESULT executeSync(Driver connect, String database, Neo4jExecuteCallback<SYNC_RESULT, ASYNC_RESULT> callback, boolean closeSession) throws Exception {
        return execute(connect, database, callback, false, closeSession).getSyncResult();
    }

    /**
     * 执行cypher语句
     *
     * @param connect  连接信息
     * @param callback
     */
    private <SYNC_RESULT, ASYNC_RESULT> ExecuteResult<SYNC_RESULT, ASYNC_RESULT> execute(Driver connect, String database, Neo4jExecuteCallback<SYNC_RESULT, ASYNC_RESULT> callback, boolean async, boolean closeSession) throws Exception {
        // 1.判断 ThreadLocal 中是否存在 TransactionId
        String transactionId = (String) ThreadLocalUtil.get(TransactionConstant.ID);
        if (StringUtils.isNotBlank(transactionId)) {
            // 开启事务
            //2.判断时候第一次开启事务
            Neo4jTransaction neo4jTransaction;
            String first = (String) ThreadLocalUtil.get(TransactionConstant.FIRST);
            Neo4jTransactionManager transactionManager;
            if (StringUtils.isBlank(first)) {
                // 3.不是第一次则判断 this.getHelper().getTransactionManager(TransactionId) 是否获取得到TransactionManager
                transactionManager = (Neo4jTransactionManager) this.getHelper().getTransactionManager(transactionId);
                if (Objects.isNull(transactionManager)) {
                    throw new BusinessException(ExceptionCode.TRANSACTION_NULL_EXCEPTION);
                }
                // 4.若 从第三步中获取到 TransactionManager，则调用 TransactionManager.getTransactionClient 获取 connection
                neo4jTransaction = transactionManager.getTransactionClient();
            } else {
                // 5.是第一次则第一次创建事务对象
                transactionManager = (Neo4jTransactionManager) this.getHelper().getTransactionManager(transactionId);
                if (!Objects.isNull(transactionManager)) {
                    throw new BusinessException(ExceptionCode.TRANSACTION_ID_EXIST_EXCEPTION);
                }
                transactionManager = new Neo4jTransactionManager();
                Neo4jSession neo4jSession;
                if (async) {
                    AsyncSession session;
                    if (StringUtils.isNotBlank(database)) {
                        session = connect.asyncSession(SessionConfig.forDatabase(database));
                    } else {
                        session = connect.asyncSession();
                    }
                    CompletionStage<AsyncTransaction> completionStage = session.beginTransactionAsync();
                    AsyncTransaction asyncTransaction = completionStage.toCompletableFuture().get();
                    neo4jTransaction = new Neo4jTransaction(asyncTransaction);
                    transactionManager.setTransaction(neo4jTransaction);
                    neo4jSession = new Neo4jSession(session);
                    transactionManager.setSession(neo4jSession);
                } else {
                    Session session;
                    if (StringUtils.isNotBlank(database)) {
                        session = connect.session(SessionConfig.forDatabase(database));
                    } else {
                        session = connect.session();
                    }
                    Transaction transaction = session.beginTransaction();
                    neo4jTransaction = new Neo4jTransaction(transaction);
                    transactionManager.setTransaction(neo4jTransaction);
                    neo4jSession = new Neo4jSession(session);
                    transactionManager.setSession(neo4jSession);
                }
                this.getHelper().createTransactionManager(transactionId, transactionManager);
                //6.删除第一次开始事务标志位
                ThreadLocalUtil.set(TransactionConstant.FIRST, null);
            }
            if (async) {
                ASYNC_RESULT asyncResult = callback.doAsyncWithTransaction(neo4jTransaction);
                return new ExecuteResult<>(null, asyncResult);
            } else {
                SYNC_RESULT syncResult = callback.doWithTransaction(neo4jTransaction);
                return new ExecuteResult<>(syncResult, null);
            }
        } else {
            // 不开启事务
            if (async) {
                AsyncSession asyncSession = null;
                try {
                    if (StringUtils.isNotBlank(database)) {
                        asyncSession = connect.asyncSession(SessionConfig.forDatabase(database));
                    } else {
                        asyncSession = connect.asyncSession();
                    }
                    ASYNC_RESULT asyncResult = callback.doAsyncWithSession(asyncSession);
                    return new ExecuteResult<>(null, asyncResult);
                } finally {
                    if (asyncSession != null && closeSession) {
                        asyncSession.closeAsync();
                    }
                }
            } else {
                Session session = null;
                try {
                    if (StringUtils.isNotBlank(database)) {
                        session = connect.session(SessionConfig.forDatabase(database));
                    } else {
                        session = connect.session();
                    }
                    SYNC_RESULT syncResult = callback.doWithSession(session);
                    return new ExecuteResult<>(syncResult, null);
                } finally {
                    if (session != null && closeSession) {
                        session.close();
                    }
                }
            }
        }
    }

    @Override
    public DataResult rawStatementExecute(DatabaseSetting databaseSetting, String rawStatement) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + ".rawStatementExecute");
    }

    /**
     * 执行结果
     *
     * @param <SYNC_RESULT>
     * @param <ASYNC_RESULT>
     */
    @Data
    private static class ExecuteResult<SYNC_RESULT, ASYNC_RESULT> {
        private SYNC_RESULT syncResult;
        private ASYNC_RESULT asyncResult;

        public ExecuteResult(SYNC_RESULT syncResult, ASYNC_RESULT asyncResult) {
            this.syncResult = syncResult;
            this.asyncResult = asyncResult;
        }
    }

    /**
     * Neo4J 检索原生实现
     */
    private static class Neo4jExecuteBasicImpl implements Neo4jExecuteCallback<List<Record>, List<Record>> {

        private String cql;

        public Neo4jExecuteBasicImpl(String cql) {
            this.cql = cql;
        }

        @Override
        public List<Record> doWithTransaction(Neo4jTransaction transaction) throws Exception {
            return transaction.run(cql).list();
        }

        @Override
        public List<Record> doAsyncWithTransaction(Neo4jTransaction transaction) throws Exception {
            return transaction.runAsync(cql).toCompletableFuture().get().listAsync().toCompletableFuture().get();
        }

        @Override
        public List<Record> doWithSession(Session session) throws Exception {
            Result run = session.run(cql);
            return run.list();
        }

        @Override
        public List<Record> doAsyncWithSession(AsyncSession session) throws Exception {
            return session.runAsync(cql).toCompletableFuture().get().listAsync().toCompletableFuture().get();
        }
    }

    /**
     * Neo4J 检索 Gremlin 包装实现
     */
    private static class Neo4jExecuteGremlinImpl implements Neo4jExecuteCallback<GraphResultSet, Future<GraphResultSet>> {

        private Neo4jHandler.Noe4jGremlinQuery gremlinQuery;

        public Neo4jExecuteGremlinImpl(Neo4jHandler.Noe4jGremlinQuery gremlinQuery) {
            this.gremlinQuery = gremlinQuery;
        }

        @Override
        public GraphResultSet doWithTransaction(Neo4jTransaction transaction) throws Exception {
            Result statementResult = transaction.run(gremlinQuery.getCql(), gremlinQuery.getParams());
            return new Neo4jResultSet(statementResult, null, gremlinQuery.getType());
        }

        @Override
        public Future<GraphResultSet> doAsyncWithTransaction(Neo4jTransaction transaction) throws Exception {
            CompletionStage<ResultCursor> stage = transaction.runAsync(gremlinQuery.getCql(), gremlinQuery.getParams());
            return stage.toCompletableFuture().thenApply(new Function<ResultCursor, GraphResultSet>() {
                @Override
                public GraphResultSet apply(ResultCursor statementResultCursor) {
                    return new Neo4jCursorResultSet(statementResultCursor, null, gremlinQuery.getType());
                }
            });
        }

        @Override
        public GraphResultSet doWithSession(Session session) throws Exception {
            Result statementResult = session.run(gremlinQuery.getCql(), gremlinQuery.getParams());
            return new Neo4jResultSet(statementResult, session, gremlinQuery.getType());
        }

        @Override
        public Future<GraphResultSet> doAsyncWithSession(AsyncSession session) throws Exception {
            CompletionStage<ResultCursor> statementResultCursorCompletionStage = session.runAsync(gremlinQuery.getCql(), gremlinQuery.getParams());
            return statementResultCursorCompletionStage.toCompletableFuture().thenApply(new Function<ResultCursor, GraphResultSet>() {
                @Override
                public GraphResultSet apply(ResultCursor statementResultCursor) {
                    return new Neo4jCursorResultSet(statementResultCursor, session, gremlinQuery.getType());
                }
            });
        }
    }
}
