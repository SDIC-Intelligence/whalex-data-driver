package com.meiya.whalex.db.module.graph;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.map.MapUtil;
import com.meiya.whalex.annotation.Support;
import com.meiya.whalex.db.constant.SupportPower;
import com.meiya.whalex.db.entity.*;
import com.meiya.whalex.db.entity.graph.GremlinDatabaseInfo;
import com.meiya.whalex.db.entity.graph.GremlinHandler;
import com.meiya.whalex.db.entity.graph.GremlinTableInfo;
import com.meiya.whalex.db.entity.graph.GremlinClient;
import com.meiya.whalex.exception.BusinessException;
import com.meiya.whalex.exception.ExceptionCode;
import com.meiya.whalex.graph.entity.GraphResult;
import com.meiya.whalex.graph.entity.GraphResultSet;
import com.meiya.whalex.graph.entity.QlResult;
import com.meiya.whalex.graph.module.AbstractGraphModuleBaseService;
import com.meiya.whalex.graph.rest.PathRsp;
import com.meiya.whalex.interior.db.builder.GraphQueryBuilder;
import com.meiya.whalex.util.JsonUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.gremlin.process.remote.traversal.DefaultRemoteTraverser;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.*;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Gremlin 图数据库实现
 *
 * @author 黄河森
 * @date 2019/12/16
 * @project whale-cloud-platformX
 */
@Slf4j
@Support(value = {
        SupportPower.TEST_CONNECTION,
        SupportPower.SEARCH,
        SupportPower.CREATE_INDEX,
        SupportPower.CREATE_TABLE,
        SupportPower.DROP_TABLE,
        SupportPower.QUERY_INDEX
})
public class BaseGremlinServiceImpl<S extends GremlinClient,
        Q extends GremlinHandler,
        D extends GremlinDatabaseInfo,
        T extends GremlinTableInfo,
        C extends AbstractCursorCache> extends AbstractGraphModuleBaseService<S, Q, D, T, C> {
    @Override
    protected QueryMethodResult queryMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        QueryMethodResult queryMethodResult = new QueryMethodResult();
        GremlinHandler.GremlinQuery gremlinQuery = queryEntity.getGremlinQuery();
        queryEntity.setQueryStr(gremlinQuery.getGremlin());
        List<Map<String, Object>> list = new ArrayList<>();

        Future<GraphResultSet> graphResultSetFuture = connect.submitAsync(gremlinQuery.getQueryTraversal());
        GraphResultSet resultSet = graphResultSetFuture.get();

        List<GraphResult> results = resultSet.stream().collect(Collectors.toList());
        String returnType = gremlinQuery.getReturnType();
        switch (returnType) {
            case GraphQueryBuilder
                    .VERTEX:
            case GraphQueryBuilder
                    .EDGE:
                Map<Object, Map<String, Object>> vertexPropertyMap = new HashMap<>();
                for (GraphResult result : results) {
                    Object object = ((DefaultRemoteTraverser) result.getObject()).get();
                    Property<Object> property = (Property<Object>) object;
                    Element element = property.element();
                    Map<String, Object> map = vertexPropertyMap.get(element.id());
                    if (map == null) {
                        map = new HashMap<>();
                        vertexPropertyMap.put(element.id(), map);
                        map.put("id", element.id());
                        map.put("label", element.label());
                    }
                    map.put(property.key(), property.value());
                }
                list.addAll(vertexPropertyMap.values());
                break;
            case GraphQueryBuilder
                    .PATH:
                List<PathRsp> paths = new ArrayList<>();
                for (GraphResult result : results) {
                    Object object = ((DefaultRemoteTraverser) result.getObject()).get();
                    Path path = (Path) object;
                    List<PathRsp.PathEdge> edgeList = new ArrayList<>();
                    List<PathRsp.PathVertex> vertexList = new ArrayList<>();
                    List<String> pathList = new ArrayList<>();
                    List<Object> objects = path.objects();
                    for (Object o : objects) {
                        if (o instanceof Vertex) {
                            Vertex vertex = (Vertex) o;
                            vertexList.add(
                                    PathRsp.PathVertex.builder().id(vertex.id().toString())
                                            .label(vertex.label())
                                            .build()
                            );
                        } else if (o instanceof Edge) {
                            Edge edge = (Edge) o;
                            edgeList.add(
                                    PathRsp.PathEdge.builder()
                                            .id(edge.id().toString())
                                            .label(edge.label())
                                            .start(edge.outVertex().id().toString())
                                            .end(edge.inVertex().id().toString())
                                    .build()
                            );
                            pathList.add(edge.id().toString());
                        }
                    }
                    PathRsp.PathVertex first = CollectionUtil.getFirst(vertexList);
                    PathRsp.PathVertex last = CollectionUtil.getLast(vertexList);
                    PathRsp currentPath = PathRsp.builder().start(first.getId())
                            .end(last.getId())
                            .edgeList(edgeList)
                            .vertexList(vertexList)
                            .pathList(pathList)
                            .build();
                    paths.add(currentPath);
                }
                List object = JsonUtil.jsonStrToObject(JsonUtil.objectToStr(paths), List.class);
                list.addAll(object);
                break;
        }
        queryMethodResult.setRows(list);
        return queryMethodResult;
    }


    @Override
    protected QueryMethodResult countMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        GremlinHandler.GremlinQuery gremlinQuery = queryEntity.getGremlinQuery();
        Future<GraphResultSet> graphResultSetFuture = connect.submitAsync(gremlinQuery.getCountTraversal());
        GraphResultSet resultSet = graphResultSetFuture.get();
        long total = resultSet.one().getLong();
        return new QueryMethodResult(true, total, null);
    }

    @Override
    protected QueryMethodResult testConnectMethod(S client, D databaseConf) throws Exception {
        boolean connect = client.connect();
        if (connect) {
            return new QueryMethodResult();
        }
        throw new BusinessException("connect Gremlin database fail, please try again.");
    }

    @Override
    protected QueryMethodResult showTablesMethod(S connect, Q queryEntity, D databaseConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "GremlinServiceImpl.showTablesMethod");
    }

    @Override
    protected QueryMethodResult getIndexesMethod(S connect, D databaseConf, T tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "GremlinServiceImpl.getIndexesMethod");
    }

    @Override
    protected QueryMethodResult createTableMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "GremlinServiceImpl.createTableMethod");
    }

    @Override
    protected QueryMethodResult dropTableMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "GremlinServiceImpl.dropTableMethod");
    }

    @Override
    protected QueryMethodResult deleteIndexMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "GremlinServiceImpl.deleteIndexMethod");
    }

    @Override
    protected QueryMethodResult createIndexMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "GremlinServiceImpl.createIndexMethod");
    }

    @Override
    protected QueryMethodResult insertMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "GremlinServiceImpl.insertMethod");
    }

    @Override
    protected QueryMethodResult updateMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        GremlinHandler.GremlinUpdate gremlinUpdate = queryEntity.getGremlinUpdate();
        GraphTraversal traversal = gremlinUpdate.getTraversal();

        Future<GraphResultSet> graphResultSetFuture = connect.submitAsync(traversal);
        GraphResultSet resultSet = graphResultSetFuture.get();
        CompletableFuture<List<GraphResult>> all = resultSet.all();
        List<GraphResult> results = all.get();
        return new QueryMethodResult(results.size(), null);
    }

    @Override
    protected QueryMethodResult delMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        GremlinHandler.GremlinDelete gremlinDelete = queryEntity.getGremlinDelete();
        GraphTraversal traversal = gremlinDelete.getTraversal();

        Future<GraphResultSet> graphResultSetFuture = connect.submitAsync(traversal);
        GraphResultSet resultSet = graphResultSetFuture.get();
        CompletableFuture<List<GraphResult>> all = resultSet.all();
        List<GraphResult> results = all.get();
        return new QueryMethodResult(gremlinDelete.getTotal(), null);
    }

    @Override
    protected QueryMethodResult monitorStatusMethod(S connect, D databaseConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "GremlinServiceImpl.monitorStatusMethod");
    }

    @Override
    protected QueryMethodResult querySchemaMethod(S connect, D databaseConf, T tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "GremlinServiceImpl.querySchemaMethod");
    }

    @Override
    protected QueryMethodResult queryDatabaseSchemaMethod(S connect, D databaseConf, T tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "GremlinServiceImpl.queryDatabaseSchemaMethod");
    }


    @Override
    protected QueryCursorMethodResult queryCursorMethod(S connect, Q queryEntity, D databaseConf, T tableConf, C cursorCache, Consumer<Map<String, Object>> consumer, Boolean rollAllData) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "GremlinServiceImpl.queryCursorMethod");
    }

    @Override
    protected QueryMethodResult saveOrUpdateMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "GremlinServiceImpl.saveOrUpdateMethod");
    }

    @Override
    protected QueryMethodResult saveOrUpdateBatchMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "GremlinServiceImpl.saveOrUpdateBatchMethod");
    }

    @Override
    protected QueryMethodResult alterTableMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "GremlinServiceImpl.alterTableMethod");
    }

    @Override
    protected QueryMethodResult tableExistsMethod(S connect, D databaseConf, T tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "GremlinServiceImpl.tableExistsMethod");
    }

    @Override
    protected QueryMethodResult queryTableInformationMethod(S connect, D databaseConf, T tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + ".queryTableInformationMethod");
    }

    @Override
    protected QueryMethodResult queryListDatabaseMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + ".queryListDatabaseMethod");
    }

    @Override
    protected QueryMethodResult queryDatabaseInformationMethod(S connect, Q queryEntity, D databaseConf, T tableConf) {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + ".queryDatabaseInformationMethod");
    }


    @Override
    public GraphResultSet gremlinMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        GremlinHandler.GremlinQuery gremlinQuery = queryEntity.getGremlinQuery();
        String gremlin = gremlinQuery.getGremlin();
        Map<String, Object> params = gremlinQuery.getParams();
        Traversal queryTraversal = gremlinQuery.getQueryTraversal();
        queryEntity.setQueryStr(gremlin);
        if (StringUtils.isNotBlank(gremlin)) {
            if (MapUtil.isNotEmpty(params)) {
                return connect.submit(gremlin, params);
            } else {
                return connect.submit(gremlin);
            }
        } else {
            return connect.submit(queryTraversal);
        }
    }

    @Override
    public Future<GraphResultSet> gremlinAsyncMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        GremlinHandler.GremlinQuery gremlinQuery = queryEntity.getGremlinQuery();
        String gremlin = gremlinQuery.getGremlin();
        Map<String, Object> params = gremlinQuery.getParams();
        Traversal queryTraversal = gremlinQuery.getQueryTraversal();
        queryEntity.setQueryStr(gremlin);
        if (StringUtils.isNotBlank(gremlin)) {
            if (MapUtil.isNotEmpty(params)) {
                return connect.submitAsync(gremlin, params);
            } else {
                return connect.submitAsync(gremlin);
            }
        } else {
            return connect.submitAsync(queryTraversal);
        }

    }

    @Override
    protected QlResult qlMethod(S graph, Q queryEntity, D database, T table) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + ".qlMethod");
    }

    @Override
    public DataResult rawStatementExecute(DatabaseSetting databaseSetting, String rawStatement) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + ".rawStatementExecute");
    }
}
