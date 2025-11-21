package com.meiya.whalex.graph.module;

import com.meiya.whalex.db.constant.DbMethodEnum;
import com.meiya.whalex.db.entity.*;
import com.meiya.whalex.db.module.AbstractDbRawStatementModule;
import com.meiya.whalex.graph.entity.*;
import com.meiya.whalex.graph.util.helper.AbstractGraphParserUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;

import java.util.Map;
import java.util.concurrent.Future;

/**
 * 图数据库基础抽象服务类
 *
 * @author 黄河森
 * @date 2022/12/27
 * @package com.meiya.whalex.graph.module
 * @project whalex-data-driver
 */
@Slf4j
public abstract class AbstractGraphModuleBaseService <S,
        Q extends AbstractDbHandler,
        D extends AbstractDatabaseInfo,
        T extends AbstractDbTableInfo,
        C extends AbstractCursorCache> extends AbstractDbRawStatementModule<S, Q, D, T, C> implements GraphModuleService, QlGraphModuleService {


    public void setDbModuleParamUtil(AbstractGraphParserUtil dbModuleParamUtil) {
        super.setDbModuleParamUtil(dbModuleParamUtil);
    }

    @Override
    public AbstractGraphParserUtil getDbModuleParamUtil() {
        return (AbstractGraphParserUtil) super.getDbModuleParamUtil();
    }

    @Override
    public GraphResultSet submitGremlin(DatabaseSetting databaseSetting, TableSetting tableSetting, String gremlin) throws Exception {
        return submitGremlin(databaseSetting, tableSetting, gremlin, null);
    }

    @Override
    public Future<GraphResultSet> submitAsyncGremlin(DatabaseSetting databaseSetting, TableSetting tableSetting, String gremlin) throws Exception {
        return submitAsyncGremlin(databaseSetting, tableSetting, gremlin, null);
    }

    @Override
    public GraphResultSet submitGremlin(DatabaseSetting databaseSetting, TableSetting tableSetting, String gremlin, Map<String, Object> params) throws Exception {
        GraphResultSet resultSet = interiorExecute(databaseSetting
                , tableSetting
                , GremlinQueryParams.builder().gremlin(gremlin).params(params).build()
                , DbMethodEnum.CUSTOMIZE_OPERATION_ON_TABLE
                , new DbModuleCallback<S, Q, D, T, GraphResultSet>() {
                    @Override
                    public GraphResultSet doWithExecute(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
                        return gremlinMethod(connect, queryEntity, databaseConf, tableConf);
                    }
                }, new DbModuleParserCallback<GremlinQueryParams, Q, D, T>() {
                    @Override
                    public Q parser(GremlinQueryParams queryParams, D database, T table) throws Exception {
                        return (Q) getDbModuleParamUtil().gremlinParser(queryParams.getGremlin(), queryParams.getParams(), database, table);
                    }
                });
        return resultSet;
    }

    @Override
    public Future<GraphResultSet> submitAsyncGremlin(DatabaseSetting databaseSetting, TableSetting tableSetting, String gremlin, Map<String, Object> params) throws Exception {
        Future<GraphResultSet> resultSet = interiorExecute(databaseSetting
                , tableSetting
                , GremlinQueryParams.builder().gremlin(gremlin).params(params).build()
                , DbMethodEnum.CUSTOMIZE_OPERATION_ON_TABLE
                , new DbModuleCallback<S, Q, D, T, Future<GraphResultSet>>() {
                    @Override
                    public Future<GraphResultSet> doWithExecute(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
                        return gremlinAsyncMethod(connect, queryEntity, databaseConf, tableConf);
                    }
                }, new DbModuleParserCallback<GremlinQueryParams, Q, D, T>() {
                    @Override
                    public Q parser(GremlinQueryParams queryParams, D database, T table) throws Exception {
                        return (Q) getDbModuleParamUtil().gremlinParser(queryParams.getGremlin(), queryParams.getParams(), database, table);
                    }
                });
        return resultSet;
    }

    @Override
    public GraphResultSet submitGremlin(DatabaseSetting databaseSetting, TableSetting tableSetting, Traversal traversal) throws Exception {
        GraphResultSet resultSet = interiorExecute(databaseSetting
                , tableSetting
                , TraversalQueryParams.builder().traversal(traversal).build()
                , DbMethodEnum.CUSTOMIZE_OPERATION_ON_TABLE
                , new DbModuleCallback<S, Q, D, T, GraphResultSet>() {
                    @Override
                    public GraphResultSet doWithExecute(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
                        return gremlinMethod(connect, queryEntity, databaseConf, tableConf);
                    }
                }, new DbModuleParserCallback<TraversalQueryParams, Q, D, T>() {
                    @Override
                    public Q parser(TraversalQueryParams traversalQueryParams, D database, T table) throws Exception {
                        return (Q) getDbModuleParamUtil().traversalParser(traversalQueryParams.getTraversal(), database, table);
                    }
                });
        return resultSet;
    }

    @Override
    public Future<GraphResultSet> submitAsyncGremlin(DatabaseSetting databaseSetting, TableSetting tableSetting, Traversal traversal) throws Exception {
        Future<GraphResultSet> resultSet = interiorExecute(databaseSetting
                , tableSetting
                , TraversalQueryParams.builder().traversal(traversal).build()
                , DbMethodEnum.CUSTOMIZE_OPERATION_ON_TABLE
                , new DbModuleCallback<S, Q, D, T, Future<GraphResultSet>>() {
                    @Override
                    public Future<GraphResultSet> doWithExecute(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
                        return gremlinAsyncMethod(connect, queryEntity, databaseConf, tableConf);
                    }
                }, new DbModuleParserCallback<TraversalQueryParams, Q, D, T>() {
                    @Override
                    public Q parser(TraversalQueryParams traversalQueryParams, D database, T table) throws Exception {
                        return (Q) getDbModuleParamUtil().traversalParser(traversalQueryParams.getTraversal(), database, table);
                    }
                });
        return resultSet;
    }

    @Override
    public QlResult run(DatabaseSetting databaseSetting, TableSetting tableSetting, String ql) throws Exception {
        QlResult result = interiorExecute(databaseSetting
                , tableSetting
                , QlQueryParams.builder().ql(ql).build()
                , DbMethodEnum.CUSTOMIZE_OPERATION_ON_TABLE
                , new DbModuleCallback<S, Q, D, T, QlResult>() {
                    @Override
                    public QlResult doWithExecute(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
                        return qlMethod(connect, queryEntity, databaseConf, tableConf);
                    }
                }, new DbModuleParserCallback<QlQueryParams, Q, D, T>() {
                    @Override
                    public Q parser(QlQueryParams queryParams, D database, T table) throws Exception {
                        return (Q) getDbModuleParamUtil().qlParser(queryParams.getQl(), queryParams.getParams(), database, table);
                    }
                });
        return result;
    }

    @Override
    public QlResult run(DatabaseSetting databaseSetting, TableSetting tableSetting, String ql, Map<String, Object> parameterMap) throws Exception {
        return null;
    }

    /**
     * Gremlin查询方法
     *
     * @param graph
     * @param queryEntity
     * @param database
     * @param table
     * @return
     * @throws Exception
     */
    protected abstract GraphResultSet gremlinMethod(S graph, Q queryEntity, D database, T table) throws Exception;

    /**
     * 异步Gremlin查询方法
     *
     * @param graph
     * @param queryEntity
     * @param database
     * @param table
     * @return
     * @throws Exception
     */
    protected abstract Future<GraphResultSet> gremlinAsyncMethod(S graph, Q queryEntity, D database, T table) throws Exception;

    /**
     * QL(CQL/GQL)查询方法
     *
     * @param graph
     * @param queryEntity
     * @param database
     * @param table
     * @return
     * @throws Exception
     */
    protected abstract QlResult qlMethod(S graph, Q queryEntity, D database, T table) throws Exception;
}
