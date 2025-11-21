package com.meiya.whalex.db.module.graph;

import cn.hutool.core.map.MapUtil;
import com.meiya.whalex.annotation.DbService;
import com.meiya.whalex.annotation.Support;
import com.meiya.whalex.db.constant.SupportPower;
import com.meiya.whalex.db.entity.*;
import com.meiya.whalex.db.entity.graph.NebulaGraphDatabaseInfo;
import com.meiya.whalex.db.entity.graph.NebulaGraphHandler;
import com.meiya.whalex.db.entity.graph.NebulaGraphQlResult;
import com.meiya.whalex.db.entity.graph.NebulaGraphTableInfo;
import com.meiya.whalex.exception.BusinessException;
import com.meiya.whalex.exception.ExceptionCode;
import com.meiya.whalex.graph.entity.GraphResultSet;
import com.meiya.whalex.graph.entity.QlResult;
import com.meiya.whalex.interior.db.constant.CloudVendorsEnum;
import com.meiya.whalex.interior.db.constant.DbResourceEnum;
import com.meiya.whalex.interior.db.constant.DbVersionEnum;
import com.vesoft.nebula.client.graph.SessionPool;
import com.vesoft.nebula.client.graph.data.ResultSet;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.Future;
import java.util.function.Consumer;

/**
 * @author 黄河森
 * @date 2024/3/7
 * @package com.meiya.whalex.db.module.graph
 * @project whalex-data-driver
 * @description NebulaGraphServiceImpl
 */
@Slf4j
@DbService(dbType = DbResourceEnum.nebulagraph, version = DbVersionEnum.NEBULAGRAPH_3_8_0, cloudVendors = CloudVendorsEnum.OPEN)
@Support(value = {
        SupportPower.TEST_CONNECTION
})
public class NebulaGraphServiceImpl extends NebulaGraphRawStatementModuleImpl<SessionPool, NebulaGraphHandler, NebulaGraphDatabaseInfo, NebulaGraphTableInfo, AbstractCursorCache>  {
    @Override
    protected GraphResultSet gremlinMethod(SessionPool graph, NebulaGraphHandler queryEntity, NebulaGraphDatabaseInfo database, NebulaGraphTableInfo table) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + ".gremlinMethod");
    }

    @Override
    protected Future<GraphResultSet> gremlinAsyncMethod(SessionPool graph, NebulaGraphHandler queryEntity, NebulaGraphDatabaseInfo database, NebulaGraphTableInfo table) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + ".gremlinAsyncMethod");
    }

    @Override
    protected QlResult qlMethod(SessionPool graph, NebulaGraphHandler queryEntity, NebulaGraphDatabaseInfo database, NebulaGraphTableInfo table) throws Exception {
        NebulaGraphHandler.GqlQuery gqlQuery = queryEntity.getGqlQuery();
        Map<String, Object> params = gqlQuery.getParams();
        ResultSet resultSet;
        if (MapUtil.isNotEmpty(params)) {
            resultSet = graph.execute(gqlQuery.getGql(), gqlQuery.getParams());
        } else {
            resultSet = graph.execute(gqlQuery.getGql());
        }
        return new NebulaGraphQlResult(resultSet);
    }

    @Override
    protected QueryMethodResult queryMethod(SessionPool connect, NebulaGraphHandler queryEntity, NebulaGraphDatabaseInfo databaseConf, NebulaGraphTableInfo tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + ".queryMethod");
    }

    @Override
    protected QueryMethodResult countMethod(SessionPool connect, NebulaGraphHandler queryEntity, NebulaGraphDatabaseInfo databaseConf, NebulaGraphTableInfo tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + ".countMethod");
    }

    @Override
    protected QueryMethodResult testConnectMethod(SessionPool connect, NebulaGraphDatabaseInfo databaseConf) throws Exception {
        boolean closed = connect.isClosed();
        if (closed) {
            throw new BusinessException(ExceptionCode.LINK_DATABASE_EXCEPTION, "链接对象已关闭");
        } else {
            return new QueryMethodResult();
        }
    }

    @Override
    protected QueryMethodResult showTablesMethod(SessionPool connect, NebulaGraphHandler queryEntity, NebulaGraphDatabaseInfo databaseConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + ".showTablesMethod");
    }

    @Override
    protected QueryMethodResult getIndexesMethod(SessionPool connect, NebulaGraphDatabaseInfo databaseConf, NebulaGraphTableInfo tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + ".getIndexesMethod");
    }

    @Override
    protected QueryMethodResult createTableMethod(SessionPool connect, NebulaGraphHandler queryEntity, NebulaGraphDatabaseInfo databaseConf, NebulaGraphTableInfo tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + ".createTableMethod");
    }

    @Override
    protected QueryMethodResult dropTableMethod(SessionPool connect, NebulaGraphHandler queryEntity, NebulaGraphDatabaseInfo databaseConf, NebulaGraphTableInfo tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + ".dropTableMethod");
    }

    @Override
    protected QueryMethodResult deleteIndexMethod(SessionPool connect, NebulaGraphHandler queryEntity, NebulaGraphDatabaseInfo databaseConf, NebulaGraphTableInfo tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + ".deleteIndexMethod");
    }

    @Override
    protected QueryMethodResult createIndexMethod(SessionPool connect, NebulaGraphHandler queryEntity, NebulaGraphDatabaseInfo databaseConf, NebulaGraphTableInfo tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + ".createIndexMethod");
    }

    @Override
    protected QueryMethodResult insertMethod(SessionPool connect, NebulaGraphHandler queryEntity, NebulaGraphDatabaseInfo databaseConf, NebulaGraphTableInfo tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + ".insertMethod");
    }

    @Override
    protected QueryMethodResult updateMethod(SessionPool connect, NebulaGraphHandler queryEntity, NebulaGraphDatabaseInfo databaseConf, NebulaGraphTableInfo tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + ".updateMethod");
    }

    @Override
    protected QueryMethodResult delMethod(SessionPool connect, NebulaGraphHandler queryEntity, NebulaGraphDatabaseInfo databaseConf, NebulaGraphTableInfo tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + ".showTablesMethod");
    }

    @Override
    protected QueryMethodResult monitorStatusMethod(SessionPool connect, NebulaGraphDatabaseInfo databaseConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + ".monitorStatusMethod");
    }

    @Override
    protected QueryMethodResult querySchemaMethod(SessionPool connect, NebulaGraphDatabaseInfo databaseConf, NebulaGraphTableInfo tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + ".querySchemaMethod");
    }

    @Override
    protected QueryMethodResult queryDatabaseSchemaMethod(SessionPool connect, NebulaGraphDatabaseInfo databaseConf, NebulaGraphTableInfo tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + ".queryDatabaseSchemaMethod");
    }

    @Override
    protected QueryCursorMethodResult queryCursorMethod(SessionPool connect, NebulaGraphHandler queryEntity, NebulaGraphDatabaseInfo databaseConf, NebulaGraphTableInfo tableConf, AbstractCursorCache cursorCache, Consumer<Map<String, Object>> consumer, Boolean rollAllData) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + ".queryCursorMethod");
    }

    @Override
    protected QueryMethodResult saveOrUpdateMethod(SessionPool connect, NebulaGraphHandler queryEntity, NebulaGraphDatabaseInfo databaseConf, NebulaGraphTableInfo tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + ".saveOrUpdateMethod");
    }

    @Override
    protected QueryMethodResult saveOrUpdateBatchMethod(SessionPool connect, NebulaGraphHandler queryEntity, NebulaGraphDatabaseInfo databaseConf, NebulaGraphTableInfo tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + ".saveOrUpdateBatchMethod");
    }

    @Override
    protected QueryMethodResult alterTableMethod(SessionPool connect, NebulaGraphHandler queryEntity, NebulaGraphDatabaseInfo databaseConf, NebulaGraphTableInfo tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + ".alterTableMethod");
    }

    @Override
    protected QueryMethodResult tableExistsMethod(SessionPool connect, NebulaGraphDatabaseInfo databaseConf, NebulaGraphTableInfo tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + ".tableExistsMethod");
    }

    @Override
    protected QueryMethodResult queryTableInformationMethod(SessionPool connect, NebulaGraphDatabaseInfo databaseConf, NebulaGraphTableInfo tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + ".queryTableInformationMethod");
    }

    @Override
    protected QueryMethodResult queryListDatabaseMethod(SessionPool connect, NebulaGraphHandler queryEntity, NebulaGraphDatabaseInfo databaseConf, NebulaGraphTableInfo tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + ".queryListDatabaseMethod");
    }

    @Override
    protected QueryMethodResult queryDatabaseInformationMethod(SessionPool connect, NebulaGraphHandler queryEntity, NebulaGraphDatabaseInfo databaseConf, NebulaGraphTableInfo tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + ".queryDatabaseInformationMethod");
    }
}
