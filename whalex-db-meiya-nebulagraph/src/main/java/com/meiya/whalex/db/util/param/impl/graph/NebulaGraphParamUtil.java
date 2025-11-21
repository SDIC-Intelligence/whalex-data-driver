package com.meiya.whalex.db.util.param.impl.graph;

import com.meiya.whalex.annotation.DbParamUtil;
import com.meiya.whalex.db.entity.*;
import com.meiya.whalex.db.entity.graph.NebulaGraphDatabaseInfo;
import com.meiya.whalex.db.entity.graph.NebulaGraphHandler;
import com.meiya.whalex.db.entity.graph.NebulaGraphTableInfo;
import com.meiya.whalex.graph.util.helper.AbstractGraphParserUtil;
import com.meiya.whalex.interior.db.constant.CloudVendorsEnum;
import com.meiya.whalex.interior.db.constant.CommonConstant;
import com.meiya.whalex.interior.db.constant.DbResourceEnum;
import com.meiya.whalex.interior.db.constant.DbVersionEnum;
import com.meiya.whalex.interior.db.operation.in.AlterTableParamCondition;
import com.meiya.whalex.interior.db.operation.in.CreateTableParamCondition;
import com.meiya.whalex.interior.db.operation.in.QueryDatabasesCondition;
import com.meiya.whalex.interior.db.operation.in.QueryTablesCondition;
import com.meiya.whalex.interior.db.search.in.QueryParamCondition;
import lombok.extern.slf4j.Slf4j;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;

import java.util.Map;

/**
 * @author 黄河森
 * @date 2024/3/7
 * @package com.meiya.whalex.db.util.param.impl.graph
 * @project whalex-data-driver
 * @description NebulaGraphParamUtil
 */
@Slf4j
@DbParamUtil(dbType = DbResourceEnum.nebulagraph, version = DbVersionEnum.NEBULAGRAPH_3_8_0, cloudVendors = CloudVendorsEnum.OPEN)
public class NebulaGraphParamUtil extends AbstractGraphParserUtil<NebulaGraphHandler, NebulaGraphDatabaseInfo, NebulaGraphTableInfo> implements CommonConstant {
    @Override
    public NebulaGraphHandler gremlinParser(String gremlin, Map<String, Object> params, NebulaGraphDatabaseInfo databaseConf, NebulaGraphTableInfo tableConf) throws Exception {
        return null;
    }

    @Override
    public NebulaGraphHandler traversalParser(Traversal traversal, NebulaGraphDatabaseInfo databaseConf, NebulaGraphTableInfo tableConf) throws Exception {
        return null;
    }

    @Override
    public NebulaGraphHandler qlParser(String ql, Map<String, Object> params, NebulaGraphDatabaseInfo databaseConf, NebulaGraphTableInfo tableConf) throws Exception {
        NebulaGraphHandler handler = new NebulaGraphHandler();
        NebulaGraphHandler.GqlQuery query = new NebulaGraphHandler.GqlQuery();
        handler.setGqlQuery(query);
        query.setGql(ql);
        query.setParams(params);
        return handler;
    }

    @Override
    protected NebulaGraphHandler transitionListTableParam(QueryTablesCondition queryTablesCondition, NebulaGraphDatabaseInfo databaseConf) throws Exception {
        return null;
    }

    @Override
    protected NebulaGraphHandler transitionListDatabaseParam(QueryDatabasesCondition queryDatabasesCondition, NebulaGraphDatabaseInfo databaseConf) throws Exception {
        return null;
    }

    @Override
    protected NebulaGraphHandler transitionCreateTableParam(CreateTableParamCondition createTableParamCondition, NebulaGraphDatabaseInfo databaseConf, NebulaGraphTableInfo tableConf) throws Exception {
        return null;
    }

    @Override
    protected NebulaGraphHandler transitionAlterTableParam(AlterTableParamCondition alterTableParamCondition, NebulaGraphDatabaseInfo databaseConf, NebulaGraphTableInfo tableConf) throws Exception {
        return null;
    }

    @Override
    protected NebulaGraphHandler transitionCreateIndexParam(IndexParamCondition indexParamCondition, NebulaGraphDatabaseInfo databaseConf, NebulaGraphTableInfo tableConf) throws Exception {
        return null;
    }

    @Override
    protected NebulaGraphHandler transitionDropIndexParam(IndexParamCondition indexParamCondition, NebulaGraphDatabaseInfo databaseConf, NebulaGraphTableInfo tableConf) throws Exception {
        return null;
    }

    @Override
    protected NebulaGraphHandler transitionQueryParam(QueryParamCondition queryParamCondition, NebulaGraphDatabaseInfo databaseConf, NebulaGraphTableInfo tableConf) throws Exception {
        return null;
    }

    @Override
    protected NebulaGraphHandler transitionUpdateParam(UpdateParamCondition updateParamCondition, NebulaGraphDatabaseInfo databaseConf, NebulaGraphTableInfo tableConf) throws Exception {
        return null;
    }

    @Override
    protected NebulaGraphHandler transitionInsertParam(AddParamCondition addParamCondition, NebulaGraphDatabaseInfo databaseConf, NebulaGraphTableInfo tableConf) throws Exception {
        return null;
    }

    @Override
    protected NebulaGraphHandler transitionDeleteParam(DelParamCondition delParamCondition, NebulaGraphDatabaseInfo databaseConf, NebulaGraphTableInfo tableConf) throws Exception {
        return null;
    }

    @Override
    protected NebulaGraphHandler transitionDropTableParam(DropTableParamCondition dropTableParamCondition, NebulaGraphDatabaseInfo databaseConf, NebulaGraphTableInfo tableConf) throws Exception {
        return null;
    }

    @Override
    public NebulaGraphHandler transitionUpsertParam(UpsertParamCondition paramCondition, NebulaGraphDatabaseInfo databaseConf, NebulaGraphTableInfo tableConf) throws Exception {
        return null;
    }

    @Override
    public NebulaGraphHandler transitionUpsertParamBatch(UpsertParamBatchCondition paramCondition, NebulaGraphDatabaseInfo databaseConf, NebulaGraphTableInfo tableConf) throws Exception {
        return null;
    }
}
