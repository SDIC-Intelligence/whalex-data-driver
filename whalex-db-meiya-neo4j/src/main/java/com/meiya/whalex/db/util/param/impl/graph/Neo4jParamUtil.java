package com.meiya.whalex.db.util.param.impl.graph;

import cn.hutool.core.map.MapUtil;
import com.meiya.whalex.annotation.DbParamUtil;
import com.meiya.whalex.db.entity.*;
import com.meiya.whalex.db.entity.graph.Neo4jDatabaseInfo;
import com.meiya.whalex.db.entity.graph.Neo4jHandler;
import com.meiya.whalex.db.entity.graph.Neo4jTableInfo;
import com.meiya.whalex.exception.BusinessException;
import com.meiya.whalex.exception.ExceptionCode;
import com.meiya.whalex.graph.constant.OperationField;
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
import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.meiya.whalex.db.util.Cypher.*;

/**
 * @author chenjp
 * @date 2020/9/27
 */
@Slf4j
@DbParamUtil(dbType = DbResourceEnum.neo4j, version = DbVersionEnum.NEO4J_4_4_11, cloudVendors = CloudVendorsEnum.OPEN)
public class Neo4jParamUtil extends AbstractGraphParserUtil<Neo4jHandler, Neo4jDatabaseInfo, Neo4jTableInfo> implements CommonConstant {

    @Override
    protected Neo4jHandler transitionListTableParam(QueryTablesCondition queryTablesCondition, Neo4jDatabaseInfo databaseInfo) throws Exception {
        return new Neo4jHandler();
    }

    @Override
    protected Neo4jHandler transitionListDatabaseParam(QueryDatabasesCondition queryDatabasesCondition, Neo4jDatabaseInfo databaseInfo) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + ".queryListDatabaseMethod");
    }

    @Override
    protected Neo4jHandler transitionCreateTableParam(CreateTableParamCondition createTableParamCondition, Neo4jDatabaseInfo databaseInfo, Neo4jTableInfo tableInfo) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "Neo4jParamUtil.transitionCreateTableParam");
    }

    @Override
    protected Neo4jHandler transitionAlterTableParam(AlterTableParamCondition alterTableParamCondition, Neo4jDatabaseInfo databaseInfo, Neo4jTableInfo tableInfo) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "Neo4jParamUtil.transitionAlterTableParam");
    }

    @Override
    protected Neo4jHandler transitionCreateIndexParam(IndexParamCondition indexParamCondition, Neo4jDatabaseInfo databaseInfo, Neo4jTableInfo tableInfo) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "Neo4jParamUtil.transitionCreateIndexParam");
    }

    @Override
    protected Neo4jHandler transitionDropIndexParam(IndexParamCondition indexParamCondition, Neo4jDatabaseInfo databaseInfo, Neo4jTableInfo tableInfo) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "Neo4jParamUtil.transitionDropIndexParam");
    }

    @Override
    protected Neo4jHandler transitionQueryParam(QueryParamCondition queryParamCondition, Neo4jDatabaseInfo databaseInfo, Neo4jTableInfo tableInfo) throws Exception {
        Neo4jHandler neo4jHandler = new Neo4jHandler();
        Neo4jHandler.Neo4jQuery neo4jQuery = new Neo4jHandler.Neo4jQuery();
        neo4jHandler.setNeo4jQuery(neo4jQuery);
        neo4jQuery.setCql(Query.doQuery(queryParamCondition));
        return neo4jHandler;
    }

    @Override
    protected Neo4jHandler transitionUpdateParam(UpdateParamCondition updateParamCondition, Neo4jDatabaseInfo databaseInfo, Neo4jTableInfo tableInfo) throws Exception {
        Neo4jHandler neo4jHandler = new Neo4jHandler();
        Neo4jHandler.Neo4jUpdate neo4jUpdate = new Neo4jHandler.Neo4jUpdate();
        neo4jHandler.setNeo4jUpdate(neo4jUpdate);
        neo4jUpdate.setCql(Update.doUpdate(updateParamCondition));
        return neo4jHandler;
    }

    @Override
    protected Neo4jHandler transitionInsertParam(AddParamCondition addParamCondition, Neo4jDatabaseInfo databaseInfo, Neo4jTableInfo tableInfo) throws Exception {
        Neo4jHandler neo4jHandler = new Neo4jHandler();
        Neo4jHandler.Neo4jInsert neo4jInsert = new Neo4jHandler.Neo4jInsert();
        neo4jHandler.setNeo4jInsert(neo4jInsert);
        List<String> edges = new ArrayList<>();
        StringBuilder vertexSb = new StringBuilder();
        List<Map<String, Object>> fieldValueList = addParamCondition.getFieldValueList();
        List<String> idIndex = new ArrayList<>(fieldValueList.size());
        List<String> returnResult = new ArrayList<>();
        for (int i = 0; i < fieldValueList.size(); i++) {
            Map<String, Object> addMap = fieldValueList.get(i);
            // 标签
            String label = (String) addMap.remove(LABEL);

            // 若含有 outV 和 inV 则认为是边，否则认为是点
            if (addMap.containsKey(OperationField.IN_V) || addMap.containsKey(OperationField.OUT_V)) {
                if (!(addMap.containsKey(OperationField.IN_V) && addMap.containsKey(OperationField.OUT_V))) {
                    throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "创建边必须指定 inV 和 outV");
                }
                idIndex.add(OperationField.EDGE);
                StringBuilder edgeSb = new StringBuilder();
                // 入节点
                String inV = String.valueOf(addMap.remove(OperationField.IN_V));
                // 出节点
                String outV = String.valueOf(addMap.remove(OperationField.OUT_V));

                edgeSb.append("MATCH (a) WHERE id(a) = ").append(inV).append(" MATCH (b) WHERE id(b) = ")
                        .append(outV).append(" CREATE (b)-[rel:").append(label);

                if (addMap.size() != 0) {
                    edgeSb.append(" {");
                    for (Map.Entry<String, Object> entry : addMap.entrySet()) {
                        String key = entry.getKey();
                        Object value = entry.getValue();
                        edgeSb.append(key).append(":");
                        if (value instanceof String) {
                            edgeSb.append("\"").append(value).append("\",");
                        } else if (value instanceof List) {
                            edgeSb.append("[");
                            List list = (List) value;
                            for (Object o : list) {
                                if (o instanceof String) {
                                    edgeSb.append("\"").append(o).append("\"");
                                } else {
                                    edgeSb.append(o);
                                }
                            }
                            edgeSb.append("],");
                        } else {
                            edgeSb.append(value).append(",");
                        }
                    }
                    edgeSb.deleteCharAt(edgeSb.length() - 1);
                    edgeSb.append("}");
                }

                edgeSb.append("]->(a) RETURN rel");
                edges.add(edgeSb.toString());
            } else {
                idIndex.add(OperationField.VERTEX);
                vertexSb.append("(").append(label.toLowerCase() + i).append(":").append(label);
                returnResult.add(label.toLowerCase() + i);
                if (MapUtil.isNotEmpty(addMap)) {
                    vertexSb.append(" {");
                    for (Map.Entry<String, Object> entry : addMap.entrySet()) {
                        String key = entry.getKey();
                        Object value = entry.getValue();
                        vertexSb.append(key).append(":");
                        if (value instanceof String) {
                            vertexSb.append("\"").append(value).append("\",");
                        } else if (value instanceof List) {
                            vertexSb.append("[");
                            List list = (List) value;
                            for (Object o : list) {
                                if (o instanceof String) {
                                    vertexSb.append("\"").append(o).append("\"");
                                } else {
                                    vertexSb.append(o);
                                }
                            }
                            vertexSb.append("],");
                        } else {
                            vertexSb.append(value).append(",");
                        }
                    }
                    vertexSb.deleteCharAt(vertexSb.length() - 1);
                    vertexSb.append("}").append("),");
                }
            }
        }
        if (vertexSb.length() > 0) {
            vertexSb = vertexSb.deleteCharAt(vertexSb.length() - 1);
            vertexSb.append(" RETURN ");
            for (String as : returnResult) {
                vertexSb.append(as).append(",");
            }
            vertexSb.deleteCharAt(vertexSb.length() - 1);
            vertexSb.insert(0, "CREATE ");
        }
        neo4jInsert.setVertexCql(vertexSb.toString());
        neo4jInsert.setEdgeCql(edges);
        neo4jInsert.setIdIndex(idIndex);
        return neo4jHandler;
    }

    @Override
    protected Neo4jHandler transitionDeleteParam(DelParamCondition delParamCondition, Neo4jDatabaseInfo databaseInfo, Neo4jTableInfo tableInfo) throws Exception {
        Neo4jHandler neo4jHandler = new Neo4jHandler();
        Neo4jHandler.Neo4jDelete neo4jDelete = new Neo4jHandler.Neo4jDelete();
        neo4jHandler.setNeo4jDelete(neo4jDelete);
        neo4jDelete.setCql(Delete.doDelete(delParamCondition));
        return neo4jHandler;
    }

    @Override
    protected Neo4jHandler transitionDropTableParam(DropTableParamCondition dropTableParamCondition, Neo4jDatabaseInfo databaseInfo, Neo4jTableInfo tableInfo) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "Neo4jParamUtil.transitionDropTableParam");
    }

    @Override
    public Neo4jHandler transitionUpsertParam(UpsertParamCondition paramCondition, Neo4jDatabaseInfo databaseInfo, Neo4jTableInfo tableInfo) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "Neo4jParamUtil.transitionUpsertParam");
    }

    @Override
    public Neo4jHandler transitionUpsertParamBatch(UpsertParamBatchCondition paramCondition, Neo4jDatabaseInfo databaseInfo, Neo4jTableInfo tableInfo) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + "transitionUpsertParamBatch");
    }

    @Override
    public Neo4jHandler gremlinParser(String gremlin, Map<String, Object> params, Neo4jDatabaseInfo databaseConf, Neo4jTableInfo tableConf) throws Exception {
        if (MapUtil.isNotEmpty(params)) {
            for (Map.Entry<String, Object> entry : params.entrySet()) {
                Object value = entry.getValue();
                if (value != null && !(value instanceof String)) {
                    entry.setValue(value.toString());
                }
            }
            gremlin = StringUtils.replaceEach(gremlin, params.keySet().toArray(new String[params.keySet().size()]), params.values().toArray(new String[params.values().size()]));
        }
        Neo4jHandler.Noe4jGremlinQuery query = GremlinToCqlParser.parser(gremlin).toCQL();
        Neo4jHandler neo4jHandler = new Neo4jHandler();
        neo4jHandler.setGremlinQuery(query);
        return neo4jHandler;
    }

    @Override
    public Neo4jHandler traversalParser(Traversal traversal, Neo4jDatabaseInfo databaseConf, Neo4jTableInfo tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + "traversalParser");
    }

    @Override
    public Neo4jHandler qlParser(String ql, Map<String, Object> params, Neo4jDatabaseInfo databaseConf, Neo4jTableInfo tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + "qlParser");
    }
}
