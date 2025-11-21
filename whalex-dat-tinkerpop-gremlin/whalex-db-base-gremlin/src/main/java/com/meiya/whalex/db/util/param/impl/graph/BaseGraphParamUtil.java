package com.meiya.whalex.db.util.param.impl.graph;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.map.MapUtil;
import com.meiya.whalex.db.entity.*;
import com.meiya.whalex.db.entity.graph.GremlinDatabaseInfo;
import com.meiya.whalex.db.entity.graph.GremlinHandler;
import com.meiya.whalex.db.entity.graph.GremlinTableInfo;
import com.meiya.whalex.exception.BusinessException;
import com.meiya.whalex.exception.ExceptionCode;
import com.meiya.whalex.graph.constant.OperationField;
import com.meiya.whalex.graph.util.helper.AbstractGraphParserUtil;
import com.meiya.whalex.interior.db.builder.GraphQueryBuilder;
import com.meiya.whalex.interior.db.constant.*;
import com.meiya.whalex.interior.db.operation.in.AlterTableParamCondition;
import com.meiya.whalex.interior.db.operation.in.CreateTableParamCondition;
import com.meiya.whalex.interior.db.operation.in.QueryDatabasesCondition;
import com.meiya.whalex.interior.db.operation.in.QueryTablesCondition;
import com.meiya.whalex.interior.db.search.condition.Rel;
import com.meiya.whalex.interior.db.search.in.Page;
import com.meiya.whalex.interior.db.search.in.QueryParamCondition;
import com.meiya.whalex.interior.db.search.in.Where;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.TextP;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;

import java.util.*;
import java.util.stream.Stream;

/**
 * @author chenjp
 * @date 2021/2/23
 */
@Slf4j
public class BaseGraphParamUtil<Q extends GremlinHandler, D extends GremlinDatabaseInfo,
        T extends GremlinTableInfo> extends AbstractGraphParserUtil<Q, D, T> implements CommonConstant {

    @Override
    protected Q transitionListTableParam(QueryTablesCondition queryTablesCondition, D databaseInfo) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + ".transitionListTableParam");
    }

    @Override
    protected Q transitionListDatabaseParam(QueryDatabasesCondition queryDatabasesCondition, D databaseInfo) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + ".queryListDatabaseMethod");
    }

    @Override
    protected Q transitionCreateTableParam(CreateTableParamCondition createTableParamCondition, D databaseConf, T tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + ".transitionCreateTableParam");
    }

    @Override
    protected Q transitionAlterTableParam(AlterTableParamCondition alterTableParamCondition, D databaseConf, T tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + ".transitionAlterTableParam");
    }

    @Override
    protected Q transitionCreateIndexParam(IndexParamCondition indexParamCondition, D databaseConf, T tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + ".transitionCreateIndexParam");
    }

    @Override
    protected Q transitionDropIndexParam(IndexParamCondition indexParamCondition, D databaseConf, T tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + ".transitionCreateIndexParam");
    }

    @Override
    protected Q transitionQueryParam(QueryParamCondition queryParamCondition, D databaseInfo, T tableInfo) throws Exception {
        Graph graph = EmptyGraph.instance();
        GraphTraversalSource g = graph.traversal();
        GraphTraversal graphTraversal = null;
        List<String> select = queryParamCondition.getSelect();
        List<Where> wheres = queryParamCondition.getWhere();
        Page page = queryParamCondition.getPage();
        String returnType = GraphQueryBuilder.VERTEX;
        if (CollectionUtil.isNotEmpty(wheres)) {
            Where returnWhere = findType(wheres);
            boolean isPath = false;
            if (returnWhere != null) {
                String param = (String) returnWhere.getParam();
                switch (param) {
                    case GraphQueryBuilder.VERTEX:
                        graphTraversal = g.V();
                        break;
                    case GraphQueryBuilder.EDGE:
                        graphTraversal = g.E();
                        returnType = GraphQueryBuilder.EDGE;
                        break;
                    case GraphQueryBuilder.PATH:
                        isPath = true;
                        returnType = GraphQueryBuilder.PATH;
                        break;
                    default:
                        throw new BusinessException("图数据库无法解析当前 [" + GraphQueryBuilder.RETURN + "] 参数值!");
                }
            } else {
                graphTraversal = g.V();
            }
            // 是否路径查询
            if (isPath) {
                Map<String, Object> pathMap = getPathWhere(wheres);
                if (MapUtil.isEmpty(pathMap)) {
                    graphTraversal = g.V().bothE().otherV().path();
                } else {
                    Object startV = pathMap.get(GraphQueryBuilder.START_VERTEX);
                    Object endV = pathMap.get(GraphQueryBuilder.END_VERTEX);
                    Object edge = pathMap.get(GraphQueryBuilder.PATH_EDGE);
                    Integer layer = (Integer) pathMap.get(GraphQueryBuilder.LAYER);
                    String option = (String) pathMap.get(GraphQueryBuilder.OPTION);

                    // 路径查询必须包含 起点-边-终点
                    if (startV == null || endV == null || edge == null) {
                        throw new BusinessException("图数据库路径查询必须包含 起始节点、关系边、终止节点 三个条件!");
                    }

                    List<Where> startWhere = (List<Where>) startV;
                    List<Where> endWhere = (List<Where>) endV;
                    List<Where> edgeWhere = (List<Where>) edge;

                    graphTraversal = g.V();

                    Where vLabelWhere = getLabel(startWhere);
                    if (vLabelWhere != null) {
                        setLabel(graphTraversal, vLabelWhere);
                    }

                    parseFirstNode(graphTraversal, startWhere);

                    GraphTraversal edgeGraphTraversal = graphTraversal;
                    if (layer != null && layer > 1) {
                        edgeGraphTraversal = __.start();
                        graphTraversal.repeat(edgeGraphTraversal);
                    }

                    // 边配置
                    Where directionWhere = findDirection(edgeWhere);
                    if (directionWhere != null) {
                        String param = (String) directionWhere.getParam();
                        switch (param.toUpperCase()) {
                            case "OUT":
                                edgeGraphTraversal.outE();
                                break;
                            case "IN":
                                edgeGraphTraversal.inE();
                                break;
                            case "BOTH":
                                edgeGraphTraversal.bothE();
                                break;
                            default:
                                throw new BusinessException("图数据库边操作无法解析当前 [_pathDirection] 参数值!");
                        }
                    } else {
                        throw new BusinessException("图数据库路径查询必须指明关系方向!");
                    }
                    Where labelWhere = getLabel(edgeWhere);
                    if (labelWhere != null) {
                        setLabel(edgeGraphTraversal, labelWhere);
                    }
                    getFirstEdge(edgeWhere, edgeGraphTraversal);

                    if (layer != null && layer > 1) {
                        graphTraversal.times(layer);
                    }

                    // 配置终点节点
                    String param = (String) directionWhere.getParam();
                    switch (param.toUpperCase()) {
                        case "OUT":
                            graphTraversal.inV();
                            break;
                        case "IN":
                            graphTraversal.outV();
                            break;
                        case "BOTH":
                            graphTraversal.otherV();
                            break;
                        default:
                            throw new BusinessException("图数据库边操作无法解析当前 [_pathDirection] 参数值!");
                    }
                    Where endVLabelWhere = getLabel(endWhere);
                    if (endVLabelWhere != null) {
                        setLabel(graphTraversal, endVLabelWhere);
                    }
                    parseFirstNode(graphTraversal, endWhere);

                    if (layer != null && layer > 1) {
                        graphTraversal.emit();
                    }

                    graphTraversal.path();
                }
            } else {
                Where labelWhere = getLabel(wheres);
                if (labelWhere != null) {
                    setLabel(graphTraversal, labelWhere);
                }
                parseFirstNode(graphTraversal, wheres);
            }
        } else {
            graphTraversal = g.V();
        }
        GraphTraversal count = null;
        if (queryParamCondition.isCountFlag()) {
            count = graphTraversal.asAdmin().clone().count();
        }
        graphTraversal.skip(page.getOffset()).limit(page.getLimit());
        if (CollectionUtil.isNotEmpty(select)) {
            graphTraversal.properties(select.toArray(new String[select.size()]));
        } else if (!StringUtils.equalsIgnoreCase(returnType, GraphQueryBuilder.PATH)) {
            graphTraversal.properties();
        }
        GremlinHandler graphBaseHandler = new GremlinHandler();
        GremlinHandler.GremlinQuery gremlinQuery = new GremlinHandler.GremlinQuery();
        graphBaseHandler.setGremlinQuery(gremlinQuery);
        gremlinQuery.setQueryTraversal(graphTraversal);
        gremlinQuery.setCountTraversal(count);
        gremlinQuery.setReturnType(returnType);
        return (Q) graphBaseHandler;
    }

    private void setLabel(GraphTraversal graphTraversal, Where vLabelWhere) {
        Rel type = vLabelWhere.getType();
        Object param = vLabelWhere.getParam();
        if (Rel.IN.equals(type)) {
            List<String> labels = (List) param;
            graphTraversal.hasLabel(labels.remove(0), labels.toArray(new String[labels.size()]));
        } else if (Rel.EQ.equals(type)) {
            graphTraversal.hasLabel((String) param);
        } else {
            throw new BusinessException("图数据库 Label 查询条件无法支持除 [IN] [EQ] 之外的操作!");
        }
    }

    private Where findDirection(List<Where> whereList) {
        for (Where where : whereList) {
            Rel type = where.getType();
            if (Rel.AND.equals(type) || Rel.OR.equals(type)) {
                Where _where = findDirection(where.getParams());
                if (_where != null) {
                    return _where;
                }
            }
            String field = where.getField();
            if (StringUtils.equalsAnyIgnoreCase(field, GraphQueryBuilder.PATH_DIRECTION)) {
                return where;
            }
        }
        return null;
    }

    private void getFirstEdge(List<Where> edgeWhere, GraphTraversal edgeGraphTraversal) {
        for (Where where : edgeWhere) {
            Rel type = where.getType();
            if (Rel.AND.equals(type) || Rel.OR.equals(type)) {
                getEdge(where.getParams(), edgeGraphTraversal);
            } else {
                String field = where.getField();
                if (StringUtils.equalsAnyIgnoreCase(GraphQueryBuilder.LABEL, field)
                        || StringUtils.equalsAnyIgnoreCase(GraphQueryBuilder.PATH_DIRECTION, field)
                        || StringUtils.equalsAnyIgnoreCase(GraphQueryBuilder.LAYER, field)
                        || StringUtils.equalsAnyIgnoreCase(GraphQueryBuilder.OPTION, field)
                        || StringUtils.equalsAnyIgnoreCase(GraphQueryBuilder.RETURN, field)
                ) {
                    continue;
                }
                parseWhere(edgeGraphTraversal, where);
            }
        }
    }

    private void getEdge(List<Where> edgeWhere, GraphTraversal edgeGraphTraversal) {
        for (Where where : edgeWhere) {
            String field = where.getField();
            if (StringUtils.equalsAnyIgnoreCase(GraphQueryBuilder.LABEL, field)
                    || StringUtils.equalsAnyIgnoreCase(GraphQueryBuilder.PATH_DIRECTION, field)
                    || StringUtils.equalsAnyIgnoreCase(GraphQueryBuilder.LAYER, field)
                    || StringUtils.equalsAnyIgnoreCase(GraphQueryBuilder.OPTION, field)
                    || StringUtils.equalsAnyIgnoreCase(GraphQueryBuilder.RETURN, field)
            ) {
                continue;
            }
            parseWhere(edgeGraphTraversal, where);
        }
    }

    private Map<String, Object> getPathWhere(List<Where> wheres) {
        return wheres.stream().flatMap(
                            where -> {
                                Map<String, Object> map;
                                Rel type = where.getType();
                                if (Rel.AND.equals(type) || Rel.OR.equals(type)) {
                                    Map<String, Object> pathWhere = getPathWhere(where.getParams());
                                    return pathWhere == null ? null : Stream.of(pathWhere);
                                }
                                String field = where.getField();
                                switch (field) {
                                    case GraphQueryBuilder.RETURN:
                                        map = null;
                                        break;
                                    case GraphQueryBuilder.START_VERTEX:
                                    case GraphQueryBuilder.END_VERTEX:
                                    case GraphQueryBuilder.PATH_EDGE:
                                        map = MapUtil.builder(new HashMap<String, Object>(1)).put(field, where.getParams()).build();
                                        break;
                                    default:
                                        map = MapUtil.builder(new HashMap<String, Object>(1)).put(field, where.getParam()).build();
                                        break;
                                }
                                return map != null ? Stream.of(map) : null;
                            }
                    ).reduce(
                            (a, b) -> {
                                if (a != null) {
                                    if (b != null) {
                                        a.putAll(b);
                                    }
                                } else {
                                    return b;
                                }
                                return a;
                            }
                    ).orElse(null);
    }

    private Where findType(List<Where> whereList) {
        for (Where where : whereList) {
            Rel type = where.getType();
            if (Rel.AND.equals(type) || Rel.OR.equals(type)) {
                Where _where = findType(where.getParams());
                if (_where != null) {
                    return _where;
                }
            }
            String field = where.getField();
            if (StringUtils.equalsAnyIgnoreCase(field, GraphQueryBuilder.RETURN)) {
                return where;
            }
        }
        return null;
    }

    private void parseFirstNode(GraphTraversal graphTraversal, List<Where> wheres) {
        for (Where where : wheres) {
            Rel type = where.getType();
            if (Rel.AND.equals(type) || Rel.OR.equals(type)) {
                parseNode(graphTraversal, where.getParams());
            } else {
                String field = where.getField();
                if (StringUtils.equalsAnyIgnoreCase(GraphQueryBuilder.LABEL, field)
                        || StringUtils.equalsAnyIgnoreCase(GraphQueryBuilder.RETURN, field)) {
                    continue;
                }
                parseWhere(graphTraversal, where);
            }
        }
    }

    private void parseNode(GraphTraversal graphTraversal, List<Where> wheres) {
        for (Where where : wheres) {
            String field = where.getField();
            if (StringUtils.equalsAnyIgnoreCase(GraphQueryBuilder.LABEL, field)
                    || StringUtils.equalsAnyIgnoreCase(GraphQueryBuilder.RETURN, field)) {
                continue;
            }
            parseWhere(graphTraversal, where);
        }
    }

    private Where getLabel(List<Where> startWhere) {
        for (Where where : startWhere) {
            Rel type = where.getType();
            if (Rel.AND.equals(type) || Rel.OR.equals(type)) {
                Where label = getLabel(where.getParams());
                if (label != null) {
                    return label;
                }
            }
            String field = where.getField();
            if (StringUtils.equalsAnyIgnoreCase(GraphQueryBuilder.LABEL, field)) {
                return where;
            }
        }
        return null;
    }

    /**
     * 解析条件
     *
     * @param g
     * @param where
     */
    private void parseWhere(GraphTraversal g, Where where) {
        String field = where.getField();
        Rel type = where.getType();
        Object param = where.getParam();
        List<Where> params = where.getParams();
        switch (type) {
            case AND:
            case OR:
                List<GraphTraversal> graphTraversals = new ArrayList<>(params.size());
                for (Where _param : params) {
                    GraphTraversal<Object, Object> start = __.start();
                    parseWhere(start, _param);
                    graphTraversals.add(start);
                }
                if (Rel.OR.equals(type)) {
                    g.or(graphTraversals.toArray(new GraphTraversal[graphTraversals.size()]));
                } else {
                    g.and(graphTraversals.toArray(new GraphTraversal[graphTraversals.size()]));
                }
                break;
            case IN:
                g.has(field, P.within(param));
                break;
            case NIN:
                g.has(field, P.without(param));
                break;
            case EQ:
                g.has(field, param);
                break;
            case NE:
                g.has(field, P.neq(param));
                break;
            case GT:
                g.has(field, P.gt(param));
                break;
            case GTE:
                g.has(field, P.gte(param));
                break;
            case LT:
                g.has(field, P.lt(param));
                break;
            case LTE:
                g.has(field, P.lte(param));
                break;
            case BETWEEN:
                Object ltWhere = null;
                Object gtWhere = null;
                for (Where _where : params) {
                    Rel rel = _where.getType();
                    switch (rel) {
                        case LT:
                        case LTE:
                            ltWhere = _where.getParam();
                            break;
                        case GT:
                        case GTE:
                            gtWhere = _where.getParam();
                            break;
                        default:
                            throw new BusinessException("BETWEEN 无法解析除 [lt] [lte] [gt] [gte] 除外的操作符!");
                    }
                }
                if (ltWhere == null || gtWhere == null) {
                    throw new BusinessException("BETWEEN 条件中必须包含 [lt] [lte] [gt] [gte] 两组对应操作符!");
                }
                g.has(field, P.between(ltWhere, gtWhere));
                break;
            case NULL:
                g.has(field, P.eq(null));
                break;
            case NOT_NULL:
                g.has(field, P.neq(null));
                break;
            case FRONT_LIKE:
                g.has(field, TextP.startingWith((String) param));
                break;
            case TAIL_LIKE:
                g.has(field, TextP.endingWith((String) param));
                break;
            case MIDDLE_LIKE:
            case LIKE:
                String value = String.valueOf(where.getParam());
                value = StringUtils.replaceEach(value, new String[]{"*", "%", "?"}, new String[]{".*", ".*", "."});
                if (!StringUtils.startsWithAny(value, "*", "%", "?")) {
                    value = "^" + value;
                }
                if (!StringUtils.endsWithAny(value, "*", "%", "?")) {
                    value = value + "$";
                }
                g.has(field, TextP.regex(value));
                break;
            case NOT_MIDDLE_LIKE:
                String notValue = String.valueOf(where.getParam());
                notValue = StringUtils.replaceEach(notValue, new String[]{"*", "%", "?"}, new String[]{".*", ".*", "."});
                if (!StringUtils.startsWithAny(notValue, "*", "%", "?")) {
                    notValue = "^" + notValue;
                }
                if (!StringUtils.endsWithAny(notValue, "*", "%", "?")) {
                    notValue = notValue + "$";
                }
                g.has(field, TextP.notRegex(notValue));
                break;
            case NOT_FRONT_LIKE:
                g.has(field, TextP.notStartingWith((String) param));
                break;
            case NOT_TAIL_LIKE:
                g.has(field, TextP.notEndingWith((String) param));
                break;
            default:
                throw new BusinessException("图数据库暂时不支持解析当前操作符 [" + type + "]");
        }
    }

    @Override
    protected Q transitionUpdateParam(UpdateParamCondition updateParamCondition, D databaseConf, T tableConf) throws Exception {
        Graph graph = EmptyGraph.instance();
        GraphTraversalSource g = graph.traversal();
        GraphTraversal graphTraversal;
        Map<String, Object> updateParamMap = updateParamCondition.getUpdateParamMap();
        List<Where> wheres = updateParamCondition.getWhere();
        String nodeType = null;
        List<String> ids = new ArrayList<>();
        String fromId = null;
        String toId = null;
        nodeType = getUpdateWhere(wheres, ids);
        if (CollectionUtil.isEmpty(ids)) {
            // 以防万一加个保障
            throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "图数据更新必须指定节点或者边ID");
        }
        if (OperationField.VERTEX.equalsIgnoreCase(nodeType)) {
            if (CollectionUtil.isNotEmpty(ids)) {
                graphTraversal = g.V(ids.toArray());
            } else {
                graphTraversal = g.V();
            }
        } else if (OperationField.EDGE.equalsIgnoreCase(nodeType)) {
            if (CollectionUtil.isNotEmpty(ids)) {
                graphTraversal = g.E(ids.toArray());
            } else {
                graphTraversal = g.E();
            }
        } else {
            throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "图数据库无法识别当前操作节点类型：" + nodeType);
        }
        for (Map.Entry<String, Object> entry : updateParamMap.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            if (StringUtils.equalsAnyIgnoreCase(key, OperationField.OUT_V)) {
                if (!OperationField.EDGE.equalsIgnoreCase(nodeType)) {
                    throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "图数据库仅支持边配" + OperationField.OUT_V + "属性!");
                }
                fromId = String.valueOf(value);
                graphTraversal.from(fromId);
            } else if (StringUtils.equalsAnyIgnoreCase(key, OperationField.IN_V)) {
                if (!OperationField.EDGE.equalsIgnoreCase(nodeType)) {
                    throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "图数据库仅支持边配" + OperationField.IN_V + "属性!");
                }
                toId = String.valueOf(value);
                graphTraversal.to(toId);
            } else {
                if (value instanceof Collection) {
                    graphTraversal.property(VertexProperty.Cardinality.list, key, value);
                } else {
                    graphTraversal.property(key, value);
                }
            }
        }

        GremlinHandler handler = new GremlinHandler();
        GremlinHandler.GremlinUpdate gremlinUpdate = new GremlinHandler.GremlinUpdate();
        gremlinUpdate.setTraversal(graphTraversal);
        handler.setGremlinUpdate(gremlinUpdate);
        return (Q) handler;
    }

    private String getUpdateWhere(List<Where> wheres, List<String> ids) {
        String nodeType = null;
        for (Where where : wheres) {
            Object param = where.getParam();
            String field = where.getField();
            Rel type = where.getType();
            switch (type) {
                case AND:
                case OR:
                    List<Where> params = where.getParams();
                    String _nodeType = getUpdateWhere(params, ids);
                    if (StringUtils.isNotBlank(nodeType)) {
                        if (!StringUtils.equalsAnyIgnoreCase(nodeType, _nodeType)) {
                            throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "图数据库更新操作同时只能操作点或边!");
                        }
                    } else {
                        nodeType = _nodeType;
                    }
                    break;
                case EQ:
                    if (StringUtils.equalsAnyIgnoreCase(field, OperationField.VERTEX)) {
                        ids.add(String.valueOf(param));
                        if (StringUtils.isNotBlank(nodeType)) {
                            if (!StringUtils.equalsAnyIgnoreCase(nodeType, OperationField.VERTEX)) {
                                throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "图数据库更新操作同时只能操作点或边!");
                            }
                        } else {
                            nodeType = OperationField.VERTEX;
                        }
                    } else if (StringUtils.equalsAnyIgnoreCase(field, OperationField.EDGE)) {
                        ids.add(String.valueOf(param));
                        if (StringUtils.isNotBlank(nodeType)) {
                            if (!StringUtils.equalsAnyIgnoreCase(nodeType, OperationField.EDGE)) {
                                throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "图数据库更新操作同时只能操作点或边!");
                            }
                        } else {
                            nodeType = OperationField.EDGE;
                        }
                    } else {
                        throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "图数据库 EQ 操作暂时仅支持节点ID与关系配置!");
                    }
                    break;
                case IN:
                    List<String> idList = (List) param;
                    ids.addAll(idList);
                    if (StringUtils.equalsAnyIgnoreCase(field, OperationField.VERTEX)) {
                        if (StringUtils.isNotBlank(nodeType)) {
                            if (!StringUtils.equalsAnyIgnoreCase(nodeType, OperationField.VERTEX)) {
                                throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "图数据库更新操作同时只能操作点或边!");
                            }
                        } else {
                            nodeType = OperationField.VERTEX;
                        }
                    } else if (StringUtils.equalsAnyIgnoreCase(field, OperationField.EDGE)) {
                        if (StringUtils.isNotBlank(nodeType)) {
                            if (!StringUtils.equalsAnyIgnoreCase(nodeType, OperationField.EDGE)) {
                                throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "图数据库更新操作同时只能操作点或边!");
                            }
                        } else {
                            nodeType = OperationField.EDGE;
                        }
                    } else {
                        throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "图数据库 IN 操作暂时仅支持节点ID配置!");
                    }
                    break;
                default:
                    throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "图数据库暂时不支持除 EQ 和 IN 之外的操作符!");
            }
        }
        return nodeType;
    }

    @Override
    protected Q transitionInsertParam(AddParamCondition addParamCondition, D databaseConf, T tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + ".transitionInsertParam");
    }

    @Override
    protected Q transitionDeleteParam(DelParamCondition delParamCondition, D databaseConf, T tableConf) throws Exception {
        Graph graph = EmptyGraph.instance();
        GraphTraversalSource g = graph.traversal();
        GraphTraversal graphTraversal;
        List<Where> wheres = delParamCondition.getWhere();
        String nodeType = null;
        List<String> ids = new ArrayList<>();
        nodeType = getUpdateWhere(wheres, ids);
        if (CollectionUtil.isEmpty(ids)) {
            // 以防万一加个保障
            throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "图数据删除必须指定节点或者边ID");
        }
        if (OperationField.VERTEX.equalsIgnoreCase(nodeType)) {
            if (CollectionUtil.isNotEmpty(ids)) {
                graphTraversal = g.V(ids.toArray());
            } else {
                graphTraversal = g.V();
            }
        } else if (OperationField.EDGE.equalsIgnoreCase(nodeType)) {
            if (CollectionUtil.isNotEmpty(ids)) {
                graphTraversal = g.E(ids.toArray());
            } else {
                graphTraversal = g.E();
            }
        } else {
            throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "图数据库无法识别当前操作节点类型：" + nodeType);
        }

        graphTraversal.drop();

        GremlinHandler handler = new GremlinHandler();
        GremlinHandler.GremlinDelete gremlinDelete = new GremlinHandler.GremlinDelete();
        gremlinDelete.setTraversal(graphTraversal);
        gremlinDelete.setTotal(Long.valueOf(ids.size()));
        handler.setGremlinDelete(gremlinDelete);
        return (Q) handler;
    }

    @Override
    protected Q transitionDropTableParam(DropTableParamCondition dropTableParamCondition, D databaseConf, T tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + ".transitionDropTableParam");
    }

    @Override
    public Q transitionUpsertParam(UpsertParamCondition paramCondition, D databaseConf, T tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + ".transitionUpsertParam");
    }

    @Override
    public Q transitionUpsertParamBatch(UpsertParamBatchCondition paramCondition, D databaseConf, T tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + ".transitionUpsertParamBatch");
    }

    @Override
    public Q gremlinParser(String gremlin, Map<String, Object> params, D databaseConf, T tableConf) throws Exception {
        GremlinHandler handler = new GremlinHandler();
        handler.setQueryStr(gremlin);
        GremlinHandler.GremlinQuery query = new GremlinHandler.GremlinQuery();
        query.setGremlin(gremlin);
        query.setParams(params);
        handler.setGremlinQuery(query);
        return (Q) handler;
    }

    @Override
    public Q traversalParser(Traversal traversal, D databaseConf, T tableConf) throws Exception {
        GremlinHandler handler = new GremlinHandler();
        GremlinHandler.GremlinQuery query = new GremlinHandler.GremlinQuery();
        query.setQueryTraversal(traversal);
        handler.setGremlinQuery(query);
        return (Q) handler;
    }

    @Override
    public Q qlParser(String ql, Map<String, Object> params, D databaseConf, T tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + ".qlParser");
    }
}
