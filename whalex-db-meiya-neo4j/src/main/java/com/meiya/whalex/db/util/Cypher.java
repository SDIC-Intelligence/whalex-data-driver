package com.meiya.whalex.db.util;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.map.MapUtil;
import com.meiya.whalex.db.entity.AddParamCondition;
import com.meiya.whalex.db.entity.DelParamCondition;
import com.meiya.whalex.db.entity.UpdateParamCondition;
import com.meiya.whalex.exception.BusinessException;
import com.meiya.whalex.interior.db.constant.CommonConstant;
import com.meiya.whalex.interior.db.search.condition.Rel;
import com.meiya.whalex.interior.db.search.in.Order;
import com.meiya.whalex.interior.db.search.in.Page;
import com.meiya.whalex.interior.db.search.in.QueryParamCondition;
import com.meiya.whalex.interior.db.search.in.Where;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import static org.apache.commons.lang3.StringUtils.*;
import static org.apache.commons.collections.CollectionUtils.isNotEmpty;
import static org.apache.commons.collections.CollectionUtils.isEmpty;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * 生成 Neo4j Cypher 语句
 *
 * @author chenjp
 * @date 2020/10/9
 */
@Slf4j
public final class Cypher implements CommonConstant {

    private static final String COLON = ":";
    private static final String SPACE = " ";
    private static final String DOT = ".";
    private static final String DASH = "-";
    private static final String QUOTE = "\"";
    private static final String LEFT_BRACE = "{";
    private static final String RIGHT_BRACE = "}";
    private static final String LEFT_BRACKET = "(";
    private static final String RIGHT_BRACKET = ")";
    private static final String LEFT_SQ_BRACKET = "[";
    private static final String RIGHT_SQ_BRACKET = "]";
    private static final String AND = "and";
    private static final String OR = "or";
    private static final String EQUALS = "=";
    private static final String RIGHT_BRACE_BRACKET = RIGHT_BRACE + RIGHT_BRACKET;
    private static final String MATCH = "match ";
    private static final String RIGHT_BRACKET_COMMA = RIGHT_BRACKET + COMMA;
    private static final String NULL = "null";


    /**
     * 查询
     */
    public static class Query {

        /**
         * 查询 match
         */
        public static String doQuery(QueryParamCondition queryParamCondition) {
            StringBuilder cql = new StringBuilder(MATCH);
            //  查询字段（返回的字段）
            List<String> selectField = queryParamCondition.getSelect();
            //  查询条件
            List<Where> wheres = queryParamCondition.getWhere();
            String alias = "*";
            if (CollectionUtil.isNotEmpty(wheres)) {
                //  是否有节点信息
                AtomicBoolean hasNodeInfo = new AtomicBoolean(false);
                //  设置节点信息
                setNodeInfo(wheres, cql, hasNodeInfo);
                //  设置关系
                setRelationship(wheres, cql, hasNodeInfo);
                //  设置where查询条件
                setWhere(wheres, cql);
                alias = wheres.stream().map(Where::getAlias).collect(Collectors.joining(COMMA));
            } else {
                cql.append("(n)");
            }
            //  设置返回结果
            setSelectField(selectField, cql, alias);
            //  设置排序
            setOrderBy(queryParamCondition.getOrder(), cql, alias);
            //  设置分页查询
            if (queryParamCondition.getPage() != null) {
                setPageInfo(queryParamCondition.getPage(), cql);
            }
            log.info("doQuery： {}", cql);
            return cql.toString();
        }

        /**
         * 设置节点信息
         */
        private static void setNodeInfo(List<Where> wheres, StringBuilder cql, AtomicBoolean hasNodeInfo) {
            wheres.forEach(where -> {
                if (where.getParam() instanceof Map || where.getParam() instanceof String) {
                    if (where.getParam() instanceof Map) {
                        Map<String, Object> labelMap = (Map<String, Object>) where.getParam();
                        if (!(labelMap.containsKey("label") && labelMap.containsKey("where"))) {
                            return;
                        }
                    }
                    hasNodeInfo.set(true);
                    //  节点信息
                    cql.append(LEFT_BRACKET);
                    hasAlias(where.getAlias(), cql);
                    if (where.getParam() != null) {
                        if (where.getParam() instanceof Map && MapUtil.isNotEmpty((Map) where.getParam())) {
                            Map<String, Object> config = (Map<String, Object>) where.getParam();
                            Object label = config.get(LABEL);
                            if (label == null) {
                                throw new BusinessException("Neo4j 节点配置需要携带 label 值!");
                            }
                            cql.append(COLON).append(label.toString().replaceAll(COMMA, COLON));
                            Object labelWhereObj = config.get("where");
                            if (labelWhereObj != null) {
                                if (!(labelWhereObj instanceof Where)) {
                                    throw new BusinessException("Neo4j 节点配置需要携带 where 值, 并且 where 值必须为 com.meiya.whalex.interior.db.search.in.Where 类型");
                                }
                                Where labelWhere = (Where) labelWhereObj;
                                cql.append(LEFT_BRACE);
                                parserRel(cql, labelWhere);
                                cql.append(RIGHT_BRACE);
                            }
                        } else {
                            cql.append(COLON).append(where.getParam().toString().replaceAll(COMMA, COLON));
                        }
                    }
                    cql.append(RIGHT_BRACKET_COMMA);
                }
            });
            removeLastStr(cql);
        }

        /**
         * 设置查询条件
         */
        public static void setWhere(List<Where> wheres, StringBuilder cql) {
            wheres.forEach(where -> {
                if (NULL.equals(where.getType().getName())) {
                    return;
                }
                if (where.getType() == Rel.AND || where.getType() == Rel.OR) {
                    List<Where> list = where.getParams();
                    if (isEmpty(list)) return;
                }
                hasWhere(cql);
                parserRel(cql, where);
            });
        }

        /**
         * 解析操作符
         *
         * @param cql
         * @param where
         */
        private static void parserRel(StringBuilder cql, Where where) {
            if (!(where.getParam() instanceof Map)) {
                switch (where.getType()) {
                    case EQ:
                        appendCql(where, cql, " = ");
                        break;
                    case NE:
                        appendCql(where, cql, " <> ");
                        break;
                    case LIKE:
                        hasAlias(where.getAlias(), cql);
                        cql.append(DOT).append(where.getField()).append(" =~ ");
                        cql.append("\".*").append(where.getParam()).append(".*\"");
                        break;
                    case GT:
                        appendCql(where, cql, " > ");
                        break;
                    case GTE:
                        appendCql(where, cql, " >= ");
                        break;
                    case LT:
                        appendCql(where, cql, " < ");
                        break;
                    case LTE:
                        appendCql(where, cql, " <= ");
                        break;
                    case AND:
                        judgeAndPrefix(cql);
                        List<Where> andParams = where.getParams();
                        if (andParams == null || andParams.isEmpty()) break;
                        andParams.forEach(element -> element.setAlias(where.getAlias()));
                        recursion(andParams, cql);
                        if (cql.indexOf(OR) != -1) {
                            cql.append(RIGHT_BRACKET).append(SPACE);
                        }
                        break;
                    case OR:
                        judgeOrPrefix(cql);
                        List<Where> orParams = where.getParams();
                        if (orParams == null || orParams.isEmpty()) break;
                        orParams.forEach(element -> element.setAlias(where.getAlias()));
                        recursion(orParams, cql);
                        if (cql.indexOf(AND) != -1) {
                            cql.append(RIGHT_BRACKET).append(SPACE);
                        }
                        break;
                    case TAIL_LIKE:
                        hasAlias(where.getAlias(), cql);
                        cql.append(DOT).append(where.getField());
                        cql.append(" starts with ").append(QUOTE).append(where.getParam()).append(QUOTE);
                        break;
                    case FRONT_LIKE:
                        hasAlias(where.getAlias(), cql);
                        cql.append(DOT).append(where.getField());
                        cql.append(" ends with ").append(QUOTE).append(where.getParam()).append(QUOTE);
                        break;
                    case EXISTS:
                        hasAlias(where.getAlias(), cql);
                        cql.append(DOT).append(where.getField());
                        cql.append(" contains ").append(QUOTE).append(where.getParam()).append(QUOTE);
                        break;
                    default:
                        break;
                }
            }
        }

        /**
         * 递归拼接and 与 or
         */
        private static void recursion(List<Where> list, StringBuilder cql) {
            for (int i = 0, length = list.size(); i < length; i++) {
                List<Where> wheres = new ArrayList<>(1);
                wheres.add(list.get(i));
                setWhere(wheres, cql);
                if (i != length - 1) {
                    cql.append(SPACE).append(OR).append(SPACE);
                }
            }
        }

        /**
         * 设置节点之间的关系
         */
        @SuppressWarnings("all")
        private static void setRelationship(List<Where> wheres, StringBuilder cql, AtomicBoolean hasNodeInfo) {
            wheres.forEach(where -> {
                if (where.getParam() instanceof Map) {
                    if(hasNodeInfo.get()) cql.append(COMMA);
                    Map<String, Object> relationshipMap = (Map<String, Object>) where.getParam();
                    String relationshipAlias = where.getAlias();
                    if (relationshipMap.containsKey(BEGIN) && relationshipMap.containsKey(END)) {
                        Where beginWhere = (Where) relationshipMap.get(BEGIN);
                        Where endWhere = (Where) relationshipMap.get(END);
                        String relationshipLabel = (String) relationshipMap.get(LABEL);
                        Map<String, Integer> depthMap = (Map<String, Integer>) relationshipMap.get(DEPTH);
                        String depth = parseDepth(depthMap);
                        if (StringUtils.isNotBlank(depth)) {
                            relationshipLabel = relationshipLabel + "*" + depth;
                        }
                        cql.append(LEFT_BRACKET).append(beginWhere.getAlias()).append(RIGHT_BRACKET);
                        cql.append(SPACE).append(DASH).append(SPACE).append(LEFT_SQ_BRACKET);
                        //  关系节点别名
                        if (isNotBlank(relationshipAlias)) {
                            cql.append(relationshipAlias);
                        }
                        //  关系节点标签
                        if (isNotBlank(relationshipLabel)) {
                            cql.append(COLON).append(relationshipLabel.replaceAll(COMMA, COLON));
                        }
                        //  关系节点信息
                        if (relationshipMap.containsKey(RELATIONSHIP_INFO)) {
                            Map<String, Object> relationshipInfo = (Map<String, Object>) relationshipMap.get(RELATIONSHIP_INFO);
                            if (relationshipMap != null) {
                                cql.append(SPACE).append(LEFT_BRACE);
                                for (Map.Entry<String, Object> entry : relationshipInfo.entrySet()) {
                                    cql.append(entry.getKey()).append(COLON);
                                    appendNodeInfo(entry.getValue(), cql);
                                    cql.append(COMMA);
                                }
                                removeLastStr(cql);
                                cql.append(RIGHT_BRACE);
                            }
                        }
                        cql.append(RIGHT_SQ_BRACKET).append(" -> ").append(LEFT_BRACKET).append(endWhere.getAlias()).append(RIGHT_BRACKET);
                    }
                }
            });
        }

        /**
         * 解析关系深度
         *
         * @param depthMap
         * @return
         */
        private static String parseDepth(Map<String, Integer> depthMap) {
            if (MapUtil.isEmpty(depthMap)) {
                return null;
            }
            Integer minHops = depthMap.get(MIN_HOPS);
            Integer maxHops = depthMap.get(MAX_HOPS);

            StringBuilder sb = new StringBuilder();

            if (minHops != null) {
                sb.append(minHops);
            }

            sb.append("...");

            if (maxHops != null) {
                sb.append(maxHops);
            }

            return sb.toString();
        }

        /**
         * 设置返回的结果
         */
        private static void setSelectField(List<String> selectField, StringBuilder cql, String alias) {
            if (isNotEmpty(selectField)) {
                hasReturn(cql);
                selectField.forEach(field -> {
                    appendAlias(alias, cql);
                    cql.append(field).append(COMMA);

                });
                removeLastStr(cql);
            } else {
                hasReturn(cql);
                List<String> aliaList = Arrays.asList(alias.split(COMMA));
                if (isNotEmpty(aliaList)) {
                    if (aliaList.size() == 1) {
                        cql.append(aliaList.get(0));
                    } else {
                        aliaList.forEach(alia -> cql.append(alia).append(COMMA));
                        removeLastStr(cql);
                    }
                }
            }
        }

        /**
         * 设置排序
         */
        private static void setOrderBy(List<Order> orders, StringBuilder cql, String alias) {
            if (isNotEmpty(orders)) {
                orders.forEach(order -> {
                    hasOrderBy(cql);
                    appendAlias(alias, cql);
                    cql.append(order.getField()).append(SPACE).append(order.getSort().getName()).append(COMMA);
                });
                removeLastStr(cql);
            }
        }

        /**
         * 设置分页信息
         */
        private static void setPageInfo(Page page, StringBuilder cql) {
            if (page.getOffset() != null && page.getOffset() != 0) {
                cql.append(" skip ").append(page.getOffset());
            }
            if (page.getLimit() != null && page.getLimit() != 0) {
                cql.append(" limit ").append(page.getLimit());
            }
        }
    }

    /**
     * 新增
     */
    public static class Insert {

        public static String doInsert(AddParamCondition addParamCondition) {
            List<Map<String, Object>> list = addParamCondition.getFieldValueList();
            if (isEmpty(list)) {
                throw new BusinessException("params error");
            }
            StringBuilder cql = new StringBuilder("create ");
            //  是否有设置关系
            AtomicBoolean hasRelationship = new AtomicBoolean(false);
            //  设置节点信息
            setNodeInfo(list, cql, hasRelationship);
            if (!hasRelationship.get()) removeLastStr(cql);
            //  设置关系信息
            setRelationship(list, cql);
            //  设置返回数据
            setSelectField(list, cql);
            log.info("doInsert： {}", cql);
            return cql.toString();
        }

        /**
         * 设置节点信息
         */
        @SuppressWarnings("all")
        private static void setNodeInfo(List<Map<String, Object>> list, StringBuilder cql, AtomicBoolean hasRelationship) {
            list.forEach(element -> {
                //  设置节点的别名与标签
                if (element.containsKey(ALIAS) || element.containsKey(LABEL)) {
                    cql.append(LEFT_BRACKET);
                    hasAlias((String) element.get(ALIAS), cql);
                    if (element.get(LABEL) != null) {
                        cql.append(COLON).append(element.get(LABEL).toString().replaceAll(COMMA, COLON));
                    }
                    cql.append(SPACE);
                }
                //  设置节点信息
                if (element.containsKey(NODE_INFO)) {
                    Map<String, Object> nodeInfo = (Map<String, Object>) element.get(NODE_INFO);
                    if (nodeInfo != null) {
                        cql.append(LEFT_BRACE);
                        for (Map.Entry<String, Object> entry : nodeInfo.entrySet()) {
                            cql.append(entry.getKey()).append(COLON);
                            appendNodeInfo(entry.getValue(), cql);
                            cql.append(COMMA);
                        }
                        removeLastStr(cql);
                        cql.append(RIGHT_BRACE);
                    }
                }
                if (!isRelationshipMap(element)) {
                    cql.append(RIGHT_BRACKET_COMMA);
                } else {
                    hasRelationship.set(true);
                }
            });
        }

        /**
         * 设置关系信息
         */
        @SuppressWarnings("all")
        private static void setRelationship(List<Map<String, Object>> list, StringBuilder cql) {
            list.forEach(element -> {
                if (isRelationshipMap(element)) {
                    Map<String, Object> beginMap = (Map<String, Object>) element.get(BEGIN);
                    Map<String, Object> endMap = (Map<String, Object>) element.get(END);
                    //  起始或终止节点为空，什么都不做
                    if (beginMap == null || endMap == null) {
                        return;
                    }
                    Map<String, Object> relationshipInfo = (Map<String, Object>) element.get(RELATIONSHIP_INFO);
                    cql.append(LEFT_BRACKET);
                    hasAlias((String) beginMap.get(ALIAS), cql);
                    cql.append(RIGHT_BRACKET).append(SPACE).append(DASH).append(SPACE).append(LEFT_SQ_BRACKET);
                    //  判断是否设置了关系的别名
                    hasRelationshipAlias((String) element.get(RELATIONSHIP_ALIAS), cql);
                    String labels = (String) element.get(RELATIONSHIP_LABEL);
                    if (isNotBlank(labels)) {
                        cql.append(COLON).append(labels.replaceAll(COMMA, COLON)).append(SPACE);
                    }
                    //  如果关系节点信息不为空-即关系的属性不为空
                    if (relationshipInfo != null) {
                        cql.append(LEFT_BRACE);
                        for (Map.Entry<String, Object> entry : relationshipInfo.entrySet()) {
                            cql.append(entry.getKey()).append(COLON);
                            appendNodeInfo(entry.getValue(), cql);
                            cql.append(COMMA);
                        }
                        cql.delete(cql.length() - 1, cql.length());
                        cql.append(RIGHT_BRACE);
                    }
                    cql.append(RIGHT_SQ_BRACKET);
                    if (endMap != null) {
                        cql.append(" -> ");
                        cql.append(LEFT_BRACKET).append(endMap.get(ALIAS)).append(RIGHT_BRACKET);
                    }
                }
            });
        }

        /**
         * 设置返回数据
         */
        private static void setSelectField(List<Map<String, Object>> list, StringBuilder cql) {
            hasReturn(cql);
            list.forEach(element -> {
                if (element.containsKey(SELECT_FIELD)) {
                    List<String> selectFields = Arrays.asList(((String) element.get(SELECT_FIELD)).split(COMMA));
                    selectFields.forEach(field -> {
                        hasAlias((String) element.get(ALIAS), cql);
                        cql.append(DOT).append(field).append(COMMA);
                    });
                } else if (isRelationshipMap(element)) {
                    hasRelationshipAlias((String) element.get(RELATIONSHIP_ALIAS), cql);
                    cql.append(COMMA);
                } else {
                    hasAlias((String) element.get(ALIAS), cql);
                    cql.append(COMMA);
                }
            });
            removeLastStr(cql);
        }
    }

    /**
     * 更新
     */
    @SuppressWarnings("all")
    public static class Update {

        public static String doUpdate(UpdateParamCondition updateParamCondition) {
            List<Where> wheres = updateParamCondition.getWhere();
            if (isEmpty(wheres)) {
                throw new BusinessException("params error");
            }
            StringBuilder cql = new StringBuilder(MATCH);
            //  节点信息
            setNode(wheres, cql);
            //  查询条件
            Query.setWhere(wheres, cql);
            //  设置更新内容
            updateInfo(updateParamCondition.getUpdateParamMap(), cql);
            //  返回字段
            returnInfo(updateParamCondition.getUpdateParamMap(), cql);
            log.info("doUpdate： {}", cql);
            return cql.toString();
        }

        private static void setNode(List<Where> wheres, StringBuilder cql) {
            wheres.forEach(where -> {
                cql.append(LEFT_BRACKET);
                hasAlias(where.getAlias(), cql);
                if (where.getParam() != null && where.getParam() instanceof List) {
                    List<String> labels = (List<String>) where.getParam();
                    if (isNotEmpty(labels))
                        cql.append(COLON).append(join(labels, COLON));
                }
                cql.append(RIGHT_BRACKET_COMMA);
            });
            removeLastStr(cql);
        }

        private static void updateInfo(Map<String, Object> map, StringBuilder cql) {
            if (map != null) {
                cql.append(" set ");
                for (Map.Entry<String, Object> entry : map.entrySet()) {
                    if (LABEL.equals(entry.getKey())) {
                        Object labels = entry.getValue();
                        if (labels instanceof List) {
                            cql.append("t:").append(join(labels, COLON));
                        }
                        if (labels instanceof String) {
                            cql.append("t:").append(((String) labels).replaceAll(COMMA, COLON));
                        }
                        cql.append(COMMA);
                    } else if (APPEND.equals(entry.getKey()) && entry.getValue() instanceof Map) {
                        Map<String, Object> addMap = (Map<String, Object>) entry.getValue();
                        cql.append("t += {");
                        for (Map.Entry<String, Object> addEntry : addMap.entrySet()) {
                            cql.append(addEntry.getKey()).append(COLON);
                            if (isNotStr(addEntry.getValue())) {
                                cql.append(addEntry.getValue());
                            } else {
                                cql.append(QUOTE).append(addEntry.getValue()).append(QUOTE);
                            }
                            cql.append(COMMA);
                        }
                        cql.delete(cql.length() - 1, cql.length());
                        cql.append(RIGHT_BRACE);
                    } else {
                        if (!SELECT_FIELD.equals(entry.getKey())) {
                            cql.append("t.").append(entry.getKey()).append(SPACE).append(EQUALS).append(SPACE);
                            appendNodeInfo(entry.getValue(), cql);
                            cql.append(COMMA);
                        }
                    }
                }
            }
        }

        private static void returnInfo(Map<String, Object> map, StringBuilder cql) {
            if (map != null) {
                hasReturn(cql);
                if (map.containsKey(SELECT_FIELD)) {
                    if (map.get(SELECT_FIELD) instanceof List) {
                        List<String> selectFields = (List<String>) map.get(SELECT_FIELD);
                        if (isNotEmpty(selectFields))
                            selectFields.forEach(field -> cql.append("t.").append(field).append(COMMA));
                    } else if (map.get(SELECT_FIELD) instanceof String) {
                        List<String> selectFields = Arrays.asList(((String) map.get(SELECT_FIELD)).split(COMMA));
                        if (isNotEmpty(selectFields))
                            selectFields.forEach(field -> cql.append("t.").append(field).append(COMMA));
                    }
                } else {
                    cql.append("t");
                }
            }
            removeLastStr(cql);
        }

    }


    /**
     * 删除
     */
    @SuppressWarnings("all")
    public static class Delete {

        public static String doDelete(DelParamCondition delParamCondition) {
            List<Where> wheres = delParamCondition.getWhere();
            if (isEmpty(wheres)) {
                throw new BusinessException("params error");
            }
            StringBuilder cql = new StringBuilder(MATCH);
            //  设置节点信息
            setLabelAndNodeInfo(wheres, cql);
            //  拼接移除标签、属性、删除信息、返回信息
            setOther(wheres, cql);
            log.info("doDelete： {}", cql);
            return cql.toString();
        }

        private static void setOther(List<Where> wheres, StringBuilder cql) {
            wheres.forEach(where -> {
                if (where.getParam() != null && where.getParam() instanceof Map) {
                    Map<String, Object> map = (Map<String, Object>) where.getParam();
                    //  移除标签
                    removeLabel(map, cql);
                    //  移除属性
                    removeField(map, cql);
                    //  删除单个或多个节点信息
                    delete(map, cql);
                    //  返回字段
                    selectField(map, cql);
                }
            });
        }

        private static void setLabelAndNodeInfo(List<Where> wheres, StringBuilder cql) {
            wheres.forEach(where -> {
                if (where.getParam() != null && where.getParam() instanceof Map) {
                    Map<String, Object> map = (Map<String, Object>) where.getParam();
                    Map<String, Object> nodeInfo = (Map<String, Object>) map.get(NODE_INFO);
                    //  设置节点标签与属性信息
                    setLabelAndNodeInfo(map, nodeInfo, cql);
                }
            });
            removeLastStr(cql);
        }

        private static void setLabelAndNodeInfo(Map<String, Object> map, Map<String, Object> nodeInfo, StringBuilder cql) {
            cql.append(LEFT_BRACKET);
            hasAlias((String) map.get(ALIAS), cql);
            if (map.containsKey(LABEL)) {
                if (map.get(LABEL) instanceof List) {
                    List<String> labels = (List<String>) map.get(LABEL);
                    if (isNotEmpty(labels))
                        cql.append(COLON).append(join(labels, COLON));
                } else if (map.get(LABEL) instanceof String) {
                    cql.append(COLON).append(map.get(LABEL).toString().replaceAll(COMMA, COLON));
                }
            }
            if (nodeInfo != null) {
                cql.append(SPACE).append(LEFT_BRACE);
                for (Map.Entry<String, Object> entry : nodeInfo.entrySet()) {
                    cql.append(entry.getKey()).append(COLON);
                    appendNodeInfo(entry.getValue(), cql);
                    cql.append(COMMA);
                }
                removeLastStr(cql);
                cql.append(RIGHT_BRACE_BRACKET);
            }
            cql.append(COMMA);
        }

        private static void removeLabel(Map<String, Object> map, StringBuilder cql) {
            if (map.containsKey(REMOVE_LABEL)) {
                if (map.get(REMOVE_LABEL) instanceof List) {
                    List<String> removeLabels = (List<String>) map.get(REMOVE_LABEL);
                    if (isNotEmpty(removeLabels)) {
                        hasRemove(cql);
                        hasAlias((String) map.get(ALIAS), cql);
                    }
                    removeLabels.forEach(label -> cql.append(COLON).append(label));
                    cql.append(COMMA);
                } else if (map.get(REMOVE_LABEL) instanceof String) {
                    hasRemove(cql);
                    hasAlias((String) map.get(ALIAS), cql);
                    cql.append(COLON).append(map.get(REMOVE_LABEL).toString().replaceAll(COMMA, COLON)).append(COMMA);
                }
            }
        }

        private static void removeField(Map<String, Object> map, StringBuilder cql) {
            if (map.containsKey(REMOVE_FIELD)) {
                if (map.get(REMOVE_FIELD) instanceof List) {
                    List<String> removeFields = (List<String>) map.get(REMOVE_FIELD);
                    hasRemove(cql);
                    removeFields.forEach(field -> {
                        hasAlias((String) map.get(ALIAS), cql);
                        cql.append(DOT).append(field).append(COMMA);
                    });
                } else if (map.get(REMOVE_FIELD) instanceof String) {
                    List<String> removeField = Arrays.asList(((String) map.get(REMOVE_FIELD)).split(COMMA));
                    hasRemove(cql);
                    removeField.forEach(field -> {
                        hasAlias((String) map.get(ALIAS), cql);
                        cql.append(DOT).append(field).append(COMMA);
                    });
                }
                removeLastStr(cql);
            }
        }

        private static void delete(Map<String, Object> map, StringBuilder cql) {
            if (map.containsKey(DELETE) && map.get(DELETE) != null && (boolean) map.get(DELETE)) {
                if (map.containsKey(DETACH) && map.get(DETACH) != null && (boolean) map.get(DETACH)) {
                    cql.append(" detach delete ");
                    hasAlias((String) map.get(ALIAS), cql);
                } else {
                    cql.append(" delete ");
                    hasAlias((String) map.get(ALIAS), cql);
                }
            }
        }

        private static void selectField(Map<String, Object> map, StringBuilder cql) {
            if (map.containsKey(SELECT_FIELD) && map.get(SELECT_FIELD) != null) {
                hasReturn(cql);
                if (map.get(SELECT_FIELD) instanceof List) {
                    List<String> selectFields = (List<String>) map.get(SELECT_FIELD);
                    if (isNotEmpty(selectFields)) {
                        selectFields.forEach(field -> {
                            hasAlias((String) map.get(ALIAS), cql);
                            cql.append(DOT).append(field).append(COMMA);
                        });
                        removeLastStr(cql);
                    }
                } else if (map.get(SELECT_FIELD) instanceof String) {
                    List<String> selectFields = Arrays.asList(map.get(SELECT_FIELD).toString().split(COMMA));
                    if (isNotEmpty(selectFields)) {
                        selectFields.forEach(field -> {
                            hasAlias((String) map.get(ALIAS), cql);
                            cql.append(DOT).append(field).append(COMMA);
                        });
                        removeLastStr(cql);
                    }
                }
            }
        }
    }


    /**
     * 是否不为String类型
     */
    public static boolean isNotStr(Object obj) {
        return !(obj instanceof String);
    }

    /**
     * 判断当前map是否是关系节点
     */
    public static boolean isRelationshipMap(Map<String, Object> map) {
        return map.containsKey(BEGIN) || map.containsKey(END);
    }

    /**
     * 追加cql
     */
    public static void appendCql(Where where, StringBuilder cql, String symbol) {
        hasAlias(where.getAlias(), cql);
        cql.append(DOT).append(where.getField()).append(symbol);
        if (isNotStr(where.getParam())) {
            cql.append(where.getParam());
        } else {
            cql.append(QUOTE).append(where.getParam()).append(QUOTE);
        }
    }

    /**
     * 判断是否有设置标签别名
     */
    public static void hasAlias(String alias, StringBuilder cql) {
        if (isNotBlank(alias)) {
            cql.append(alias);
        } else {
            cql.append("t");
        }
    }

    /**
     * 判断是否有设置关系别名
     */
    public static void hasRelationshipAlias(String alias, StringBuilder cql) {
        if (isNotBlank(alias)) {
            cql.append(alias);
        } else {
            cql.append("r");
        }
    }

    /**
     * 移除最后一个字符
     */
    private static void removeLastStr(StringBuilder cql) {
        cql.delete(cql.length() - 1, cql.length());
    }

    /**
     * 当前语句中是否已有where关键字
     */
    private static void hasWhere(StringBuilder cql) {
        hasStr(cql, "where");
    }

    /**
     * 当前语句中是否已有return关键字
     */
    private static void hasReturn(StringBuilder cql) {
        hasStr(cql, "return");
    }

    /**
     * 当前语句中是否已有remove关键字
     */
    private static void hasRemove(StringBuilder cql) {
        hasStr(cql, "remove");
    }

    private static void hasStr(StringBuilder cql, String str) {
        if (cql.indexOf(str) == -1) {
            cql.append(SPACE).append(str).append(SPACE);
        }
    }

    /**
     * 判断and关系前缀
     */
    private static void judgeAndPrefix(StringBuilder cql) {
        if (cql.indexOf(OR) != -1) {
            cql.append(SPACE).append(OR).append(SPACE).append(LEFT_BRACKET);
        }
        if (cql.indexOf(AND) != -1) {
            cql.append(SPACE).append(AND).append(SPACE);
        }
    }

    /**
     * 判断or关系前缀
     */
    private static void judgeOrPrefix(StringBuilder cql) {
        if (cql.indexOf(AND) != -1) {
            cql.append(SPACE).append(AND).append(SPACE).append(LEFT_BRACKET);
        }
        if (cql.indexOf(OR) != -1) {
            cql.append(SPACE).append(OR).append(SPACE);
        }
    }

    /**
     * 判断当前值是否为String或其他类型
     */
    private static void appendNodeInfo(Object value, StringBuilder cql) {
        if (isNotStr(value)) {
            cql.append(value);
        } else {
            cql.append(QUOTE).append(value).append(QUOTE);
        }
    }

    private static void appendAlias(String alias, StringBuilder cql) {
        if (isNotBlank(alias)) {
            List<String> aliaList = Arrays.asList(alias.split(COMMA));
            if (aliaList.size() == 1) {
                hasAlias(alias, cql);
                cql.append(DOT);
            }
        } else {
            hasAlias(alias, cql);
            cql.append(DOT);
        }
    }

    private static void hasOrderBy(StringBuilder cql) {
        if (cql.indexOf("order by") == -1) {
            cql.append(" order by ");
        }
    }


}
