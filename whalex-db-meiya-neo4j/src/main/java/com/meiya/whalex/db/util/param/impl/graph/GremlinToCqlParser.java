package com.meiya.whalex.db.util.param.impl.graph;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.util.ArrayUtil;
import cn.hutool.core.util.StrUtil;
import com.meiya.whalex.db.entity.graph.Neo4jHandler;
import com.meiya.whalex.graph.entity.GraphDirection;
import com.meiya.whalex.graph.exception.GremlinParserException;
import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.gremlin.language.grammar.GremlinQueryParser;
import org.apache.tinkerpop.gremlin.process.traversal.*;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.LoopTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.TrueTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.PropertyType;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.lang.reflect.Field;
import java.util.*;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Gremlin 转 CQL 语法
 *
 * @author 黄河森
 * @date 2023/1/3
 * @package com.meiya.whalex.db.util.param.impl.graph
 * @project whalex-data-driver
 */
public class GremlinToCqlParser {

    /**
     * 原始 Gremlin 语法
     */
    private String gremlin;

    /**
     * 解析的节点/关系树
     */
    private CypherObject node;

    /**
     * 当前操作节点
     */
    private CypherObject current;

    /**
     * 条件解析
     */
    private List<CypherWhere> wheres = new ArrayList<>();

    /**
     * 返参解析
     */
    private List<String> returns = new ArrayList<>();

    /**
     * 当前 Gremlin 解析返回的最终类型
     */
    private ReturnType returnType;

    /**
     * Neo4J 工具语法
     * unwind、foreach
     */
    private List<UtilFunction> utilCQL = new ArrayList<>();

    /**
     * 分页
     */
    private CypherPage page;

    public GremlinToCqlParser(String gremlin) {
        this.gremlin = gremlin;
    }

    /**
     * 解析构造
     *
     * @param gremlin
     * @return
     */
    public static GremlinToCqlParser parser(String gremlin) {
        return new GremlinToCqlParser(gremlin);
    }

    /**
     * 转为 CQL
     *
     * @return
     */
    public Neo4jHandler.Noe4jGremlinQuery toCQL() {
        DefaultGraphTraversal parse = (DefaultGraphTraversal) GremlinQueryParser.parse(gremlin);
        Step step = parse.getStartStep();
        while (step != null) {
            parserStep(step, null, null);
            returnType = checkEndStep(step);
            if (returnType != null) {
                break;
            } else {
                step = step.getNextStep();
            }
        }
        // 占位符参数
        Map<String, Object> params = new HashMap<>();
        String cypher = generateCypher(params);
        Neo4jHandler.Noe4jGremlinQuery query = new Neo4jHandler.Noe4jGremlinQuery(cypher, params, returnType);
        return query;
    }

    /**
     * 解析操作步骤
     *
     * @param step
     */
    private void parserStep(Step step, Integer relationShipStart, Integer relationShipEnd) {
        // 处理节点或者边对象
        if (step instanceof GraphStep) {
            graphStep((GraphStep) step);
        } else if (step instanceof HasStep) {
            hasStep((HasStep) step);
        } else if (step instanceof VertexStep) {
            vertexStep((VertexStep) step, relationShipStart, relationShipEnd);
        } else if (step instanceof PathStep) {
            pathStep((PathStep) step);
        } else if (step instanceof EdgeVertexStep) {
            edgeVertexStep((EdgeVertexStep) step, relationShipStart, relationShipEnd);
        } else if (step instanceof PropertyMapStep) {
            propertyMapStep((PropertyMapStep) step);
        } else if (step instanceof PropertiesStep) {
            propertiesStep((PropertiesStep) step);
        } else if (step instanceof IdStep) {
            idStep((IdStep) step);
        } else if (step instanceof LabelStep) {
            labelStep((LabelStep) step);
        } else if (step instanceof EdgeOtherVertexStep) {
            edgeOtherVertexStep((EdgeOtherVertexStep) step);
        } else if (step instanceof CountGlobalStep) {
            countGlobalStep((CountGlobalStep) step);
        } else if (step instanceof RangeGlobalStep) {
            rangeGlobalStep((RangeGlobalStep) step);
        } else if (step instanceof TailGlobalStep) {
            tailGlobalStep((TailGlobalStep) step);
        } else if (step instanceof TraversalFilterStep) {
            traversalFilterStep((TraversalFilterStep) step);
        } else if (step instanceof NotStep) {
            notStep((NotStep) step);
        } else if (step instanceof RepeatStep) {
            repeatStep((RepeatStep) step);
        } else {
            throw new GremlinParserException("Neo4j 无法解析当前操作 [" + step.getClass().getName() + "]");
        }
    }

    /**
     * 解析指定重复执行语句
     * .repeat()
     *
     * @param repeatStep
     */
    private void repeatStep(RepeatStep repeatStep) {
        Traversal.Admin repeatTraversal = repeatStep.getRepeatTraversal();
        // 条件，重复执行的终止条件
        Traversal.Admin untilTraversal = repeatStep.getUntilTraversal();
        // 是否收集路径结果
        Traversal.Admin emitTraversal = repeatStep.getEmitTraversal();
        boolean emit;
        // emit 条件对象
        Step startEmitStep = null;
        if (emitTraversal instanceof TrueTraversal) {
            emit = Boolean.TRUE;
        } else if (emitTraversal instanceof DefaultGraphTraversal) {
            emit = Boolean.TRUE;
            DefaultGraphTraversal traversal = (DefaultGraphTraversal) emitTraversal;
            startEmitStep = traversal.getStartStep();
        } else {
            emit = Boolean.FALSE;
        }
        Integer relationShipStart = null;
        Integer relationShipEnd = null;
        // 循环终止条件解析
        if (untilTraversal != null) {
            if (untilTraversal instanceof LoopTraversal) {
                // .times() 或者 // .loops()
                LoopTraversal loopTraversal = (LoopTraversal) untilTraversal;
                // 最大循环次数
                long maxLoops = loopTraversal.getMaxLoops();
                if (emit) {
                    relationShipEnd = Integer.valueOf(String.valueOf(maxLoops));
                } else {
                    relationShipStart = Integer.valueOf(String.valueOf(maxLoops));
                    relationShipEnd = relationShipStart;
                }
                parserRepeat(repeatTraversal, relationShipStart, relationShipEnd);
                if (emit && startEmitStep != null) {
                    parserStep(startEmitStep, relationShipStart, relationShipEnd);
                }
            } else if (untilTraversal instanceof DefaultGraphTraversal) {
                DefaultGraphTraversal graphTraversal = (DefaultGraphTraversal) untilTraversal;
                Step startStep = graphTraversal.getStartStep();
                relationShipStart = 0;
//            boolean untilFirst = repeatStep.untilFirst;
                // .until().repeat()，Neo4J 暂时不支持该操作，所以都当成 .repeat().until() 处理
                // .repeat().until()
                parserRepeat(repeatTraversal, relationShipStart, relationShipEnd);
                while (startStep != null) {
                    parserStep(startStep, relationShipStart, relationShipEnd);
                    startStep = startStep.getNextStep();
                    if (startStep instanceof RepeatStep.RepeatEndStep || startStep instanceof EmptyStep) {
                        break;
                    }
                }
            } else {
                throw new GremlinParserException("Neo4J 无法解析 until 类型 [" + untilTraversal.getClass().getName() + "]");
            }
        } else {
            if (emit) {
                relationShipStart = 1;
            } else {
                relationShipStart = 1;
                relationShipEnd = 0;
            }
            parserRepeat(repeatTraversal, relationShipStart, relationShipEnd);
            if (emit && startEmitStep != null) {
                parserStep(startEmitStep, relationShipStart, relationShipEnd);
            }
        }
    }

    /**
     * 解析 .repeat() 内容
     *
     * @param repeatTraversal
     * @param relationShipStart
     * @param relationShipEnd
     */
    private void parserRepeat(Traversal.Admin repeatTraversal, Integer relationShipStart, Integer relationShipEnd) {
        // 重复操作解析
        Step step = repeatTraversal.getStartStep();
        while (step != null) {
            parserStep(step, relationShipStart, relationShipEnd);
            step = step.getNextStep();
            if (step instanceof RepeatStep.RepeatEndStep || step instanceof EmptyStep) {
                break;
            }
        }
    }

    /**
     * 解析 hasNot
     *
     * @param step
     */
    private void notStep(NotStep step) {
        try {
            Field notTraversal = NotStep.class.getDeclaredField("notTraversal");
            notTraversal.setAccessible(true);
            Traversal.Admin traversal = (Traversal.Admin) notTraversal.get(step);
            List<Step> steps = traversal.getSteps();
            for (Step subStep : steps) {
                if (subStep instanceof PropertiesStep) {
                    PropertiesStep propertiesStep = (PropertiesStep) subStep;
                    String[] propertyKeys = propertiesStep.getPropertyKeys();
                    if (propertyKeys.length == 1) {
                        this.wheres.add(CypherWhere.builder().op(CypherRel.IS_NULL).key(getKey(current, propertyKeys[0])).build());
                    } else {
                        List<CypherWhere> whereList = new ArrayList<>(propertyKeys.length);
                        CypherWhere op = CypherWhere.builder().value(whereList).op(CypherRel.AND).build();
                        for (String propertyKey : propertyKeys) {
                            whereList.add(CypherWhere.builder().op(CypherRel.IS_NULL).key(getKey(current, propertyKey)).build());
                        }
                        this.wheres.add(op);
                    }
                } else {
                    throw new GremlinParserException("Neo4J 无法解析当前 NotStep 对象内部的子步骤: [" + subStep.getClass().getName() + "]");
                }
            }
        } catch (NoSuchFieldException e) {
            throw new GremlinParserException("Neo4J 解析 [NotStep] 获取不到 [notTraversal] 属性!");
        } catch (IllegalAccessException e) {
            throw new GremlinParserException("Neo4J 解析 [NotStep] 获取 [notTraversal] 属性异常!", e);
        }
    }

    /**
     * 解析 TraversalFilterStep 对象
     * .has(propertyKey)
     *
     * @param step
     */
    private void traversalFilterStep(TraversalFilterStep step) {
        List<Step> steps = step.getFilterTraversal().getSteps();
        for (Step subStep : steps) {
            if (subStep instanceof PropertiesStep) {
                PropertiesStep propertiesStep = (PropertiesStep) subStep;
                String[] propertyKeys = propertiesStep.getPropertyKeys();
                if (propertyKeys.length == 1) {
                    this.wheres.add(CypherWhere.builder().op(CypherRel.NOT_NULL).key(getKey(current, propertyKeys[0])).build());
                } else {
                    List<CypherWhere> whereList = new ArrayList<>(propertyKeys.length);
                    CypherWhere op = CypherWhere.builder().value(whereList).op(CypherRel.AND).build();
                    for (String propertyKey : propertyKeys) {
                        whereList.add(CypherWhere.builder().op(CypherRel.NOT_NULL).key(getKey(current, propertyKey)).build());
                    }
                    this.wheres.add(op);
                }
            } else {
                throw new GremlinParserException("Neo4J 无法解析当前 TraversalFilterStep 对象内部的子步骤: [" + subStep.getClass().getName() + "]");
            }
        }
    }

    /**
     * .tail() 获取倒数的指定个数元素
     *
     * @param step
     */
    private void tailGlobalStep(TailGlobalStep step) {
        throw new GremlinParserException("Neo4J 暂不支持 .tail() 操作");
    }

    /**
     * .range(skip, limit)
     *
     * @param step
     */
    private void rangeGlobalStep(RangeGlobalStep step) {
        long lowRange = step.getLowRange();
        long highRange = step.getHighRange();
        CypherPage.CypherPageBuilder pageBuilder = CypherPage.builder().skip(lowRange);

        if (highRange >= 0) {
            pageBuilder.limit(highRange - lowRange);
        }

        this.page = pageBuilder.build();
    }

    /**
     * .count()
     *
     * @param step
     */
    private void countGlobalStep(CountGlobalStep step) {
        Step previousStep = step.getPreviousStep();
        if (previousStep instanceof PropertiesStep) {
            // .properties().count() 统计属性数场景
            utilCQL.add(UtilFunction.builder().type(UtilFunction.UtilType.UNWIND)
                    .key(getKeys(current))
                    .as("allKeys").build());
            returns.add("count(allKeys)");
        } else {
            returns.add("count(" + current.getAs() + ")");
        }
    }

    /**
     * 解析 .otherV()
     *
     * @param step
     */
    private void edgeOtherVertexStep(EdgeOtherVertexStep step) {
        Direction direction;
        if (current instanceof CypherRelationship) {
            CypherRelationship cypherRelationship = (CypherRelationship) current;
            GraphDirection graphDirection = cypherRelationship.getDirection();
            switch (graphDirection) {
                case UNDEFINED:
                case BOTH:
                    direction = Direction.BOTH;
                    break;
                case IN:
                    direction = Direction.OUT;
                    break;
                case OUT:
                    direction = Direction.IN;
                    break;
                default:
                    throw new GremlinParserException("Neo4J 关系操作解析无法识别 [" + graphDirection.name() + "]");
            }
        } else {
            throw new GremlinParserException("Neo4J 关系操作解析不支持 V().outV()/inV()/bothV()/otherV() 场景");
        }
        // 当前是节点操作
        conversionToV(direction, step, null, null);
    }

    /**
     * 校验是否是最后步骤，如果是返回返回类型
     *
     * @param step
     * @return
     */
    private ReturnType checkEndStep(Step step) {
        if (step.getNextStep() instanceof EmptyStep) {
            return getReturnType(step);
        } else {
            return null;
        }
    }

    /**
     * 解析 .values()/.properties()
     *
     * @param step
     */
    private void propertiesStep(PropertiesStep step) {
        // .properties().count() 情况下，properties 不作为输出结果
        if (!(step.getNextStep() instanceof EmptyStep) && step.getNextStep() instanceof CountGlobalStep) {
            return;
        }
        PropertyType returnType = step.getReturnType();
        String[] propertyKeys = step.getPropertyKeys();
        if (PropertyType.VALUE.equals(returnType)) {
            if (ArrayUtil.isNotEmpty(propertyKeys)) {
                for (String propertyKey : propertyKeys) {
                    returns.add(getKey(current, propertyKey));
                }
            } else {
                returns.add(getProperties(current));
            }
        } else {
            if (ArrayUtil.isNotEmpty(propertyKeys)) {
                returns.add(getId(current.getAs()));
                if (current instanceof CypherNode) {
                    returns.add(nodeLabel(current.getAs()));
                } else {
                    returns.add(relationshipLabel(current.getAs()));
                }
                for (String propertyKey : propertyKeys) {
                    returns.add(getKey(current, propertyKey));
                }
            } else {
                returns.add(current.getAs());
            }
        }

    }

    /**
     * 解析 .id
     *
     * @param step
     */
    private void idStep(IdStep step) {
        returns.add(getId(current.getAs()));
    }

    /**
     * 解析 .label()
     *
     * @param step
     */
    private void labelStep(LabelStep step) {
        if (current instanceof CypherNode) {
            returns.add(nodeLabel(current.getAs()));
        } else {
            returns.add(relationshipLabel(current.getAs()));
        }
    }


    /**
     * 根据结束 step 的上一个 step 类型判断返回类型
     *
     * @param endStep
     * @return
     */
    private ReturnType getReturnType(Step endStep) {
        ReturnType returnType;
        if (endStep instanceof PropertyMapStep) {
            String[] propertyKeys = ((PropertyMapStep) endStep).getPropertyKeys();
            if (ArrayUtil.isNotEmpty(propertyKeys)) {
                returnType = ReturnType.VALUE_MAP.getReturnType();
            } else {
                returnType = ReturnType.VALUE_MAP_ALL.getReturnType();
            }
        } else if (endStep instanceof PathStep) {
            List<ReturnType> returnNode = new ArrayList<>();
            CypherObject cypherObject = node;
            while (cypherObject != null) {
                if (StringUtils.isNotBlank(cypherObject.getAs())) {
                    if (cypherObject instanceof CypherNode) {
                        returnNode.add(ReturnType.V.getReturnType());
                    } else {
                        returnNode.add(ReturnType.E.getReturnType());
                    }
                }
                cypherObject = cypherObject.getNextStep();
            }
            returnType = ReturnType.PATH.getReturnType(returnNode);
        } else if (endStep instanceof PropertiesStep) {
            PropertiesStep propertiesStep = (PropertiesStep) endStep;
            String[] propertyKeys = propertiesStep.getPropertyKeys();
            PropertyType type = propertiesStep.getReturnType();
            if (PropertyType.VALUE.equals(type)) {
                if (ArrayUtil.isNotEmpty(propertyKeys)) {
                    returnType = ReturnType.VALUES.getReturnType();
                } else {
                    returnType = ReturnType.VALUES_ALL.getReturnType();
                }
            } else {
                if (ArrayUtil.isNotEmpty(propertyKeys)) {
                    returnType = ReturnType.PROPERTIES.getReturnType();
                } else {
                    returnType = ReturnType.PROPERTIES_ALL.getReturnType();
                }
            }
        } else if (endStep instanceof LabelStep) {
            returnType = ReturnType.LABEL.getReturnType();
        } else if (endStep instanceof EdgeVertexStep && ((EdgeVertexStep) endStep).getDirection().equals(Direction.BOTH)) {
            returnType = ReturnType.BOTH_V.getReturnType();
        } else if (endStep instanceof CountGlobalStep) {
            returnType = ReturnType.COUNT.getReturnType();
        } else if (endStep instanceof HasStep) {
            HasStep hasStep = (HasStep) endStep;
            returnType = getReturnType(hasStep.getPreviousStep());
        } else {
            if (current instanceof CypherNode) {
                returnType = ReturnType.V.getReturnType();
            } else {
                returnType = ReturnType.E.getReturnType();
            }
        }
        return returnType;
    }

    /**
     * 生成 Cypher
     *
     * @return
     */
    private String generateCypher(Map<String, Object> params) {
        String match = generateMatch();
        String where = generateWhere(params);
        String utilCQL = generateUtil();
        String aReturn = generateReturn();
        StringBuilder cypher = new StringBuilder();
        cypher.append("MATCH ");
        if (returnType instanceof ReturnType.PATH) {
            cypher.append("path = ");
        }
        cypher.append(match);

        if (StringUtils.isNotBlank(where)) {
            cypher.append("WHERE ").append(where);
        }

        if (StringUtils.isNotBlank(utilCQL)) {
            cypher.append(utilCQL);
        }

        cypher.append("RETURN ").append(aReturn);

        if (page != null) {
            if (page.getSkip() != null) {
                cypher.append(" SKIP ").append(page.getSkip());
            }
            if (page.getLimit() != null) {
                cypher.append(" LIMIT ").append(page.getLimit());
            }
        }
        return cypher.toString();
    }

    /**
     * 解析生成 Neo4J 工具语法使用
     * unwind、foreach
     *
     * @return
     */
    private String generateUtil() {
        if (utilCQL.size() > 0) {
            StringBuilder sb = new StringBuilder();
            for (UtilFunction util : utilCQL) {
                sb.append(util.getType().name()).append(" ")
                        .append(util.getKey())
                        .append(" as ")
                        .append(util.getAs())
                        .append(" ");
            }
            return sb.toString();
        }
        return null;
    }

    /**
     * 生成 MATCH
     *
     * @return
     */
    private String generateMatch() {
        StringBuilder match = new StringBuilder();
        CypherObject object = node;

        while (object != null) {
            String as = object.getAs();
            List<String> labels = object.getLabel();
            if (object instanceof CypherNode) {
                // 当前遍历为节点
                CypherNode cypherNode = (CypherNode) object;
                object = cypherNode.getNextStep();
                match.append("(").append(as);
                if (CollectionUtil.isNotEmpty(labels)) {
                    match.append(":");
                    for (String label : labels) {
                        match.append(label).append("|");
                    }
                    match.deleteCharAt(match.length() - 1);
                }
                match.append(") ");
            } else {
                // 当前遍历为关系
                CypherRelationship relationship = (CypherRelationship) object;
                GraphDirection direction = relationship.getDirection();
                object = relationship.getNextStep();
                Integer start = relationship.getStart();
                Integer end = relationship.getEnd();
                // 表示开始为关系，前面需要先添加起始节点
                if (relationship.getPreviousStep() == null) {
                    match.append("(_start) ");
                }

                // 解析关系箭头方向
                switch (direction) {
                    case BOTH:
                    case OUT:
                    case UNDEFINED:
                        match.append("- ");
                        break;
                    case IN:
                        match.append("<- ");
                }

                // 拼接关系别名
                match.append("[").append(as);
                // 拼接关系标签
                if (CollectionUtil.isNotEmpty(labels)) {
                    match.append(":");
                    for (String label : labels) {
                        match.append(label).append("|");
                    }
                    match.deleteCharAt(match.length() - 1);
                }
                // 判断关系层级
                if (start != null || end != null) {
                    if (start != null && end != null) {
                        match.append("*").append(start).append("..").append(end).append(" ");
                    } else if (start != null && end == null) {
                        match.append("*").append(start).append("..").append(" ");
                    } else if (start == null && end != null) {
                        match.append("*").append("..").append(end).append(" ");
                    }
                }
                match.append("]");
                // 解析关系箭头方向
                switch (direction) {
                    case BOTH:
                    case IN:
                        match.append("- ");
                        break;
                    case OUT:
                    case UNDEFINED:
                        match.append("-> ");
                }
                if (relationship.getNextStep() == null) {
                    match.append("(_end) ");
                }
            }
        }
        return match.toString();
    }

    /**
     * 生成 where
     *
     * @return
     */
    private String generateWhere(Map<String, Object> params) {
        return generateBoolean(wheres, params, CypherRel.AND);
    }

    /**
     * 生成 return
     *
     * @return
     */
    private String generateReturn() {
        StringBuilder returnSb = new StringBuilder();
        if (CollectionUtil.isNotEmpty(returns)) {
            for (String aReturn : returns) {
                returnSb.append(aReturn).append(",");
            }
            returnSb.deleteCharAt(returnSb.length() - 1);
        } else {
            // 若无指定返回内容，则关系情况下，返回与当前关系相关的两个节点
            if (current instanceof CypherRelationship) {
                CypherRelationship relationship = (CypherRelationship) current;
                String previousNodeAs;
                CypherNode previousNode = relationship.getPreviousStep();
                if (previousNode != null) {
                    previousNodeAs = previousNode.getAs();
                } else {
                    previousNodeAs = "_start";
                }

                String nextNodeAs;
                CypherNode nextNode = relationship.getNextStep();
                if (nextNode != null) {
                    nextNodeAs = nextNode.getAs();
                } else {
                    nextNodeAs = "_end";
                }
                returnSb.append(previousNodeAs)
                        .append(",")
                        .append(relationship.getAs())
                        .append(",")
                        .append(nextNodeAs);
            } else {
                // 返回与当前节点自己
                if (returnType != null && returnType instanceof ReturnType.BOTH_V) {
                    CypherRelationship relationship = ((CypherNode) current).getPreviousStep();
                    CypherNode previousStep = relationship.getPreviousStep();
                    returnSb.append(previousStep.getAs()).append(",");
                }
                returnSb.append(current.as);
            }
        }
        return returnSb.toString();
    }

    /**
     * 解析 布尔运算符
     *
     * @param wheres
     * @param params
     * @param parentBool
     * @return
     */
    private String generateBoolean(List<CypherWhere> wheres, Map<String, Object> params, CypherRel parentBool) {
        StringBuilder whereSb = new StringBuilder();
        Iterator<CypherWhere> iterator = wheres.iterator();
        // 迭代器
        while (iterator.hasNext()) {
            CypherWhere where = iterator.next();
            String key = where.getKey();
            Object value = where.getValue();
            CypherRel op = where.getOp();
            if (op.equals(CypherRel.AND) || op.equals(CypherRel.OR)) {
                String generateBoolean = generateBoolean((List<CypherWhere>) value, params, op);
                if (iterator.hasNext()) {
                    whereSb.append("(").append(StrUtil.trim(generateBoolean)).append(") ").append(parentBool.getType()).append(" ");
                } else {
                    whereSb.append(generateBoolean);
                }
            } else {
                generateComparison(whereSb, key, value, op, params);
                if (iterator.hasNext()) {
                    whereSb.append(parentBool.getType());
                }
                whereSb.append(" ");
            }
        }
        return whereSb.toString();
    }

    /**
     * 解析比较运算符
     *
     * @param whereSb
     * @param key
     * @param value
     * @param op
     * @return
     */
    private void generateComparison(StringBuilder whereSb, String key, Object value, CypherRel op, Map<String, Object> params) {
        whereSb.append(key).append(" ");
        switch (op) {
            case IN:
                whereSb.append("IN [");
                if (value.getClass().isArray() && !(value instanceof byte[])) {
                    Object[] values = (Object[]) value;
                    for (Object o : values) {
                        whereSb.append(getValue(key, o, params)).append(",");
                    }
                    whereSb = whereSb.delete(whereSb.length() - 1, whereSb.length());
                } else if (value instanceof List) {
                    List values = (List) value;
                    for (Object o : values) {
                        whereSb.append(getValue(key, o, params)).append(",");
                    }
                    whereSb = whereSb.delete(whereSb.length() - 1, whereSb.length());
                } else {
                    whereSb.append(getValue(key, value, params));
                }
                whereSb.append("] ");
                break;
            case EQ:
                whereSb.append("= ").append(getValue(key, value, params)).append(" ");
                break;
            case NE:
                whereSb.append("<> ").append(getValue(key, value, params)).append(" ");
                break;
            case LT:
                whereSb.append("< ").append(getValue(key, value, params)).append(" ");
                break;
            case LTE:
                whereSb.append("<= ").append(getValue(key, value, params)).append(" ");
                break;
            case GTE:
                whereSb.append(">= ").append(getValue(key, value, params)).append(" ");
                break;
            case GT:
                whereSb.append("> ").append(getValue(key, value, params)).append(" ");
                break;
            case NOT_NULL:
                whereSb.append("IS NOT NULL ");
                break;
            case IS_NULL:
                whereSb.append("IS NULL ");
                break;
            default:
                throw new GremlinParserException("Neo4J 无法解析当前操作符 [" + op.name() + "]");
        }
    }

    /**
     * 获取占位符
     *
     * @param key
     * @return
     */
    private String getValue(String key, Object value, Map<String, Object> params) {
        key = StringUtils.replaceEach(key, new String[]{"(", ")", "."}, new String[]{"_", "", "_"});
        String _key = key + "_" + params.size();
        params.put(_key, value);
        return "$" + _key;
    }

    /**
     * 图节点/边步骤解析
     *
     * @param graphStep
     */
    private void graphStep(GraphStep graphStep) {
        Class returnClass = graphStep.getReturnClass();
        Object[] ids = graphStep.getIds();
        // 当前表示检索节点
        if (returnClass.equals(Vertex.class)) {
            // 当前不为起始节点
            if (current != null) {
                if (current instanceof CypherNode) {
                    // 节点
                    throw new GremlinParserException("请检查 V().V() 操作是否合理!");
                } else {
                    // 关系边
                    CypherRelationship relationship = (CypherRelationship) current;
                    current = relationship.createNextNode().build();
                }
            } else {
                // 创建当前起始节点
                current = CypherNode.builder().build();
                node = (CypherNode) current;
            }
        } else if (returnClass.equals(Edge.class)) {
            // 检索边
            // 当前不为起始节点
            if (current != null) {
                if (current instanceof CypherNode) {
                    // 节点
                    CypherNode node = (CypherNode) current;
                    current = node.createNextRelationship().direction(GraphDirection.UNDEFINED).build();
                } else {
                    // 关系边
                    throw new GremlinParserException("请检查 E().E() 操作是否合理!");
                }
            } else {
                // 创建当前起始边
                current = CypherRelationship.builder().direction(GraphDirection.UNDEFINED).build();
                node = current;
            }
        }
        // 判断是否有 id 条件
        if (ids != null && ids.length > 0) {
            CypherWhere cypherWhere;
            // 若存在 id 则新增条件
            if (ids.length > 1) {
                cypherWhere = CypherWhere.builder().key(getId(current.getAs())).op(CypherRel.IN).value(ids).build();
            } else {
                cypherWhere = CypherWhere.builder().key(getId(current.getAs())).op(CypherRel.EQ).value(ids[0]).build();
            }
            wheres.add(cypherWhere);
        }
    }

    /**
     * has() 条件解析
     *
     * @param hasStep
     */
    private void hasStep(HasStep hasStep) {
        List<HasContainer> hasContainers = hasStep.getHasContainers();
        List<CypherWhere> whereList = new ArrayList<>();
        for (HasContainer hasContainer : hasContainers) {
            String key = hasContainer.getKey();
            P<?> predicate = hasContainer.getPredicate();
            if (StringUtils.equals(key, "~id")) {
                BiPredicate<?, ?> biPredicate = predicate.getBiPredicate();
                if (Contains.within.equals(biPredicate)) {
                    Contains contains = (Contains) biPredicate;
                    List<Object> values = (List<Object>) predicate.getValue();
                    values = values.stream().flatMap(value -> {
                        if (value instanceof String) {
                            return Stream.of(Long.parseLong((String) value));
                        } else {
                            return Stream.of(value);
                        }
                    }).collect(Collectors.toList());
                    whereList.add(CypherWhere.builder().key(getId(current.getAs())).op(parserRel(contains)).value(values).build());
                } else if (Compare.eq.equals(biPredicate)) {
                    Compare compare = (Compare) biPredicate;
                    Object value = predicate.getValue();
                    if (value instanceof String) {
                        value = Long.parseLong((String) value);
                    }
                    CypherWhere where = CypherWhere.builder().key(getId(current.getAs())).op(parserRel(compare)).value(value).build();
                    whereList.add(where);
                } else {
                    throw new GremlinParserException("Neo4J 无法解析当前操作 [" + biPredicate.getClass().getName() + "]");
                }
            } else if (StringUtils.equals(key, "~label")) {
                // has(label) 场景
                BiPredicate<?, ?> biPredicate = predicate.getBiPredicate();
                if (Contains.within.equals(biPredicate)) {
                    current.setLabel((List<String>) predicate.getValue());
                } else if (Compare.eq.equals(biPredicate)) {
                    current.setLabel(CollectionUtil.newArrayList(predicate.getValue()));
                } else {
                    throw new GremlinParserException("Neo4J 无法解析当前操作 [" + biPredicate.getClass().getName() + "]");
                }
            } else if (StringUtils.equals(key, "~key")) {
                // .hasKey
                BiPredicate<?, ?> biPredicate = predicate.getBiPredicate();
                if (Contains.within.equals(biPredicate)) {
                    List<String> values = (List<String>) predicate.getValue();
                    List<CypherWhere> andWhere = new ArrayList<>(values.size());
                    for (String value : values) {
                        andWhere.add(CypherWhere.builder().key(getKey(current, value)).op(CypherRel.NOT_NULL).build());
                    }
                    whereList.add(CypherWhere.builder().op(CypherRel.AND).value(andWhere).build());
                } else if (Compare.eq.equals(biPredicate)) {
                    whereList.add(CypherWhere.builder().key(getKey(current, (String) predicate.getValue())).op(CypherRel.NOT_NULL).build());
                } else {
                    throw new GremlinParserException("Neo4J 无法解析当前操作 [" + biPredicate.getClass().getName() + "]");
                }
            } else {
                BiPredicate<?, ?> biPredicate = predicate.getBiPredicate();
                if (biPredicate instanceof Contains) {
                    Contains contains = (Contains) predicate.getBiPredicate();
                    CypherWhere where = CypherWhere.builder().key(getKey(current, key))
                            .op(parserRel(contains))
                            .value(predicate.getValue())
                            .build();
                    whereList.add(where);
                } else if (biPredicate instanceof Compare) {
                    Compare compare = (Compare) predicate.getBiPredicate();
                    CypherWhere where = CypherWhere.builder().key(getKey(current, key))
                            .op(parserRel(compare))
                            .value(predicate.getValue())
                            .build();
                    whereList.add(where);
                } else {
                    throw new GremlinParserException("Neo4J 无法解析当前操作 [" + biPredicate.getClass().getName() + "]");
                }
            }
        }
        if (CollectionUtil.isNotEmpty(whereList)) {
            if (whereList.size() == 1) {
                wheres.add(whereList.get(0));
            } else {
                CypherWhere where = CypherWhere.builder().op(CypherRel.AND)
                        .value(whereList)
                        .build();
                wheres.add(where);
            }
        }
    }

    /**
     * 节点到边操作解析
     *
     * @param vertexStep
     * @param relationShipStart 关系遍历起始层级
     * @param relationShipEnd   关系遍历终止层级
     */
    private void vertexStep(VertexStep vertexStep, Integer relationShipStart, Integer relationShipEnd) {
        String[] edgeLabels = vertexStep.getEdgeLabels();
        Direction direction = vertexStep.getDirection();
        Class returnClass = vertexStep.getReturnClass();
        // 解析关系
        if (returnClass.equals(Edge.class)) {
            // 当前是节点操作
            if (current instanceof CypherNode) {
                current = ((CypherNode) current).createNextRelationship().direction(parserDirection(direction)).start(relationShipStart).end(relationShipEnd).build();
            } else {
                throw new GremlinParserException("Neo4J 无法解析边到边关系, 请检查语法是否错误");
                // 当前操作是关系
//                CypherRelationship currentR = (CypherRelationship) this.current;
//                CypherNode node = currentR.createNextNode().build();
//                this.current = node.createNextRelationship().direction(parserDirection(direction)).build();

            }
        } else if (returnClass.equals(Vertex.class)) {
            // 当前是节点操作
            conversionToV(direction, vertexStep, relationShipStart, relationShipEnd);
        } else {
            throw new GremlinParserException("Neo4J 无法解析当前返回类型 [" + returnClass.getName() + "]");
        }

        // 判断是否过滤标签
        if (ArrayUtil.isNotEmpty(edgeLabels)) {
            // 对象别名
            String as;
            if (current instanceof CypherRelationship) {
                as = current.getAs();
            } else {
                as = ((CypherNode) current).getPreviousStep().getAs();
            }
            CypherWhere cypherWhere;
            if (edgeLabels.length > 1) {
                cypherWhere = CypherWhere.builder().key(relationshipLabel(as)).op(CypherRel.IN).value(edgeLabels).build();
            } else {
                cypherWhere = CypherWhere.builder().key(relationshipLabel(as)).op(CypherRel.EQ).value(edgeLabels[0]).build();
            }
            wheres.add(cypherWhere);
        }
    }

    /**
     * 构建 toV 节点
     *
     * @param direction
     */
    private void conversionToV(Direction direction, Step step, Integer relationShipStart, Integer relationShipEnd) {
        if (current instanceof CypherNode) {
            if (step instanceof EdgeVertexStep || step instanceof EdgeOtherVertexStep) {
                throw new GremlinParserException("Neo4J 关系操作解析不支持 V().outV()/inV()/bothV()/otherV() 场景");
            }
            CypherRelationship relationship = ((CypherNode) current).createNextRelationship().hidden(true).direction(parserDirection(direction))
                    .start(relationShipStart).end(relationShipEnd).build();
            current = relationship.createNextNode().build();
        } else {
            // 当前操作是关系
            CypherRelationship cypherRelationship = (CypherRelationship) this.current;
            GraphDirection graphDirection = cypherRelationship.getDirection();
            switch (graphDirection) {
                case BOTH:
                    if (direction.equals(Direction.BOTH)) {
                        cypherRelationship.setStart(relationShipStart);
                        cypherRelationship.setEnd(relationShipEnd);
                        this.current = cypherRelationship.createNextNode().build();
                        break;
                    }
                    createVtoE(direction, cypherRelationship, relationShipStart, relationShipEnd);
                    break;
                case OUT:
                    if (direction.equals(Direction.IN)) {
                        cypherRelationship.setStart(relationShipStart);
                        cypherRelationship.setEnd(relationShipEnd);
                        this.current = cypherRelationship.createNextNode().build();
                        break;
                    }
                    createVtoE(direction, cypherRelationship, relationShipStart, relationShipEnd);
                    break;
                case IN:
                    if (direction.equals(Direction.OUT)) {
                        cypherRelationship.setStart(relationShipStart);
                        cypherRelationship.setEnd(relationShipEnd);
                        this.current = cypherRelationship.createNextNode().build();
                        break;
                    }
                    createVtoE(direction, cypherRelationship, relationShipStart, relationShipEnd);
                    break;
                case UNDEFINED:
                    this.current = cypherRelationship.setStart(relationShipStart).setEnd(relationShipEnd).setDirection(parserDirection(direction)).createNextNode().build();
                    break;
                default:
                    createVtoE(direction, cypherRelationship, relationShipStart, relationShipEnd);
            }
        }
    }

    /**
     * 创建 V - E -> V
     *
     * @param direction
     * @param cypherRelationship
     * @param relationShipStart
     * @param relationShipEnd
     */
    private void createVtoE(Direction direction, CypherRelationship cypherRelationship, Integer relationShipStart, Integer relationShipEnd) {
        CypherNode cypherNode = cypherRelationship.createNextNode().build();
        CypherRelationship relationship = cypherNode.createNextRelationship().direction(parserDirection(direction))
                .start(relationShipStart).end(relationShipEnd).build();
        this.current = relationship.createNextNode().build();
    }

    /**
     * 解析 .path()
     *
     * @param pathStep
     */
    private void pathStep(PathStep pathStep) {
        returns.add("path");
    }

    /**
     * 解析 .outV/inV/bothV(edgeLabel...)
     *
     * @param step
     * @param relationShipStart
     * @param relationShipEnd
     */
    private void edgeVertexStep(EdgeVertexStep step, Integer relationShipStart, Integer relationShipEnd) {
        Direction direction = step.getDirection();
        // 当前是节点操作
        conversionToV(direction, step, relationShipStart, relationShipEnd);
    }

    /**
     * .valueMap 解析
     *
     * @param step
     */
    private void propertyMapStep(PropertyMapStep step) {
        String[] propertyKeys = step.getPropertyKeys();
        if (ArrayUtil.isNotEmpty(propertyKeys)) {
            for (String propertyKey : propertyKeys) {
                returns.add(getKey(current, propertyKey));
            }
        } else {
            returns.add(getProperties(current));
        }
    }

    /**
     * 解析方向
     *
     * @return
     */
    private GraphDirection parserDirection(Direction direction) {
        switch (direction) {
            case IN:
                return GraphDirection.IN;
            case OUT:
                return GraphDirection.OUT;
            case BOTH:
                return GraphDirection.BOTH;
            default:
                throw new GremlinParserException("Neo4J 关系操作解析异常 [" + direction.name() + "]");
        }
    }

    /**
     * 获取条件KEY
     *
     * @param as
     * @return
     */
    private String getId(String as) {
        return "id(" + as + ")";
    }

    /**
     * 节点标签
     *
     * @param as
     * @return
     */
    private String nodeLabel(String as) {
        return "labels(" + as + ")";
    }

    /**
     * 关系标签条件
     *
     * @param as
     * @return
     */
    private String relationshipLabel(String as) {
        return "type(" + as + ")";
    }

    /**
     * 获取条件KEY
     *
     * @param object
     * @param field
     * @return
     */
    private String getKey(CypherObject object, String field) {
        return object.getAs() + "." + field;
    }

    /**
     * 获取节点的所有属性
     *
     * @param object
     * @return
     */
    private String getProperties(CypherObject object) {
        return "properties(" + object.getAs() + ")";
    }

    /**
     * 获取节点所有属性名称
     *
     * @param object
     * @return
     */
    private String getKeys(CypherObject object) {
        return "keys(" + object.getAs() + ")";
    }

    /**
     * 解析操作符
     *
     * @param compare
     * @return
     */
    private CypherRel parserRel(Compare compare) {
        switch (compare) {
            case eq:
                return CypherRel.EQ;
            case gt:
                return CypherRel.GT;
            case lt:
                return CypherRel.LT;
            case gte:
                return CypherRel.GTE;
            case lte:
                return CypherRel.LTE;
            case neq:
                return CypherRel.NE;
            default:
                throw new GremlinParserException("Neo4J 无法解析当前操作符 [" + compare.name() + "]");
        }
    }

    /**
     * 解析操作符
     *
     * @param contains
     * @return
     */
    private CypherRel parserRel(Contains contains) {
        switch (contains) {
            case within:
                return CypherRel.IN;
            default:
                throw new GremlinParserException("Neo4J 无法解析当前操作符 [" + contains.name() + "]");
        }
    }

}
