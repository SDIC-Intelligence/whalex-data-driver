package com.meiya.whalex.util;

import com.meiya.whalex.db.entity.*;
import com.meiya.whalex.db.entity.graph.GraphBaseConstant;
import com.meiya.whalex.db.entity.graph.GraphHas;
import com.meiya.whalex.db.entity.graph.GremlinFunction;
import com.meiya.whalex.interior.db.search.in.Page;
import com.meiya.whalex.interior.db.search.in.QueryParamCondition;
import com.meiya.whalex.interior.db.search.in.Where;
import com.meiya.whalex.util.collection.CollectionUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 提取gremlin公共语法至抽象包中
 *
 * @author chenjp
 * @date 3/4/2021
 */
@Slf4j
public final class GremlinUtil implements GremlinFunction, GraphBaseConstant {

    private static final String DOT = ".";
    private static final String COMMA = ",";
    private static final String RIGHT_BRACKET = ")";
    private static final String SINGLE_QUOTE = "'";

    private GremlinUtil() {

    }


    /**
     * 查询
     */
    public static String doQuery(QueryParamCondition queryParamCondition) {
        List<Where> queryParams = queryParamCondition.getWhere();
        StringBuilder gremlin = new StringBuilder("g");
        if (CollectionUtils.isEmpty(queryParams)) {
            gremlin.append(".V()");
        } else {
            buildGremlin(queryParams, gremlin);
            if (log.isDebugEnabled()) {
                log.debug("doQuery Gremlin：{}", gremlin);
            }
        }
//        gremlin.append(".valueMap()");
        return gremlin.toString();
    }

    /**
     * 构建gremlin语句
     *
     * @param queryParams 查询条件
     * @param gremlin     gremlin语句
     */
    private static void buildGremlin(List<Where> queryParams, StringBuilder gremlin) {
        buildGremlin(queryParams, gremlin, new AtomicBoolean(true));
    }

    /**
     * 构建gremlin语句
     *
     * @param queryParams 查询条件
     * @param gremlin     gremlin语句
     * @param shouldDot   是否需要.
     */
    private static void buildGremlin(List<Where> queryParams, StringBuilder gremlin, AtomicBoolean shouldDot) {
        queryParams.forEach(param -> {
            switch (param.getField()) {
                case V:
                    build(_V, gremlin, param);
                    break;
                case E:
                    build(_E, gremlin, param);
                    break;
                case HAS:
                    if (null != param.getParam()) {
                        build(_HAS, gremlin, param, shouldDot);
                    } else {
                        innerFunc(_HAS, gremlin, param, shouldDot);
                    }
                    break;
                case AS:
                    build(_AS, gremlin, param, shouldDot);
                    break;
                case IN_V:
                    build(_IN_V, gremlin, param, shouldDot);
                    break;
                case IN_E:
                    build(_IN_E, gremlin, param, shouldDot);
                    break;
                case OUT_V:
                    build(_OUT_V, gremlin, param, shouldDot);
                    break;
                case OUT_E:
                    build(_OUT_E, gremlin, param, shouldDot);
                    break;
                case BY:
                    if (null != param.getParam()) {
                        build(_BY, gremlin, param, shouldDot);
                    } else {
                        innerFunc(_BY, gremlin, param, shouldDot);
                    }
                    break;
                case ID:
                    build(_ID, gremlin, param, shouldDot);
                    break;
                case LABEL:
                    build(_LABEL, gremlin, param, shouldDot);
                    break;
                case PROPERTIES:
                    build(_PROPERTIES, gremlin, param, shouldDot);
                    break;
                case KEY:
                    build(_KEY, gremlin, param, shouldDot);
                    break;
                case VALUE:
                    build(_VALUE, gremlin, param, shouldDot);
                    break;
                case VALUES:
                    build(_VALUES, gremlin, param, shouldDot);
                    break;
                case VALUE_MAP:
                    build(_VALUE_MAP, gremlin, param, shouldDot);
                    break;
                case OUT:
                    build(_OUT, gremlin, param, shouldDot);
                    break;
                case IN:
                    build(_IN, gremlin, param, shouldDot);
                    break;
                // TODO
                case BOTH:
                    build(_BOTH, gremlin, param, shouldDot);
                    break;
                // TODO
                case BOTH_E:
                    build(_BOTH_E, gremlin, param, shouldDot);
                    break;
                // TODO
                case BOTH_V:
                    build(_BOTH_V, gremlin, param, shouldDot);
                    break;
                // TODO
                case OTHER_V:
                    build(_OTHER_V, gremlin, param, shouldDot);
                    break;
                // TODO
                case HAS_ID:
                    build(_HAS_ID, gremlin, param, shouldDot);
                    break;
                // TODO
                case HAS_LABEL:
                    build(_HAS_LABEL, gremlin, param, shouldDot);
                    break;
                // TODO
                case HAS_KEY:
                    build(_HAS_KEY, gremlin, param, shouldDot);
                    break;
                // TODO
                case HAS_VALUE:
                    build(_HAS_VALUE, gremlin, param, shouldDot);
                    break;
                // TODO
                case HAS_NOT:
                    build(_HAS_NOT, gremlin, param, shouldDot);
                    break;
                case COUNT:
                    build(_COUNT, gremlin, param, shouldDot);
                    break;
                case RANGE:
                    List<String> ranges = Arrays.asList(param.getParam().toString().split(COMMA));
                    section(_RANGE, gremlin, param, ranges, shouldDot);
                    break;
                case LIMIT:
                    build(_LIMIT, gremlin, param, shouldDot);
                    break;
                case TAIL:
                    build(_TAIL, gremlin, param, shouldDot);
                    break;
                case SKIP:
                    build(_SKIP, gremlin, param, shouldDot);
                    break;
                case PATH:
                    build(_PATH, gremlin, param, shouldDot);
                    break;
                case SIMPLE_PATH:
                    build(_SIMPLE_PATH, gremlin, param, shouldDot);
                    break;
                case CYCLIC_PATH:
                    build(_CYCLIC_PATH, gremlin, param);
                    break;
                //  TODO
                case REPEAT:
                    innerFunc(_REPEAT, gremlin, param, shouldDot);
                    break;
                case TIMES:
                    build(_TIMES, gremlin, param);
                    break;
                case UNTIL:
                    innerFunc(_UNTIL, gremlin, param, shouldDot);
                    break;
                case EMIT:
                    innerFunc(_EMIT, gremlin, param, shouldDot);
                    break;
                case _LOOPS:
                    build(_LOOPS, gremlin, param, shouldDot);
                    break;
                case ORDER:
                    build(_ORDER, gremlin, param, shouldDot);
                    break;
                case GROUP:
                    build(_GROUP, gremlin, param, shouldDot);
                    break;
                case GROUP_COUNT:
                    build(_GROUP_COUNT, gremlin, param, shouldDot);
                    break;
                case DEDUP:
                    build(_DEDUP, gremlin, param, shouldDot);
                    break;
                case WHERE:
                    innerFunc(_WHERE, gremlin, param, shouldDot);
                    break;
                // TODO 后续更新支持 lambda 表达式
                case FILTER:
                    if (null != param.getParam()) {
                        build(_FILTER, gremlin, param, shouldDot);
                    } else {
                        innerFunc(_FILTER, gremlin, param, shouldDot);
                    }
                    break;
                case IS:
                    if (null != param.getParam()) {
                        build(_IS, gremlin, param, shouldDot);
                    } else {
                        innerFunc(_IS, gremlin, param, shouldDot);
                    }
                    break;
                case AND:
                    build(_AND, gremlin, param, shouldDot);
                    break;
                case OR:
                    build(_OR, gremlin, param, shouldDot);
                    break;
                case NOT:
                    build(_NOT, gremlin, param, shouldDot);
                    break;
                case SUM:
                    build(_SUM, gremlin, param, shouldDot);
                    break;
                case MAX:
                    build(_MAX, gremlin, param, shouldDot);
                    break;
                case MIN:
                    build(_MIN, gremlin, param, shouldDot);
                    break;
                case MEAN:
                    build(_MEAN, gremlin, param, shouldDot);
                    break;
                case MATH:
                    build(_MATH, gremlin, param, shouldDot);
                    break;
                case AS_SELECT:
                    build(_AS_SELECT, gremlin, param, shouldDot);
                    break;
                case AS_WHERE:
                    build(_AS_WHERE, gremlin, param, shouldDot);
                    break;
                case AS_MATCH:
                    build(_AS_MATCH, gremlin, param, shouldDot);
                    break;
                case AS_DEDUP:
                    build(_AS_DEDUP, gremlin, param, shouldDot);
                    break;
                case CHOOSE:
                    build(_CHOOSE, gremlin, param, shouldDot);
                    break;
                case BRANCH:
                    build(_BRANCH, gremlin, param, shouldDot);
                    break;
                case COALESCE:
                    build(_COALESCE, gremlin, param, shouldDot);
                    break;
                case OPTIONAL:
                    build(_OPTIONAL, gremlin, param, shouldDot);
                    break;
                case UNION:
                    build(_UNION, gremlin, param, shouldDot);
                    break;
                case AGGREGATE:
                    build(_AGGREGATE, gremlin, param, shouldDot);
                    break;
                case STORE:
                    build(_STORE, gremlin, param, shouldDot);
                    break;
                case UNFOLD:
                    build(_UNFOLD, gremlin, param, shouldDot);
                    break;
                case FOLD:
                    build(_FOLD, gremlin, param, shouldDot);
                    break;
                // TODO 后续支持复杂语句
                case MATCH:
                    build(_MATCH, gremlin, param, shouldDot);
                    break;
                case SAMPLE:
                    build(_SAMPLE, gremlin, param, shouldDot);
                    break;
                case COIN:
                    build(_COIN, gremlin, param, shouldDot);
                    break;
                case CONSTANT:
                    build(_CONSTANT, gremlin, param, shouldDot);
                    break;
                case INJECT:
                    build(_INJECT, gremlin, param, shouldDot);
                    break;
                case SACK:
                    build(_SACK, gremlin, param, shouldDot);
                    break;
                case BARRIER:
                    build(_BARRIER, gremlin, param, shouldDot);
                    break;
                case LOCAL:
                    build(_LOCAL, gremlin, param, shouldDot);
                    break;
                case HAS_NEXT:
                    build(_HAS_NEXT, gremlin, param, shouldDot);
                    break;
                case NEXT:
                    build(_NEXT, gremlin, param, shouldDot);
                    break;
                case TRY_NEXT:
                    build(_TRY_NEXT, gremlin, param, shouldDot);
                    break;
                case TO_LIST:
                    build(_TO_LIST, gremlin, param, shouldDot);
                    break;
                case TO_SET:
                    build(_TO_SET, gremlin, param, shouldDot);
                    break;
                case TO_BULK_SET:
                    build(_TO_BULK_SET, gremlin, param, shouldDot);
                    break;
                case FILL:
                    build(_FILL, gremlin, param, shouldDot);
                    break;
                case ITERATE:
                    build(_ITERATE, gremlin, param, shouldDot);
                    break;
                case MAP:
                    build(_MAP, gremlin, param, shouldDot);
                    break;
                case FLATMAP:
                    build(_FLATMAP, gremlin, param, shouldDot);
                    break;
                case SIDE_EFFECT:
                    build(_SIDE_EFFECT, gremlin, param, shouldDot);
                    break;
                case PROFILE:
                    build(_PROFILE, gremlin, param, shouldDot);
                    break;
                case EXPLAIN:
                    build(_EXPLAIN, gremlin, param, shouldDot);
                    break;
                case EQ:
                    simpleFunc(_EQ, gremlin, param);
                    break;
                case NEQ:
                    simpleFunc(_NEQ, gremlin, param);
                    break;
                case NULL:
                    gremlin.append(SINGLE_QUOTE).append(param.getParam()).append(SINGLE_QUOTE);
                    break;
                case BETWEEN:
                    build(_BETWEEN, gremlin, param, shouldDot);
                    break;
                case SELECT:
                    build(_SELECT, gremlin, param, shouldDot);
                    break;
                case LT:
                    simpleFunc(_LT, gremlin, param);
                    break;
                case LTE:
                    simpleFunc(_LTE, gremlin, param);
                    break;
                case GT:
                    simpleFunc(_GT, gremlin, param);
                    break;
                case GTE:
                    simpleFunc(_GTE, gremlin, param);
                    break;
                case INSIDE:
                    List<String> insides = Arrays.asList(param.getParam().toString().split(COMMA));
                    section(_INSIDE, gremlin, param, insides, shouldDot);
                    break;
                case OUTSIDE:
                    List<String> outsides = Arrays.asList(param.getParam().toString().split(COMMA));
                    section(_OUTSIDE, gremlin, param, outsides, shouldDot);
                    break;
                case WITHIN:
                    simpleFunc(_WITHIN, gremlin, param);
                    break;
                case WITHOUT:
                    simpleFunc(_WITHOUT, gremlin, param);
                    break;
                case LOOPS:
                    build(_LOOPS, gremlin, param, shouldDot);
                    break;
                case INCR:
                    if (shouldDot.get()) {
                        appendDot(gremlin);
                    }
                    gremlin.append(_INCR);
                    break;
                case DECR:
                    if (shouldDot.get()) {
                        appendDot(gremlin);
                    }
                    gremlin.append(_DECR);
                    break;
                case SHUFFLE:
                    if (shouldDot.get()) {
                        appendDot(gremlin);
                    }
                    gremlin.append(_SHUFFLE);
                    break;
                case UNDER:
                    gremlin.append(_UNDER);
                    break;
                case LAB:
                    gremlin.append("label");
                    break;
                default:
                    break;
            }
        });
    }

    /**
     * 设置区间
     *
     * @param func      函数名
     * @param gremlin   gremlin语句
     * @param param     参数
     * @param list      value
     * @param shouldDot 是否需要.
     */
    private static void section(String func, StringBuilder gremlin,
                                Where param, List<String> list, AtomicBoolean shouldDot) {
        if (!list.isEmpty()) {
            if (shouldDot.get()) {
                appendDot(gremlin);
            }
            gremlin.append(func);
            for (int i = 0; i < list.size(); i++) {
                gremlin.append(Integer.parseInt(list.get(i)));
                if (i != list.size() - 1) {
                    gremlin.append(COMMA);
                }
            }
            gremlin.append(RIGHT_BRACKET);
        } else {
            build(func, gremlin, param);
        }
    }

    /**
     * 简单函数
     *
     * @param func    函数名
     * @param gremlin gremlin语句
     * @param param   参数
     */
    private static void simpleFunc(String func, StringBuilder gremlin, Where param) {
        gremlin.append(func);
        if (null != param.getParam()) {
            if (param.getParam() instanceof Integer) {
                gremlin.append(param.getParam());
            } else {
                gremlin.append(SINGLE_QUOTE).append(param.getParam()).append(SINGLE_QUOTE);
            }
        }
        gremlin.append(RIGHT_BRACKET);
    }

    /**
     * 函数内部的循环函数
     *
     * @param func      函数名
     * @param gremlin   gremlin语句
     * @param param     参数
     * @param shouldDot 是否需要.
     */
    private static void innerFunc(String func, StringBuilder gremlin, Where param, AtomicBoolean shouldDot) {
        List<Where> wheres = param.getParams();
        if (CollectionUtils.isNotEmpty(wheres)) {
            if (shouldDot.get()) {
                appendDot(gremlin);
            }
            gremlin.append(func);
            for (int i = 0, length = wheres.size(); i < length; i++) {
                if (StringUtils.isNotBlank(wheres.get(i).getField())) {
                    shouldDot.set(false);
                    buildGremlin(Collections.singletonList(wheres.get(i)), gremlin, shouldDot);
                    if (i != length - 1) {
                        if (!NULL.equals(wheres.get(i).getField())) {
                            gremlin.append(DOT);
                        } else {
                            gremlin.append(COMMA);
                        }
                    }
                }
            }
            shouldDot.set(true);
            gremlin.append(RIGHT_BRACKET);
        } else {
            if (shouldDot.get()) {
                appendDot(gremlin);
            }
            gremlin.append(func).append(RIGHT_BRACKET);
        }
    }

    private static void appendStrValue(String func, StringBuilder gremlin, Where param) {
        appendStrValue(func, gremlin, param, new AtomicBoolean(false));
    }

    private static void appendStrValue(String func, StringBuilder gremlin, Where param, AtomicBoolean shouldDot) {
        List<String> values = Arrays.asList(param.getParam().toString().split(COMMA));
        if (!values.isEmpty()) {
            if (shouldDot.get()) {
                appendDot(gremlin);
            }
            gremlin.append(func);
            for (int i = 0, length = values.size(); i < length; i++) {
                gremlin.append(SINGLE_QUOTE).append(values.get(i)).append(SINGLE_QUOTE);
                if (i != length - 1) {
                    gremlin.append(COMMA);
                }
            }
            gremlin.append(RIGHT_BRACKET);
        }
    }

    /**
     * 构建函数剩余部分内容
     */
    private static void build(String func, StringBuilder gremlin, Where param) {
        build(func, gremlin, param, new AtomicBoolean(true));
    }

    /**
     * 构建函数剩余部分内容
     */
    private static void build(String func, StringBuilder gremlin, Where param, AtomicBoolean shouldDot) {
        if (shouldDot.get()) {
            appendDot(gremlin);
        }
        gremlin.append(func);
        appendRightBracket(param.getParam(), gremlin);
    }

    /**
     * 拼接has函数中的内容
     */
    private static String getHas(GraphHas has) {
        StringBuilder hasBuilder = new StringBuilder();
        if (StringUtils.isNotBlank(has.getLabel())) {
            hasBuilder.append(SINGLE_QUOTE).append(has.getLabel()).append(SINGLE_QUOTE).append(COMMA);
        }
        if (StringUtils.isNotBlank(has.getKey())) {
            hasBuilder.append(SINGLE_QUOTE).append(has.getKey()).append(SINGLE_QUOTE).append(COMMA);
        }
        if (StringUtils.isNotBlank(has.getValue())) {
            hasBuilder.append(SINGLE_QUOTE).append(has.getValue()).append(SINGLE_QUOTE).append(COMMA);
        }
        return hasBuilder.substring(0, hasBuilder.length() - 1);
    }

    /**
     * 追加符号.
     */
    private static void appendDot(StringBuilder gremlin) {
        gremlin.append(DOT);
    }

    /**
     * 追加右括号 )
     */
    private static void appendRightBracket(Object param, StringBuilder gremlin) {
        if (param instanceof GraphHas) {
            gremlin.append(getHas((GraphHas) param));
        } else if (param instanceof Where) {
            Where where = (Where) param;
            buildGremlin(Collections.singletonList(where), gremlin, new AtomicBoolean(false));
        } else if (null != param) {
            if (param instanceof Integer) {
                gremlin.append(param);
            } else {
                List<String> values = Arrays.asList(param.toString().split(COMMA));
                if (!values.isEmpty()) {
                    for (int i = 0, length = values.size(); i < length; i++) {
                        gremlin.append(SINGLE_QUOTE).append(values.get(i)).append(SINGLE_QUOTE);
                        if (i != length - 1) {
                            gremlin.append(COMMA);
                        }
                    }
                }
            }

        }
        gremlin.append(RIGHT_BRACKET);
    }
}
