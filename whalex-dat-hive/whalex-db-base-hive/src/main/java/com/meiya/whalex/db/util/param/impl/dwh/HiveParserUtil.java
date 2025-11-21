package com.meiya.whalex.db.util.param.impl.dwh;

import cn.hutool.core.collection.CollectionUtil;
import com.meiya.whalex.db.entity.AddParamCondition;
import com.meiya.whalex.db.entity.DelParamCondition;
import com.meiya.whalex.db.entity.IndexParamCondition;
import com.meiya.whalex.db.entity.UpdateParamCondition;
import com.meiya.whalex.db.entity.ani.AniHandler;
import com.meiya.whalex.db.entity.dwh.HiveFieldTypeEnum;
import com.meiya.whalex.db.util.common.RelDbUtil;
import com.meiya.whalex.exception.BusinessException;
import com.meiya.whalex.interior.db.constant.CommonConstant;
import com.meiya.whalex.interior.db.operation.in.CreateTableParamCondition;
import com.meiya.whalex.interior.db.search.condition.AggFunctionType;
import com.meiya.whalex.interior.db.search.condition.AggOpType;
import com.meiya.whalex.interior.db.search.condition.Method;
import com.meiya.whalex.interior.db.search.condition.Rel;
import com.meiya.whalex.interior.db.search.in.AggFunction;
import com.meiya.whalex.interior.db.search.in.Aggs;
import com.meiya.whalex.interior.db.search.in.Order;
import com.meiya.whalex.interior.db.search.in.Page;
import com.meiya.whalex.interior.db.search.in.QueryParamCondition;
import com.meiya.whalex.interior.db.search.in.Where;
import com.meiya.whalex.util.AggResultTranslateUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.*;

/**
 * 关系型数据库通用，解析查询实体转换为SQL
 *
 * @author 黄河森
 * @date 2019/12/23
 * @project whale-cloud-platformX
 */
public class HiveParserUtil {

    public final static String DOUBLE_QUOTATION_MARKS = "`";

    /**
     * 拼接聚合条件select
     *
     * @param aggs
     * @param selectSb
     * @param groupSb
     * @param orderList
     * @param aggFunctionList
     */
    public static void aggResolver(Aggs aggs, StringBuilder selectSb, StringBuilder groupSb, List<Order> orderList, List<AggFunction> aggFunctionList) {
        AggOpType type = aggs.getType();
        if(!type.equals(AggOpType.GROUP) && !type.equals(AggOpType.TERMS)) {
            throw new BusinessException("关系型数据库，只支持 group 和 terms聚合操作");
        }
        if (selectSb.length() == 0) {
            selectSb.append("select ").append(aggs.getField()).append(" as ").append(aggs.getAggName());
        } else {
            selectSb.append(",").append(aggs.getField()).append(" as ").append(aggs.getAggName());
        }
        if (groupSb.length() == 0) {
            groupSb.append("group by ").append(aggs.getField());
        } else {
            groupSb.append(",").append(aggs.getField());
        }
        if (CollectionUtil.isNotEmpty(aggs.getOrders())) {
            orderList.addAll(aggs.getOrders());
        }
        if (CollectionUtil.isNotEmpty(aggs.getAggList())) {
            aggResolver(aggs.getAggList().get(0), selectSb, groupSb, orderList, aggFunctionList);
        } else {
            selectSb.append(",count(1) as doc_count");
            List<AggFunction> aggFunctions = aggs.getAggFunctions();
            if(CollectionUtils.isNotEmpty(aggFunctions)) {
                aggFunctionList.addAll(aggFunctions);
                aggFunctions.stream().forEach(item->{
                    // 关系型数据库中，distinct不做操作
                    if(item.getAggFunctionType() != AggFunctionType.DISTINCT) {
                        selectSb.append(", " + item.getAggFunctionType().getType() + "(" + item.getField() + ") as " + item.getFunctionName());
                    }
                });
            }
        }
    }

    public static AniHandler.AniQuery buildAggQuerySql(QueryParamCondition paramCondition) {
        AniHandler.AniQuery aniQuery = new AniHandler.AniQuery();
        StringBuilder sb = new StringBuilder();
        List<Aggs> aggList = paramCondition.getAggList();
        //关系型数据库，聚合只有一层
        Aggs aggs = aggList.get(0);
        // 拼接 select
        StringBuilder selectSb = new StringBuilder();
        // 拼接group by
        StringBuilder groupSb = new StringBuilder();
        List<Order> orderList = new ArrayList<>();
        List<AggFunction> aggFunctions = new ArrayList<>();
        // 聚合解析
        aggResolver(aggs, selectSb, groupSb, orderList, aggFunctions);
        sb.append(selectSb);
        sb.append(" from ");
        // 表名
        // 先使用占位符
        sb.append("${tableName}");
        sb.append(" ");

        //where
        if (CollectionUtils.isNotEmpty(paramCondition.getWhere())) {
            sb.append(buildWhereSql(paramCondition.getWhere()));
        }
        // 分组
        sb.append(" ").append(groupSb);

        //排序， 每一个聚合函数的结果都有对应的别名，顾需要做相应的转换
        if(CollectionUtils.isNotEmpty(orderList)) {
            orderList.stream().forEach(item->{
                String field = item.getField();
                if(CollectionUtils.isNotEmpty(aggFunctions)) {
                    for(AggFunction aggFunction : aggFunctions) {
                        if(aggFunction.getField().equalsIgnoreCase(field)) {
                            item.setField(aggFunction.getFunctionName());
                            break;
                        }
                    }
                }
            });
        }
        sb.append(buildOrderSql(orderList));

        // 分页
        Page page = new Page();
        page.setOffset(aggs.getOffset());
        page.setLimit(aggs.getLimit());
        sb.append(buildLimitSql(page));

        // 详细数据
        aniQuery.setLimit(aggs.getLimit());
        aniQuery.setOffset(aggs.getOffset());
        aniQuery.setAgg(true);
        List<String> aggNames = new ArrayList<>();
        AggResultTranslateUtil.parserRelationDataAggKey(aggs, aggNames);
        aniQuery.setAggNames(aggNames);
        aniQuery.setSql(sb.toString());
        return aniQuery;
    }

    /**
     * 解析 查询 SQL
     *
     * @param paramCondition
     * @return
     */
    public static AniHandler.AniQuery parserQuerySql(QueryParamCondition paramCondition) {
        if(CollectionUtils.isNotEmpty(paramCondition.getAggList())) {
            return buildAggQuerySql(paramCondition);
        }

        AniHandler.AniQuery aniQuery = new AniHandler.AniQuery();
        // 查询sql
        StringBuilder sb = new StringBuilder();
        // 总数sql
        StringBuilder sbCount = new StringBuilder();
        // 公用sql部分
        StringBuilder sbCom = new StringBuilder();

        sb.append("select ");
        sb.append(buildSelectSql(paramCondition.getSelect()));
        sb.append(" from ");

        sbCount.append("select count(1) as count from ");

        // 表名
        // 先使用占位符
        sbCom.append("${tableName}");
        sb.append(" ");

        // 查询条件
        // TODO:生成占位符的sql
        sbCom.append(buildWhereSql(paramCondition.getWhere()));

        sbCount.append(sbCom);

        // 总数
        aniQuery.setSqlCount(sbCount.toString());

        sb.append(sbCom);

        //分组
        sb.append(buildGroupSql(paramCondition.getGroup()));

        // 排序，如果存在分页必须根据 rand() 排序
        /*if (paramCondition.getPage() != null) {
            List<Order> orderList = paramCondition.getOrder();
            if (orderList == null) {
                orderList = new ArrayList<>();
                paramCondition.setOrder(orderList);
            }
            orderList.add(new Order("rand()"));
        }*/
        sb.append(buildOrderSql(paramCondition.getOrder()));

        // 分页
        sb.append(buildLimitSql(paramCondition.getPage()));

        // 详细数据
        aniQuery.setSql(sb.toString());

        return aniQuery;
    }



    /**
     * 构造 SELECT
     *
     * @param selects
     * @return
     */
    private static String buildSelectSql(List<String> selects) {
        StringBuilder sb = new StringBuilder();
        if (selects != null && !selects.isEmpty()) {
            for (String select : selects) {
                if (StringUtils.isBlank(select)) {
                    continue;
                }
                String checkMethod = checkMethod(select);
                if (StringUtils.isBlank(checkMethod)) {
                    String checkAlias = checkAlias(select, DOUBLE_QUOTATION_MARKS);
                    if (StringUtils.isBlank(checkAlias)) {
                        sb.append(DOUBLE_QUOTATION_MARKS);
                        sb.append(select);
                        sb.append(DOUBLE_QUOTATION_MARKS);
                    } else {
                        sb.append(checkAlias);
                    }
                    sb.append(",");
                } else {
                    sb.append(checkMethod);
                    sb.append(",");
                }
            }
        }

        if (sb.length() > 0) {
            sb.deleteCharAt(sb.length() - 1);
        } else {
            sb.append(" * ");
        }
        return sb.toString();
    }

    /**
     * 验证查询字段是否带有函数
     *
     * @param select
     * @return
     */
    private static String checkMethod(String select) {
        boolean flag = StringUtils.startsWithIgnoreCase(select, "f(");
        if (flag) {
            select = select.substring(2, select.lastIndexOf(")"));
            String methodName = select.substring(0, select.indexOf("("));
            String parse = Method.parse(methodName.toLowerCase()).getName();
            String field = select.substring(select.indexOf("(") + 1, select.lastIndexOf(")"));
            StringBuffer stringBuffer = new StringBuffer();
            StringBuffer queryField = stringBuffer.append(parse).append("(").append(DOUBLE_QUOTATION_MARKS).append(field).append(DOUBLE_QUOTATION_MARKS).append(") AS ").append(DOUBLE_QUOTATION_MARKS)
                    .append(parse)
                    .append("(")
                    .append(field)
                    .append(")")
                    .append(DOUBLE_QUOTATION_MARKS);
            return queryField.toString();
        } else {
            return null;
        }
    }

    /**
     * 构造 WHERE
     *
     * @param wheres
     * @return
     */
    private static String buildWhereSql(List<Where> wheres) {
        String sql = buildWhereSql(wheres, Rel.AND);
        if (sql.length() > 0) {
            sql = sql.substring(0, sql.length() - 4);
            return " where " + sql;
        }
        return "";
    }

    /**
     * 构造 WHERE 递归
     *
     * @param wheres
     * @return
     */
    private static String buildWhereSql(List<Where> wheres, Rel rel) {
        StringBuilder sb = new StringBuilder();
        if (wheres != null && !wheres.isEmpty()) {
            for (Where where : wheres) {
                if (Rel.AND.equals(where.getType()) || Rel.OR.equals(where.getType())) {
                    List<Where> params = where.getParams();
                    String sql = buildWhereSql(params, where.getType());
                    if (sql.length() > 0) {
                        sb.append(" (");
                        sql = sql.substring(0, sql.length() - 2 - where.getType().getName().length());
                        sb.append(sql);
                        sb.append(") ");
                        sb.append(rel.getName());
                        sb.append(" ");
                    }
                } else {
                    sb.append(buildWhereSql(where, rel));
                }
            }
        }
        return sb.toString();
    }

    /**
     * 构造 WHERE 递归
     *
     * @param where
     * @param rel
     * @return
     */
    private static String buildWhereSql(Where where, Rel rel) {
        StringBuilder sb = new StringBuilder();
        if (whereHandler(where)) {
            sb.append(relStrHandler(where));
            sb.append(" ");
            sb.append(rel.getName());
            sb.append(" ");
        }
        return sb.toString();
    }

    /**
     * 字段判断，不为空并且不为分表时间字段
     *
     * @param where
     * @return
     */
    private static boolean whereHandler(Where where) {
        return where != null && !StringUtils.isBlank(where.getField()) && !CommonConstant.BURST_ZONE.equalsIgnoreCase(where.getField());
    }

    /**
     * 解析操作符
     *
     * @param where
     * @return
     */
    private static String relStrHandler(Where where) {
        Rel rel = where.getType();
        StringBuilder sb = new StringBuilder();
        sb.append(where.getField());
        switch (rel) {
            case EQ:
            case TERM:
                sb.append(" = ");
                sb.append("'");
                sb.append(where.getParam());
                sb.append("'");
                break;
            case NE:
                sb.append(" != ");
                sb.append("'");
                sb.append(where.getParam());
                sb.append("'");
                break;
            case GT:
                sb.append(" > ");
                sb.append("'");
                sb.append(where.getParam());
                sb.append("'");
                break;
            case GTE:
                sb.append(" >= ");
                sb.append("'");
                sb.append(where.getParam());
                sb.append("'");
                break;
            case LT:
                sb.append(" < ");
                sb.append("'");
                sb.append(where.getParam());
                sb.append("'");
                break;
            case LTE:
                sb.append(" <= ");
                sb.append("'");
                sb.append(where.getParam());
                sb.append("'");
                break;
            case IN:
                sb.append(" in ");
                List param = (List) where.getParam();
                sb.append(" ( ");
                for (Object o : param) {
                    sb.append("'");
                    sb.append(o);
                    sb.append("'");
                    sb.append(",");
                }
                sb.deleteCharAt(sb.length() - 1);
                sb.append(" ) ");
                break;
            case NIN:
                sb.append(" not in ");
                List paramNin = (List) where.getParam();
                sb.append(" ( ");
                for (Object o : paramNin) {
                    sb.append("'");
                    sb.append(o);
                    sb.append("'");
                    sb.append(",");
                }
                sb.deleteCharAt(sb.length() - 1);
                sb.append(" ) ");
                break;
            case NULL:
                sb.append(" is null ");
                break;
            case NOT_NULL:
                sb.append(" is not null ");
                break;
            case LIKE:
                sb.append(" like ");
                sb.append("'%");
                sb.append(where.getParam());
                sb.append("%'");
                break;
            case MIDDLE_LIKE:
                sb.append(" like ");
                sb.append("'");
                String p = (String) where.getParam();
                p = p.replace("*", "%");
                p = p.replace("?", "_");
                sb.append(p);
                sb.append("'");
                break;
            case FRONT_LIKE:
                sb.append(" like ");
                sb.append("'%");
                sb.append(where.getParam());
                sb.append("'");
                break;
            case TAIL_LIKE:
                sb.append(" like ");
                sb.append("'");
                sb.append(where.getParam());
                sb.append("%'");
                break;
            default:
                sb.append(" = ");
                break;
        }
        return sb.toString();
    }

    /**
     * 解析分页
     *
     * @param page
     * @return
     */
    private static String buildLimitSql(Page page) {
        page = RelDbUtil.pageHandler(page);
        StringBuilder sb = new StringBuilder();
        sb.append(" limit ");
        sb.append(page.getLimit());
        /*if (page.getOffset() > 0) {
            sb.append(" offset ")
                    .append(page.getOffset());
        }*/
        return sb.toString();
    }

    /**
     * 分组SQL
     *
     * @param groupList
     * @return
     */
    private static String buildGroupSql(List<String> groupList) {
        StringBuilder sb = new StringBuilder();
        if (groupList != null && !groupList.isEmpty()) {
            sb.append(" GROUP BY ");
            for (int i = 0; i < groupList.size(); i++) {
                String group = groupList.get(i);
                sb.append(group);
                if (i < groupList.size() - 1) {
                    sb.append(",");
                }
            }
        }
        return sb.toString();
    }

    /**
     * 排序SQL
     *
     * @param orders
     * @return
     */
    private static String buildOrderSql(List<Order> orders) {
        StringBuilder sb = new StringBuilder();
        if (orders != null && !orders.isEmpty()) {
            StringBuilder sb1 = new StringBuilder();
            for (Order order : orders) {
                if (!RelDbUtil.orderHandler(order)) {
                    continue;
                }
                sb1.append(order.getField());
                sb1.append(" ");
                sb1.append(order.getSort().getName());
                sb1.append(",");
            }
            if (sb1.length() > 0) {
                sb1.deleteCharAt(sb1.length() - 1);
                sb.append(" order by ");
                sb.append(sb1);
            }
        }
        return sb.toString();
    }

    private static String checkAlias(String select, String marks) {
        if (StringUtils.isBlank(select)) {
            return null;
        }
        boolean flag = StringUtils.startsWithIgnoreCase(select, "as(");
        if (flag) {
            select = select.substring(3, select.lastIndexOf(")"));
            String[] split = select.split(",");
            String field;
            String alias;
            if (split.length == 1) {
                field = alias = StringUtils.trim(split[0]);
            } else {
                field = StringUtils.trim(split[0]);
                alias = StringUtils.trim(split[1]);
            }
            StringBuffer stringBuffer = new StringBuffer();
            stringBuffer.append(marks)
                    .append(field)
                    .append(marks)
                    .append(" as ")
                    .append(marks)
                    .append(alias)
                    .append(marks);
            return stringBuffer.toString();
        } else {
            return null;
        }
    }
}
