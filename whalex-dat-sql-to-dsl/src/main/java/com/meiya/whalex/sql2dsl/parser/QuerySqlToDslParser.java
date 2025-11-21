package com.meiya.whalex.sql2dsl.parser;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.map.MapUtil;
import cn.hutool.core.util.EnumUtil;
import com.meiya.whalex.db.entity.PageResult;
import com.meiya.whalex.exception.BusinessException;
import com.meiya.whalex.interior.db.builder.AggregateBuilder;
import com.meiya.whalex.interior.db.builder.QueryParamBuilder;
import com.meiya.whalex.interior.db.search.condition.AggFunctionType;
import com.meiya.whalex.interior.db.search.condition.AggOpType;
import com.meiya.whalex.interior.db.search.condition.DateHistogramBoundsType;
import com.meiya.whalex.interior.db.search.condition.Sort;
import com.meiya.whalex.interior.db.search.in.AggFunction;
import com.meiya.whalex.interior.db.search.in.Aggs;
import com.meiya.whalex.interior.db.search.in.Order;
import com.meiya.whalex.interior.db.search.in.QueryParamCondition;
import com.meiya.whalex.interior.db.search.in.Where;
import com.meiya.whalex.sql2dsl.entity.SqlAgg;
import com.meiya.whalex.sql2dsl.entity.SqlDateHistogramAgg;
import com.meiya.whalex.sql2dsl.entity.SqlGroupAgg;
import com.meiya.whalex.sql2dsl.enums.GroupByFunctionEnum;
import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUnresolvedFunction;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;


/**
 * sql转dsl
 *
 * @author 蔡荣桂
 * @date 2023/02/02
 * @project whalex-dat-sql
 */
public class QuerySqlToDslParser extends AbstractSqlToDslParser<QueryParamCondition>{

    private String sql;

    private String tableName;

    private Object[] params;

    private List<String> selectField = new ArrayList<>();

    private List<String> schema = new ArrayList<>();

    private Map<String, String> asFieldMap  = new HashMap<>();

    // SELECT 中的 函数
    private List<FunctionOperation> functionOperations = new ArrayList<>();

    // ORDER BY 中的函数
    private Map<Integer, FunctionOperation> orderFunctionOperations = new LinkedHashMap<>();

    //聚合查询
    private List<SqlAgg> groupList = new ArrayList<>();

    //最多查几条 等于0 不做限制
    private int maxRows;

    // 排序字段
    private List<Order> orderList = new ArrayList<>();

    // 返回条数
    private Integer limit = null;

    // 跳跃索引
    private Integer skip = null;

    public QuerySqlToDslParser(String sql, Object[] params) {
        this.sql = sql;
        this.params = params;
    }

    public QuerySqlToDslParser(String sql, int maxRows, Object[] params) {
        if(maxRows < 0) {
            maxRows = 0;
        }
        this.sql = sql;
        this.maxRows = maxRows;
        this.params = params;
    }

    private static class FunctionOperation {
        private String operator;
        private String field;
        private String asField;
    }

    public QueryParamCondition handle() throws SqlParseException {
        SqlParser sqlParser = SqlParser.create(getSqlConvertPlaceholders(sql), config);
        SqlNode sqlNode = sqlParser.parseStmt();

        // 解析sql转化成dat语法
        QueryParamBuilder queryParamBuilder = QueryParamBuilder.builder().page(0, 10000);

        //order and page
        if(sqlNode instanceof SqlOrderBy) {

            SqlOrderBy sqlOrderBy = (SqlOrderBy) sqlNode;
            buildOrderAndPage(sqlOrderBy);

            //查询sql
            sqlNode = sqlOrderBy.query;
        }

        if (this.skip == null) {
            this.skip = 0;
        }
        if (this.limit == null) {
            if (maxRows > 0) {
                this.limit = maxRows;
            } else {
                this.limit = 10000;
            }
        }

        if(sqlNode instanceof SqlSelect) {

            SqlSelect sqlSelect = (SqlSelect) sqlNode;

            //获取表名
            tableName = getName(sqlSelect.getFrom());

            //select 解析
            SqlNodeList selectList = sqlSelect.getSelectList();
            selectParse(selectList);

            // 处理 ORDER BY 中的函数
            if (CollectionUtil.isNotEmpty(orderFunctionOperations)) {
                for (Map.Entry<Integer, FunctionOperation> entry : orderFunctionOperations.entrySet()) {
                    Integer index = entry.getKey();
                    FunctionOperation value = entry.getValue();
                    boolean flag = true;
                    for (FunctionOperation functionOperation : this.functionOperations) {
                        // 若排序函数已经存在于SELECT函数中，则直接使用SELECT函数名做排序
                        if (StringUtils.equalsIgnoreCase(functionOperation.operator, value.operator) && StringUtils.equalsIgnoreCase(functionOperation.field, value.field)) {
                            if (!StringUtils.equalsIgnoreCase(functionOperation.asField, value.asField)) {
                                Order order = this.orderList.get(index);
                                order.setField(functionOperation.asField);
                            }
                            flag = false;
                            break;
                        }
                    }
                    // 不存在与 SELECT 函数中，则将 ORDER BY 函数放入
                    if (flag) {
                        this.functionOperations.add(value);
                    }
                }
            }

            //字段
            if(!selectField.isEmpty() && !selectField.contains("*")) {
                queryParamBuilder.select(selectField.toArray(new String[selectField.size()]));
            }

            // where 解析
            SqlNode whereNode = sqlSelect.getWhere();

            if (whereNode != null) {

                Where where = DatWhereBuilder
                        .create()
                        .whereSqlNode(whereNode, params)
                        .build();


                queryParamBuilder.where(where);
            }

            //group 解析
            SqlNodeList groupNode = sqlSelect.getGroup();
            if (groupNode != null) {
                Aggs aggs = handleGroup(groupNode, functionOperations, this.orderList, skip, limit);
                queryParamBuilder.aggregate(aggs);

                // HAVING 解析
                SqlNode havingNode = sqlSelect.getHaving();
                if (havingNode != null) {
                    Where where = DatWhereBuilder
                            .create()
                            .whereSqlNode(havingNode, params)
                            .build();
                    aggs.setHaving(where);
                    // 若为日期直方图聚合则设置桶操作为 HARD_BOUNDS
                    if (AggOpType.DATE_HISTOGRAM.equals(aggs.getType())) {
                        aggs.setDateHistogramBoundsType(DateHistogramBoundsType.HARD_BOUNDS);
                    }
                }
            } else if (!functionOperations.isEmpty()) {
                for (FunctionOperation functionOperation : functionOperations) {
                    AggFunction aggFunction = AggFunction.create(functionOperation.asField,
                            functionOperation.field,
                            AggFunctionType.findType(functionOperation.operator.toLowerCase()));
                    queryParamBuilder.aggFunction(aggFunction);
                }
            }

            queryParamBuilder.page(skip, limit);
            // 排序处理
            if (CollectionUtil.isNotEmpty(this.orderList)) {
                queryParamBuilder.orders(this.orderList);
            }

        }

        if(tableName == null) {
            throw new RuntimeException("表名为空");
        }

        return queryParamBuilder.build();
    }

    @Override
    public String getTableName() {
        return tableName;
    }

    private void buildOrderAndPage(SqlOrderBy sqlOrderBy) {

        //排序
        SqlNodeList orderList = sqlOrderBy.orderList;
        buildOrder(orderList);

        //分页
        SqlNode fetch = sqlOrderBy.fetch;
        SqlNode offset = sqlOrderBy.offset;
        buildPage(fetch, offset);
    }



    private void buildPage(SqlNode fetch, SqlNode offset) {
        Integer limit = null;
        Integer skip = null;

        if(fetch != null) {
            limit = Integer.parseInt(getName(fetch));
        }

        if(offset != null) {
            skip =  Integer.parseInt(getName(offset));
        }

        if(limit != null || skip != null) {

            if (skip == null) {
                skip = 0;
            }

            //限制查询数量
            if(maxRows > 0 && maxRows < limit) {
                limit = maxRows;
            }

            this.limit = limit;
            this.skip = skip;
        }
    }

    private void buildOrder(SqlNodeList orderList) {
        if(orderList == null) {
            return;
        }
        for (SqlNode sqlNode : orderList) {
            if(sqlNode instanceof SqlIdentifier) {
                //升序
                String field = getName(sqlNode);
                this.orderList.add(Order.create(field, Sort.ASC));
            }else if(sqlNode  instanceof SqlBasicCall){
                SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlNode;
                // 判断排序是函数还是降序
                String operator = getOperator(sqlBasicCall.getOperator());
                if (StringUtils.equalsIgnoreCase(operator, "DESC")) {
                    //降序
                    SqlNode node = sqlBasicCall.getOperandList().get(0);

                    if (node instanceof SqlBasicCall) {
                        // 函数
                        SqlBasicCall _sqlBasicCall = (SqlBasicCall) node;
                        String field = getName(_sqlBasicCall.getOperandList().get(0));
                        String _operator = getOperator(_sqlBasicCall.getOperator());
                        FunctionOperation functionOperation = buildFunctionOperation(_operator, field, null);
                        this.orderList.add(Order.create(functionOperation.asField, Sort.DESC));
                        this.orderFunctionOperations.put(this.orderList.size() - 1,functionOperation);
                    } else {
                        String field = getName(node);
                        this.orderList.add(Order.create(field, Sort.DESC));
                    }
                } else {
                    // 排序条件是函数
                    String field = getName(sqlBasicCall.getOperandList().get(0));
                    FunctionOperation functionOperation = buildFunctionOperation(operator, field, null);
                    this.orderList.add(Order.create(functionOperation.asField, Sort.ASC));
                    this.orderFunctionOperations.put(this.orderList.size() - 1,functionOperation);
                }
            }
        }
    }

    private Aggs handleGroup(SqlNodeList groupNode, List<FunctionOperation> functionOperations, List<Order> orderList, Integer offset, Integer limit) {

        Map<String, Order> orderMap = null;
        // 在聚合中排序的字段，需要被移除
        List<String> removeOrderField = new ArrayList<>();

        if (CollectionUtil.isNotEmpty(orderList)) {
            orderMap = new HashMap<>(orderList.size());
            for (Order order : orderList) {
                orderMap.put(order.getField(), order);
            }
        }

        for (SqlNode sqlNode : groupNode) {
            if (sqlNode instanceof SqlBasicCall) {
                // GROUP BY 字段为函数
                SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlNode;
                SqlAgg sqlAgg = groupByFunctionParse(sqlBasicCall);
                groupList.add(sqlAgg);
            } else {
                List<String> names = getNames(sqlNode);
                for (String name : names) {
                    SqlGroupAgg sqlGroupAgg = SqlGroupAgg.builder().fieldName(name).build();
                    groupList.add(sqlGroupAgg);
                }
            }
        }

        Aggs firstAggs = null;
        Aggs aggs = null;

        for (int i = 0; i < groupList.size(); i++) {
            SqlAgg sqlAgg = groupList.get(i);
            String group = sqlAgg.getFieldName();
            if(aggs == null) {
                aggs = buildAgg(sqlAgg);
                firstAggs  = aggs;
            }else {
                Aggs childAggs = buildAgg(sqlAgg);
                aggs.setAggList(CollectionUtil.newArrayList(childAggs));
                aggs = childAggs;
            }
            // 若 order 不为空，则判断是否有根据当前聚合字段排序的条件
            if (MapUtil.isNotEmpty(orderMap)) {
                // 判断排序字段和聚合字段是否一致
                if (orderMap.containsKey(group)) {
                    removeOrderField.add(group);
                    Order order = orderMap.get(group);
                    List<Order> orders = aggs.getOrders();
                    if (orders == null) {
                        orders = new ArrayList<>();
                        aggs.setOrders(orders);
                    }
                    orders.add(order);
                } else if (orderMap.containsKey(aggs.getAggName())) {
                    // 判断是否为分组函数，例如 date_histogram,并且排序也为分组函数
                    String aggName = aggs.getAggName();
                    removeOrderField.add(aggName);
                    Order order = orderMap.remove(aggName);
                    order = Order.create(aggs.getField(), order.getSort());
                    List<Order> orders = aggs.getOrders();
                    if (orders == null) {
                        orders = new ArrayList<>();
                        aggs.setOrders(orders);
                    }
                    orders.add(order);
                }

            }
        }

        if (aggs == null) {
            return firstAggs;
        }

        // 分页限定
        if (offset != null) {
            aggs.setOffset(offset);
        }
        if (limit != null) {
            aggs.setLimit(limit);
        }

        // 处理聚合函数
        for (FunctionOperation functionOperation : functionOperations) {
            // 处理 时间函数
            String operator = functionOperation.operator;
            if (EnumUtil.contains(GroupByFunctionEnum.class, operator.toUpperCase())) {
                AggOpType type = aggs.getType();
                GroupByFunctionEnum groupByFunctionEnum = GroupByFunctionEnum.valueOf(operator.toUpperCase());
                if (groupByFunctionEnum.getOpType().equals(type)) {
                    continue;
                }
            }

            AggFunction aggFunction = AggFunction.create(functionOperation.asField,
                    functionOperation.field,
                    AggFunctionType.findType(operator.toLowerCase()));
            aggs.addFunction(aggFunction);
        }

        // 处理聚合函数排序条件
        if (CollectionUtil.isNotEmpty(functionOperations) && MapUtil.isNotEmpty(orderMap)) {
            List<String> functionFields = functionOperations.stream().flatMap(functionOperation -> {
                        if (StringUtils.isNotBlank(functionOperation.asField)) {
                            return Stream.of(functionOperation.asField);
                        } else {
                            return Stream.of(functionOperation.field);
                        }
                    })
                    .collect(Collectors.toList());
            List<Order> orders = new ArrayList<>();

            for (Map.Entry<String, Order> entry : orderMap.entrySet()) {
                if (functionFields.contains(entry.getKey())) {
                    orders.add(entry.getValue());
                    removeOrderField.add(entry.getKey());
                }
            }
            // 设置聚合函数排序条件
            if (CollectionUtil.isNotEmpty(orders)) {
                List<Order> aggOrders = aggs.getOrders();
                if (aggOrders == null) {
                    aggs.setOrders(orders);
                } else {
                    aggOrders.addAll(orders);
                }
            }
        }

        // 存在需要移除的聚合字段
        if (CollectionUtil.isNotEmpty(removeOrderField)) {
            Iterator<Order> iterator = orderList.iterator();
            while (iterator.hasNext()) {
                Order next = iterator.next();
                String field = next.getField();
                if (removeOrderField.contains(field)) {
                    iterator.remove();
                }
            }
        }

        return firstAggs;
    }

    /**
     * 组装聚合对象
     *
     * @param sqlAgg
     * @return
     */
    private Aggs buildAgg(SqlAgg sqlAgg) {
        AggOpType opType = sqlAgg.getOpType();
        String aggName = sqlAgg.getAggName();
        String fieldName = sqlAgg.getFieldName();
        switch (opType) {
            case GROUP:
                return Aggs.createGroup(aggName, fieldName);
            case DATE_HISTOGRAM:
                SqlDateHistogramAgg sqlDateHistogramAgg = (SqlDateHistogramAgg) sqlAgg;
                if (CollectionUtil.isNotEmpty(functionOperations)) {
                    Iterator<FunctionOperation> iterator = this.functionOperations.iterator();
                    while (iterator.hasNext()) {
                        FunctionOperation functionOperation = iterator.next();
                        String asField = functionOperation.asField;
                        String operator = functionOperation.operator;
                        String field = functionOperation.field;
                        if (StringUtils.equalsIgnoreCase(fieldName, field) && (StringUtils.equalsAnyIgnoreCase(operator, GroupByFunctionEnum.DATE_FORMAT.name(), GroupByFunctionEnum.DATE_HISTOGRAM.name()))) {
                            // 若 SELECT 函数中存在分组函数，则以 SELECT 函数的别名作为分组函数的别名
                            aggName = asField;
                            sqlAgg.setAggName(aggName);
                            break;
                        }
                    }
                }

                return AggregateBuilder.builder()
                        .histogramForDate(
                                aggName,
                                sqlDateHistogramAgg.getFieldName(),
                                sqlDateHistogramAgg.getIntervalNum(),
                                sqlDateHistogramAgg.getHistogramDateType(),
                                sqlDateHistogramAgg.getFormat()
                        ).build();
            default:
                throw new BusinessException("暂时无法解析 SQL GROUP BY 聚合操作 " + opType.getOp());
        }
    }


    private void selectParse(SqlNodeList selectList){

        // abc ddddd, count(1) as ddd, sum(scbm), csbm
        for (SqlNode sqlNode : selectList) {
            if(sqlNode instanceof SqlIdentifier) {

                //字段
                List<String> fields = getNames(sqlNode);

                //去掉空结果
                fields = fields.stream().filter(field -> {
                    return StringUtils.isNotBlank(field);
                }).collect(Collectors.toList());

                selectField.addAll(fields);
                schema.addAll(fields);

            }else if(sqlNode instanceof SqlBasicCall){

                SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlNode;

                SqlOperator sqlOperator = sqlBasicCall.getOperator();

                if(sqlOperator instanceof SqlAsOperator) {

                    List<SqlNode> operandList = sqlBasicCall.getOperandList();
                    SqlNode sqlNode0 = operandList.get(0);
                    SqlNode sqlNode1 = operandList.get(1);

                    String asField = getName(sqlNode1);

                    schema.add(asField);

                    if(sqlNode0 instanceof SqlIdentifier) {
                        //字段有别名
                        String field = getName(sqlNode0);
                        selectField.add(field);
                        asFieldMap.put(field, asField);
                    }else if(sqlNode0 instanceof SqlBasicCall){
                        //函数有别名
                        SqlBasicCall functionSqlBasicCall = (SqlBasicCall) sqlNode0;
                        String operator = getOperator(functionSqlBasicCall.getOperator());
                        String field = getName(functionSqlBasicCall.getOperandList().get(0));
                        functionOperations.add(buildFunctionOperation(operator, field, asField));
                    }


                } else if(sqlOperator instanceof SqlUnresolvedFunction) {
                    String operator = getOperator(sqlOperator);
                    String field = getName(sqlBasicCall.getOperandList().get(0));
                    FunctionOperation functionOperation = buildFunctionOperation(operator, field, null);
                    functionOperations.add(functionOperation);

                    String func = operator + "("+field+")";

                    schema.add(func);
                    asFieldMap.put(func, functionOperation.asField);
                }
            }
        }

    }

    private String getValue(List<SqlNode> list, int index) {
        if(index < list.size()) {
            String name = getName(list.get(index));
            if(name.startsWith("'") && name.endsWith("'")) {
                return name.substring(1, name.length() - 1);
            }
            return name;
        }
        return null;
    }

    private AggregateBuilder.HistogramDateFormat getHistogramDateFormat(String format) {
        if(format == null) {
            throw new IllegalArgumentException("时间格式不能为空");
        }

        if("%Y-%m-%d %H:%i:%s".equalsIgnoreCase(format)) {
            return AggregateBuilder.HistogramDateFormat.NORM_DATETIME_PATTERN;
        }else if("%Y-%m-%d %H:%i".equalsIgnoreCase(format)){
            return AggregateBuilder.HistogramDateFormat.NORM_DATETIME_MINUTE_PATTERN;
        }else if("%Y-%m-%d %H".equalsIgnoreCase(format)) {
            return AggregateBuilder.HistogramDateFormat.NORM_DATETIME_HOUR_PATTERN;
        }else if("%Y-%m-%d".equalsIgnoreCase(format)) {
            return AggregateBuilder.HistogramDateFormat.NORM_DATE_PATTERN;
        }else if("%Y-%m".equalsIgnoreCase(format)) {
            return AggregateBuilder.HistogramDateFormat.NORM_DATE_MONTH_PATTERN;
        }else if("%Y".equalsIgnoreCase(format)) {
            return AggregateBuilder.HistogramDateFormat.NORM_DATE_YEAR_PATTERN;
        }

        throw new IllegalArgumentException("无效的时间格式");
    }

    private AggregateBuilder.HistogramDateType getHistogramDateType(AggregateBuilder.HistogramDateFormat dateFormat, String dateType) {

        if(dateType != null) {
            return AggregateBuilder.HistogramDateType.getHistogramDateTypeByValue(dateType);
        }


        if(dateFormat == AggregateBuilder.HistogramDateFormat.NORM_DATE_PATTERN) {
            return AggregateBuilder.HistogramDateType.DAY;
        }else if(dateFormat == AggregateBuilder.HistogramDateFormat.NORM_DATE_YEAR_PATTERN){
            return AggregateBuilder.HistogramDateType.YEAR;
        }else if(dateFormat == AggregateBuilder.HistogramDateFormat.NORM_DATE_MONTH_PATTERN){
            return AggregateBuilder.HistogramDateType.MONTH;
        }else if(dateFormat == AggregateBuilder.HistogramDateFormat.NORM_DATETIME_MINUTE_PATTERN){
            return AggregateBuilder.HistogramDateType.MINUTE;
        }else if(dateFormat == AggregateBuilder.HistogramDateFormat.NORM_DATETIME_HOUR_PATTERN){
            return AggregateBuilder.HistogramDateType.HOUR;
        }else if(dateFormat == AggregateBuilder.HistogramDateFormat.NORM_DATETIME_PATTERN){
            return AggregateBuilder.HistogramDateType.SECOND;
        }

        throw new IllegalArgumentException("无效的时间间隔单位");
    }

    private FunctionOperation buildFunctionOperation(String operator, String field, String asField) {
        FunctionOperation functionOperation = new FunctionOperation();
        functionOperation.operator = operator;
        functionOperation.field = field;
        if (StringUtils.isBlank(asField)) {
            asField = operator + "_" + StringUtils.replace(field, ".", "_");
        }
        functionOperation.asField = asField;
        return functionOperation;
    }


    private String getOperator(SqlOperator sqlOperator) {
        return sqlOperator.getName();
    }


    private List<String> getNames(SqlNode sqlNode) {
        SqlIdentifier  sqlIdentifier = (SqlIdentifier) sqlNode;
        return sqlIdentifier.names;
    }

    private String getName(SqlNode sqlNode) {
        if(sqlNode instanceof SqlIdentifier) {
            List<String> names = getNames(sqlNode);
            return names.get(names.size() - 1);
        } else {
            SqlLiteral sqlLiteral = (SqlLiteral) sqlNode;
            return sqlLiteral.getValue().toString();
        }
    }

    /**
     * 函数分组解析
     *
     * @return
     */
    private SqlAgg groupByFunctionParse(SqlBasicCall sqlBasicCall) {
        String operator = getOperator(sqlBasicCall.getOperator());
        if (EnumUtil.contains(GroupByFunctionEnum.class, operator.toUpperCase())) {
            GroupByFunctionEnum groupByFunctionEnum = GroupByFunctionEnum.valueOf(operator.toUpperCase());
            switch (groupByFunctionEnum) {
                case DATE_FORMAT:
                    AggregateBuilder.HistogramDateFormat histogramDateFormat = getHistogramDateFormat(getValue(sqlBasicCall.getOperandList(), 1));
                    AggregateBuilder.HistogramDateType histogramDateType = getHistogramDateType(histogramDateFormat, null);
                    return SqlDateHistogramAgg.builder().fieldName(getName(sqlBasicCall.getOperandList().get(0))).intervalNum(1).histogramDateType(histogramDateType).format(histogramDateFormat).build();
                case DATE_HISTOGRAM:
                    List<SqlNode> operandList = sqlBasicCall.getOperandList();
                    String format = getValue(operandList, 1);
                    String intervalNumStr = getValue(operandList, 2);
                    String dateType = getValue(operandList, 3);
                    String sort = getValue(operandList, 4);
                    String sortFiled = getValue(operandList, 5);
                    String nullSort = getValue(operandList, 6);
                    int intervalNum = 1;
                    if(intervalNumStr != null) {
                        intervalNum = Integer.parseInt(intervalNumStr);
                    }
                    histogramDateFormat = getHistogramDateFormat(format);
                    histogramDateType = getHistogramDateType(histogramDateFormat, dateType);
                    return SqlDateHistogramAgg.builder().fieldName(getName(sqlBasicCall.getOperandList().get(0))).intervalNum(intervalNum).histogramDateType(histogramDateType).format(histogramDateFormat).build();
                default:
                    throw new RuntimeException("暂不支持 GROUP BY " + operator + " 函数!");
            }
        } else {
            throw new RuntimeException("暂不支持 GROUP BY " + operator + " 函数!");
        }
    }

    public List<Map> handleResult(PageResult pageResult) {

        List<Map> rows = pageResult.getRows();

        if(CollectionUtils.isEmpty(rows)) {
            return new ArrayList<>();
        }


        if(groupList.isEmpty() && functionOperations.isEmpty())  {
            return rows;
        }

        Map aggs = MapUtils.getMap(rows.get(0), "aggs");
        Map aggregations = MapUtils.getMap(aggs, "aggregations");

        if(!groupList.isEmpty()) {
            List<Map> aggsResult = handleAggsResult(groupList, 0, aggregations);
            return aggsResult;
        } else {
            return handleAggsResult(aggregations);
        }
    }

    private List<Map> handleAggsResult(Map pre) {

        List<Map> list = new ArrayList<>();


        Map result = new HashMap();

        Set keySet = pre.keySet();
        for (Object key: keySet) {
            Map value = (Map) pre.get(key);
            Object v = value.get("value");
            result.put(key, v);
        }

        list.add(result);
        return list;
    }

    /**
     * 递归到最后一层，结果从最后一层返回，逐层加工
     * @param groupList
     * @param index
     * @param pre
     * @return
     */
    private List<Map> handleAggsResult(List<SqlAgg> groupList, int index, Map pre) {

        List<Map> aggsList = new ArrayList<>();

        // 聚合对象
        SqlAgg sqlAgg = groupList.get(index);

        //聚合名称
        String aggsName = sqlAgg.getAggName();

        // 字段
        String field = sqlAgg.getFieldName();

        //获取集合
        Map groupMap = MapUtils.getMap(pre, aggsName);
        List<Map> buckets = (List<Map>) MapUtils.getObject(groupMap, "buckets");
        if (CollectionUtil.isNotEmpty(buckets)) {
            for(int j = 0; j < buckets.size(); j++) {

                Map bucket = buckets.get(j);

                //最后一层
                if(index + 1 == groupList.size()) {
                    Set<String> keySet = bucket.keySet();

                    Map<String, Object> result = new HashMap<>();
                    for(String key : keySet) {
                        Object o = bucket.get(key);
                        if(o instanceof Map) {
                            Object value = MapUtils.getObject((Map) o, "value");
                            result.put(key, value);
                        }else {
                            result.put(key, o);
                        }
                    }
                    //字段赋值
                    result.put(field, result.remove("key"));

                    // 函数字段处理，例如 date_histogram
                    if (result.containsKey("key_as_string")) {
                        result.put(aggsName, result.remove("key_as_string"));
                    }

                    aggsList.add(result);
                } else {

                    //有下一层
                    List<Map> maps = handleAggsResult(groupList, index + 1, bucket);

                    //字段赋值
                    for (Map aggs : maps) {
                        aggs.put(field, bucket.get("key"));
                    }

                    aggsList.addAll(maps);
                }
            }
        }
        return aggsList;
    }

    public List<String> getSchema(List<Map> result) {

        Set<String> asFieldSet = asFieldMap.keySet();

        //别名字段处理
        for (Map map : result) {

            for (String asField : asFieldSet) {
                String field = asFieldMap.get(asField);
                map.put(asField, map.get(field));
            }

        }

        //获取schema
        if(schema.isEmpty() || schema.contains("*") || schema.contains("")) {
            if(!result.isEmpty()) {
                Set<String> keySet = result.get(0).keySet();
                schema.addAll(keySet);
            }
        }

        return schema;
    }
}
