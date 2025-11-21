package com.meiya.whalex.interior.db.search.in;

import com.meiya.whalex.interior.db.search.condition.AggOpType;
import com.meiya.whalex.interior.db.search.condition.DateHistogramBoundsType;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * 聚合函数
 *
 * @author 黄河森
 * @date 2019/9/10
 * @project whale-cloud-platformX
 */
@ApiModel(value = "聚合操作参数")
public class Aggs implements Serializable {

    /**
     * 分组结果的别名（es会使用到）
     */
    @ApiModelProperty(value = "分组结果的别名", notes = "仅ES组件使用")
    private String aggName;

    /**
     * 聚合类型
     * {@link}
     * <p>
     * 其他的暂未封装
     */
    @ApiModelProperty(value = "聚合类型")
    private AggOpType type = AggOpType.GROUP;

    /**
     * 分组字段
     */
    @ApiModelProperty(value = "分组字段")
    private String field;

    @ApiModelProperty(value = "执行结果处理方式")
    private String executionHint;

    //===============时间统计=====================
    /**
     * 时间间隔
     */
    @ApiModelProperty(value = "时间间隔")
    private String interval;

    /**
     * 时间格式
     */
    @ApiModelProperty(value = "时间格式")
    private String format;

    /**
     * 时区配置
     */
    @ApiModelProperty(value = "时区配置，支持 +08:00 格式")
    private String timeZone;

    /**
     * 分桶操作模式
     */
    @ApiModelProperty(value = "日期直方图分桶操作模式")
    private DateHistogramBoundsType dateHistogramBoundsType = DateHistogramBoundsType.EXTENDED_BOUNDS;
    //===============时间统计=====================

    //===============自定义范围聚合================
    @ApiModelProperty(value = "自定义范围聚合条件", notes = "匹配 type: range 操作")
    private List<RangeKey> rangeKeys;
    //===============自定义范围聚合================

    /**
     * 排序
     */
    @ApiModelProperty(value = "排序", hidden = true, notes = "兼容旧的语法，无法删除，新应用禁止使用")
    @Deprecated
    private String order;

    /**
     * 排序类型,desc asc
     */
    @ApiModelProperty(value = "排序类型", allowableValues = "asc,desc", hidden = true, notes = "兼容旧的语法，无法删除，新应用禁止使用")
    @Deprecated
    private String orderType;

    private List<Order> orders;

    /**
     * 偏移量
     */
    @ApiModelProperty(value = "偏移量")
    private Integer offset = 0;

    /**
     * 返回数量
     */
    @ApiModelProperty(value = "返回数量")
    private Integer limit = 10;

    /**
     * 限制分组count的最小返回值
     */
    @ApiModelProperty(value = "限制分组count的最小返回值")
    private Integer mincount = 0;

    /**
     * 嵌套聚合参数
     */
    @ApiModelProperty(value = "嵌套聚合参数", hidden = true, notes = "兼容旧版本后续废除")
    @Deprecated
    private Aggs aggs;

    @ApiModelProperty(value = "嵌套聚合参数")
    private List<Aggs> aggList;

    /**
     * 直接拼接到聚合参数后的参数（由于es聚合操作多变复杂，允许直接写好聚合参数传入）
     */
    @ApiModelProperty(value = "拼接聚合函数")
    private String srcAggsParam;

    @ApiModelProperty(value = "聚合方法")
    private List<AggFunction> aggFunctions;

    @ApiModelProperty(value = "过滤聚合结果")
    private Where having;

    @ApiModelProperty(value = "当前聚合命中的前 N 条数据", notes = "若设置此值，则会返回当前参与聚合的前N条数据")
    @Deprecated
    private Integer hitNum;

    @ApiModelProperty(value = "当前聚合命中的前 N 条数据", notes = "若设置此值，则会返回当前参与聚合的前N条数据")
    private AggHit aggHit;

    public String getExecutionHint() {
        return executionHint;
    }

    public void setExecutionHint(String executionHint) {
        this.executionHint = executionHint;
    }

    /**
     * 若设置 hitNum 值，则返回报文中对应的key名称
     *
     * @return
     */
    public String getHitName() {
        String hitName = getAggName() + "_hit";
        return hitName;
    }

    public String getAggName() {
        if (StringUtils.isBlank(aggName) && StringUtils.isNotBlank(field)) {
            if (type != null) {
                aggName = field + "_" + type.getOp();
            } else {
                aggName = field + "_agg";
            }
        }
        return aggName;
    }

    public void setAggName(String aggName) {
        this.aggName = aggName;
    }

    public AggOpType getType() {
        return type;
    }

    public void setType(AggOpType type) {
        this.type = type;
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public String getInterval() {
        return interval;
    }

    public void setInterval(String interval) {
        this.interval = interval;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public String getTimeZone() {
        return timeZone;
    }

    public void setTimeZone(String timeZone) {
        this.timeZone = timeZone;
    }

    public DateHistogramBoundsType getDateHistogramBoundsType() {
        return dateHistogramBoundsType;
    }

    public void setDateHistogramBoundsType(DateHistogramBoundsType dateHistogramBoundsType) {
        this.dateHistogramBoundsType = dateHistogramBoundsType;
    }

    public String getOrder() {
        return order;
    }

    public void setOrder(String order) {
        this.order = order;
    }

    public Integer getOffset() {
        return offset;
    }

    public void setOffset(Integer offset) {
        this.offset = offset;
    }

    public Integer getLimit() {
        return limit;
    }

    public void setLimit(Integer limit) {
        this.limit = limit;
    }

    public Integer getMincount() {
        return mincount;
    }

    public void setMincount(Integer mincount) {
        this.mincount = mincount;
    }

    public Aggs getAggs() {
        return aggs;
    }

    public void setAggs(Aggs aggs) {
        this.aggs = aggs;
    }

    public String getSrcAggsParam() {
        return srcAggsParam;
    }

    public void setSrcAggsParam(String srcAggsParam) {
        this.srcAggsParam = srcAggsParam;
    }

    public String getOrderType() {
        return orderType;
    }

    public void setOrderType(String orderType) {
        this.orderType = orderType;
    }

    public List<AggFunction> getAggFunctions() {
        return aggFunctions;
    }

    public void setAggFunctions(List<AggFunction> aggFunctions) {
        this.aggFunctions = aggFunctions;
    }

    public List<Order> getOrders() {
        return orders;
    }

    public void setOrders(List<Order> orders) {
        this.orders = orders;
    }

    public Where getHaving() {
        return having;
    }

    public void setHaving(Where having) {
        this.having = having;
    }

    public Integer getHitNum() {
        return hitNum;
    }

    public void setHitNum(Integer hitNum) {
        this.hitNum = hitNum;
    }

    public AggHit getAggHit() {
        return aggHit;
    }

    public void setAggHit(AggHit aggHit) {
        this.aggHit = aggHit;
    }

    public List<Aggs> getAggList() {
        return aggList;
    }

    public void setAggList(List<Aggs> aggList) {
        this.aggList = aggList;
    }

    public static List<Aggs> createAggList() {
        return new ArrayList<>();
    }

    public static Aggs create(String aggName, String field, AggOpType opType) {
        Aggs aggs = new Aggs();
        aggs.setAggName(aggName);
        aggs.setField(field);
        aggs.setType(opType);
        return aggs;
    }

    public static Aggs create(String aggName, String field, AggOpType opType, int limit) {
        Aggs aggs = new Aggs();
        aggs.setAggName(aggName);
        aggs.setField(field);
        aggs.setType(opType);
        aggs.setLimit(limit);
        return aggs;
    }

    public static Aggs createGroup(String aggName, String field) {
        return create(aggName, field, AggOpType.GROUP);
    }

    public static Aggs createGroup(String aggName, String field, int limit) {
        return create(aggName, field, AggOpType.GROUP, limit);
    }

    public Aggs addFunction(AggFunction aggFunction) {
        if (this.aggFunctions == null) {
            this.aggFunctions = new ArrayList<>();
        }
        this.aggFunctions.add(aggFunction);
        return this;
    }

    public List<RangeKey> getRangeKeys() {
        return rangeKeys;
    }

    public void setRangeKeys(List<RangeKey> rangeKeys) {
        this.rangeKeys = rangeKeys;
    }

    /**
     * 自定义范围聚合条件
     */
    @AllArgsConstructor
    @NoArgsConstructor
    @Data
    public static class RangeKey {
        @ApiModelProperty(value = "名称")
        private String key;
        @ApiModelProperty(value = "起始值")
        private Object from;
        @ApiModelProperty(value = "结束值")
        private Object to;
    }
}
