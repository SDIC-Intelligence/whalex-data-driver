package com.meiya.whalex.interior.db.search.in;

import com.meiya.whalex.util.JsonUtil;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * DB 组件查询封装实体
 *
 * @author Huanghesen
 * @date 2018/9/10
 * @project whale-cloud-api
 */
@ApiModel(value = "组件查询条件参数")
public class QueryParamCondition implements Cloneable, Serializable {

    /**
     * 查询字段
     */
    @ApiModelProperty(value = "查询字段")
    private List<String> select;

    /**
     * 云组件分区字段
     */
    @ApiModelProperty(value = "云组件分区字段")
    @Deprecated
    private List<String> partition;


    /**
     * 查询条件
     */
    @ApiModelProperty(value = "查询条件")
    private List<Where> where;

    /**
     * 分组，最简单的分组，以字段field分组，更强大的分组请使用aggs
     */
    @ApiModelProperty(value = "分组条件")
    @Deprecated
    private List<String> group;

    /**
     * 功能更强的聚合操作
     */
    @ApiModelProperty(value = "聚合函数", hidden = true, notes = "兼容旧版本，后续禁止使用")
    private Aggs aggs;

    @ApiModelProperty(value = "聚合操作参数")
    private List<Aggs> aggList;

    @ApiModelProperty(value = "聚合函数")
    private List<AggFunction> aggFunctionList;

    /**
     * 排序
     */
    @ApiModelProperty(value = "排序规则")
    private List<Order> order;

    /**
     * 分页
     */
    @ApiModelProperty(value = "分页")
    private Page page = new Page();

    @ApiModelProperty(value = "是否数据量统计", example = "false")
    private boolean countFlag = false;

    /**
     * 是否对没有时间条件的查询进行时间限制
     */
    @ApiModelProperty(value = "是否限制查询时间", example = "false")
    private boolean isLimitDate = false;

    @ApiModelProperty(value = "是否异步查询", example = "false")
    private Boolean isAsync = Boolean.FALSE;

    /**
     * 一些关系型数据库可使用
     */
    @ApiModelProperty(value = "自定义SQL")
    private String sql;

    /**
     * 是否设置读取超时时间，内部使用，严禁提供给外部
     */
    @ApiModelProperty(value = "限制读取时间")
    private boolean limitReadTime = true;

    /**
     * 返回的最大版本数
     */
    @ApiModelProperty(value = "返回字段最大版本数", notes = "目前只有HBase组件支持当前参数")
    private Integer maxVersion;

    /**
     * 是否自动携带配置的统计规则进行查询
     */
    private Boolean needWhere;

    /**
     * 是否服务查询
     */
    @ApiModelProperty(hidden = true)
    private boolean fromFwFlag = false;

    /**
     * 是否ws查询,兼容旧的，但是不能再使用
     */
    @ApiModelProperty(hidden = true)
    private boolean fromWsFlag = false;

    /**
     * 是否查询异地资源
     */
    @ApiModelProperty(value = "是否查询异地资源", notes = "是否查询异地资源")
    protected boolean needRemote;

    /**
     * 配置查询路由字段
     */
    @ApiModelProperty(value = "配置查询路由字段", notes = "配置查询路由字段")
    protected String routingField;

    @ApiModelProperty(value = "自定义组件原生语法")
    private Object dsl;

    @ApiModelProperty(value = "批量数", notes = "通过游标获取时，批量数大小")
    private Integer batchSize;

    @ApiModelProperty(value = "游标ID", notes = "若为 NULL，则重新创建游标")
    private String cursorId;

    @ApiModelProperty(value = "该条件下是否第一次游标查询", notes = "是:则会去创建游标;否:则会判断缓存中是否存在游标信息,无则表示首次对此资源游标查询")
    private Boolean firstCursor = Boolean.TRUE;

    @ApiModelProperty(value = "是否游标滚动执行到所有数据拉取结束", notes = "true：标识每次游标方法将遍历完所有数据才结束; false：标识每次游标方法只拉取 batchSize 就结束此次方法执行", hidden = true)
    private Boolean cursorRollAllData = Boolean.TRUE;

    @ApiModelProperty(value = "业务时间字段")
    private String businessTime;

    @Deprecated
    @ApiModelProperty(value = "是否是主节点查询", allowableValues = "true, false", notes = "主节点查询")
    private boolean primaryNode = false;

    @ApiModelProperty(value = "查询偏好", notes = "查询偏好")
    private String preference;

    @ApiModelProperty(value = "响应限制每个碎片的指定数量的文档", notes = "响应限制每个碎片的指定数量的文档")
    private Integer terminateAfter;

    @ApiModelProperty(value = "*特殊查询语法：建议词查询")
    private Suggest suggest;

    @ApiModelProperty(value = "高亮个数")
    private int highlightNum = 5;
    @ApiModelProperty(value = "高亮标签前缀")
    private String preTags;
    @ApiModelProperty(value = "高亮标签后缀")
    private String postTags;
    @ApiModelProperty(value = "高亮字段")
    private List<String> highlightFields;

    @ApiModelProperty(value = "关联查询")
    private AssociatedQuery associatedQuery;

    @ApiModelProperty(value = "查询提示")
    private Hint hint;

    public String getPreference() {
        return preference;
    }

    public void setPreference(String preference) {
        this.preference = preference;
    }

    public Integer getTerminateAfter() {
        return terminateAfter;
    }

    public void setTerminateAfter(Integer terminateAfter) {
        this.terminateAfter = terminateAfter;
    }

    public Suggest getSuggest() {
        return suggest;
    }

    public void setSuggest(Suggest suggest) {
        this.suggest = suggest;
    }

    public String getRoutingField() {
        return routingField;
    }

    public void setRoutingField(String routingField) {
        this.routingField = routingField;
    }

    public boolean isPrimaryNode() {
        return primaryNode;
    }

    public void setPrimaryNode(boolean primaryNode) {
        this.primaryNode = primaryNode;
    }

    public String getCursorId() {
        return cursorId;
    }

    public void setCursorId(String cursorId) {
        this.cursorId = cursorId;
    }

    public Boolean getFirstCursor() {
        return firstCursor;
    }

    public void setFirstCursor(Boolean firstCursor) {
        this.firstCursor = firstCursor;
    }

    public boolean needRemote() {
        return needRemote;
    }

    public QueryParamCondition setNeedRemote(boolean needRemote) {
        this.needRemote = needRemote;
        return this;
    }

    public static QueryParamCondition create() {
        return new QueryParamCondition();
    }

    public QueryParamCondition select(List<String> select) {
        this.select = select;
        return this;
    }

    public QueryParamCondition highlightFields(List<String> highlightFields) {
        this.highlightFields = highlightFields;
        return this;
    }

    public QueryParamCondition preTags(String preTags) {
        this.preTags = preTags;
        return this;
    }

    public QueryParamCondition postTags(String postTags) {
        this.postTags = postTags;
        return this;
    }

    public QueryParamCondition highlightNum(int highlightNum) {
        this.highlightNum = highlightNum;
        return this;
    }

    public List<AggFunction> getAggFunctionList() {
        return aggFunctionList;
    }

    public void setAggFunctionList(List<AggFunction> aggFunctionList) {
        this.aggFunctionList = aggFunctionList;
    }

    public QueryParamCondition select(String select) {
        if (this.select == null) {
            this.select = new ArrayList<>();
        }
        this.select.add(select);
        return this;
    }


    public List<String> getPartition() {
        return partition;
    }

    public void setPartition(List<String> partition) {
        this.partition = partition;
    }

    public QueryParamCondition where(List<Where> where) {
        this.where = where;
        return this;
    }

    public QueryParamCondition where(Where where) {
        if (this.where == null) {
            this.where = new ArrayList<>();
        }
        this.where.add(where);
        return this;
    }

    public QueryParamCondition group(List<String> group) {
        this.group = group;
        return this;
    }

    public QueryParamCondition group(String group) {
        if (this.group == null) {
            this.group = new ArrayList<>();
        }
        this.group.add(group);
        return this;
    }

    @Deprecated
    public QueryParamCondition aggs(Aggs aggs) {
        this.aggs = aggs;
        return this;
    }

    public QueryParamCondition aggregate(Aggs agg) {
        if (this.aggList == null) {
            this.aggList = new ArrayList<>();
        }
        this.aggList.add(agg);
        return this;
    }

    public QueryParamCondition aggFunction(AggFunction aggFunction) {
        if (this.aggFunctionList == null) {
            this.aggFunctionList = new ArrayList<>();
        }
        this.aggFunctionList.add(aggFunction);
        return this;
    }

    public QueryParamCondition order(List<Order> order) {
        this.order = order;
        return this;
    }

    public QueryParamCondition order(Order order) {
        if (this.order == null) {
            this.order = new ArrayList<>();
        }
        this.order.add(order);
        return this;
    }

    public QueryParamCondition page(Page page) {
        this.page = page;
        return this;
    }

    public QueryParamCondition countFlag(boolean countFlag) {
        this.countFlag = countFlag;
        return this;
    }

    public List<String> getSelect() {
        return select;
    }

    public void setSelect(List<String> select) {
        this.select = select;
    }

    public List<Where> getWhere() {
        return where;
    }

    public void setWhere(List<Where> where) {
        this.where = where;
    }

    public List<String> getGroup() {
        return group;
    }

    public void setGroup(List<String> group) {
        this.group = group;
    }

    public Aggs getAggs() {
        return aggs;
    }

    public void setAggs(Aggs aggs) {
        this.aggs = aggs;
    }

    public List<Order> getOrder() {
        return order;
    }

    public void setOrder(List<Order> order) {
        this.order = order;
    }

    public Page getPage() {
        return page;
    }

    public void setPage(Page page) {
        this.page = page;
    }

    public boolean isCountFlag() {
        return countFlag;
    }

    public void setCountFlag(boolean countFlag) {
        this.countFlag = countFlag;
    }

    @Override
    public String toString() {
        return "ParamCondition{" +
                ", select=" + select +
                ", where=" + where +
                ", order=" + order +
                ", page=" + page +
                '}';
    }

    public boolean isLimitDate() {
        return isLimitDate;
    }

    public void setLimitDate(boolean limitDate) {
        isLimitDate = limitDate;
    }

    public Boolean getAsync() {
        return isAsync;
    }

    public void setAsync(Boolean async) {
        isAsync = async;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public boolean isLimitReadTime() {
        return limitReadTime;
    }

    public void setLimitReadTime(boolean limitReadTime) {
        this.limitReadTime = limitReadTime;
    }

    public Integer getMaxVersion() {
        return maxVersion;
    }

    public void setMaxVersion(Integer maxVersion) {
        this.maxVersion = maxVersion;
    }

    public Boolean isNeedWhere() {
        return needWhere;
    }

    public void setNeedWhere(boolean needWhere) {
        this.needWhere = needWhere;
    }

    public boolean isFromFwFlag() {
        return fromFwFlag;
    }

    public void setFromFwFlag(boolean fromFwFlag) {
        this.fromFwFlag = fromFwFlag;
    }

    public boolean isFromWsFlag() {
        return fromWsFlag;
    }

    public void setFromWsFlag(boolean fromWsFlag) {
        this.fromWsFlag = fromWsFlag;
    }

    public boolean isNeedRemote() {
        return needRemote;
    }

    public Integer getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(Integer batchSize) {
        this.batchSize = batchSize;
    }

    public Boolean getCursorRollAllData() {
        return cursorRollAllData;
    }

    public void setCursorRollAllData(Boolean cursorRollAllData) {
        this.cursorRollAllData = cursorRollAllData;
    }

    public String getBusinessTime() {
        return businessTime;
    }

    public void setBusinessTime(String businessTime) {
        this.businessTime = businessTime;
    }

    public List<Aggs> getAggList() {
        return aggList;
    }

    public void setAggList(List<Aggs> aggList) {
        this.aggList = aggList;
    }

    /**
     * 重写Object.clone 实现深度克隆
     *
     * @return
     * @throws CloneNotSupportedException
     */
    @Override
    public Object clone() throws CloneNotSupportedException {
        String objectToStr = JsonUtil.objectToStr(this);
        return JsonUtil.jsonStrToObject(objectToStr, QueryParamCondition.class);
    }

    public Object getDsl() {
        return dsl;
    }

    public void setDsl(Object dsl) {
        this.dsl = dsl;
    }

    public AssociatedQuery getAssociatedQuery() {
        return associatedQuery;
    }

    public void setAssociatedQuery(AssociatedQuery associatedQuery) {
        this.associatedQuery = associatedQuery;
    }

    public int getHighlightNum() {
        return highlightNum;
    }

    public void setHighlightNum(int highlightNum) {
        this.highlightNum = highlightNum;
    }

    public String getPreTags() {
        return preTags;
    }

    public void setPreTags(String preTags) {
        this.preTags = preTags;
    }

    public String getPostTags() {
        return postTags;
    }

    public void setPostTags(String postTags) {
        this.postTags = postTags;
    }

    public List<String> getHighlightFields() {
        return highlightFields;
    }

    public void setHighlightFields(List<String> highlightFields) {
        this.highlightFields = highlightFields;
    }

    public Hint getHint() {
        return hint;
    }

    public void setHint(Hint hint) {
        this.hint = hint;
    }
}
