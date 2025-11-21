package com.meiya.whalex.interior.db.search.in;

import com.meiya.whalex.interior.db.constant.CommonConstant;
import com.meiya.whalex.interior.db.search.condition.Rel;
import com.meiya.whalex.util.JsonUtil;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * sql的where条件
 * 如果是最后一层key-value，赋值field-param即可
 * 如果还有内层参数，赋值给params接口，并制定这些参数之间的关系and,or
 *
 * and,or 配合params使用
 * 剩下的关系配合field，param 使用
 */
@ApiModel(value = "查询条件操作参数")
public class Where implements Cloneable, Serializable {

    @ApiModelProperty(value = "嵌套查询条件")
    private List<Where> params;
    @ApiModelProperty(value = "条件字段名")
    private String field;
    @ApiModelProperty(value = "条件字段值")
    private Object param;
    @ApiModelProperty(value = "字段权重")
    private int boost = 1;
    @ApiModelProperty(value = "别名(图库)")
    private String alias;
    /**
     * 描述param类型，现在有：date（mongo适配了）
     */
    @ApiModelProperty(value = "条件字段值类型", allowableValues = "objectId, date")
    private String paramType;

    @ApiModelProperty(value = "操作符")
    private Rel type = Rel.AND;

    @ApiModelProperty(value = "es multi_match操作符，默认是or, 可选值[and, or]")
    private String operator;

    @ApiModelProperty(value = "es multi_match查询方式，可选值[best_fields, most_fields, cross_fields, phrase, phrase_prefix, bool_prefix]")
    private String queryType;

    @ApiModelProperty(value = "时间格式条件字段格式化类型", allowableValues = "yyyy-MM-dd HH:mm:ss, yyyy-MM-dd, yyyy-MM-dd'T'HH:mm:ss.000'Z', yyyy-MM-dd'T'HH:mm:ss'Z'")
    private String paramFormat;

    public static Where create() {
        return new Where();
    }

    public static Where create(Rel rel){
        Where where = create();
        where.type = rel;
        return where;
    }

    /**
     * 创建包装（外层）查询条件
     * @param params
     * @param rel and,or等
     * @return
     */
    public static Where create(List<Where> params, Rel rel){
        Where where = create(rel);
        where.params = params;
        return where;
    }

    /**
     * 创建底层查询条件，field=param
     * @param field
     * @param param
     * @return
     */
    public static Where create(String field, Object param) {
        return create(field, param, Rel.EQ);
    }

    public static Where builder(String field, Object param,String alias) {
        Where where = create();
        where.field = field;
        where.param = param;
        where.alias = alias;
        where.setType(Rel.EQ);
        return where;
    }

    public static Where builder(String alias, Object param) {
        return builder(alias,param,Rel.EQ);
    }

    public static Where builder(String alias, Object param,Rel type) {
        Where where = create();
        where.param = param;
        where.alias = alias;
        where.type = type;
        return where;
    }

    public static Where builder(Object param) {
        Where where = create();
        where.param = param;
        return where;
    }

    public static Where builder(String field,Object param,String alias,Rel type){
        Where where = create();
        where.field = field;
        where.param = param;
        where.alias = alias;
        where.type = type;
        return where;
    }

    /**
     * 创建底层查询条件，field[type]param,type:>,<  等等
     * @param field
     * @param param
     * @param type
     * @return
     */
    public static Where create(String field, Object param, Rel type) {
        return create(field, param, null, type);
    }

    public static Where create(String field, Object param, String paramType) {
        return create(field, param, paramType, Rel.EQ);
    }

    public static Where create(String field, Object param, String paramType, String paramFormat) {
        return create(field, param, paramType, Rel.EQ, paramFormat);
    }

    public static Where create(String field, Object param, String paramType, Rel type) {
        Where where = create();
        where.field = field;
        where.param = param;
        where.paramType = paramType;
        where.type = type;
        return where;
    }

    public static Where create(String field, Object param, Rel type, String paramFormat) {
        Where where = create();
        where.field = field;
        where.param = param;
        where.type = type;
        where.paramFormat = paramFormat;
        return where;
    }

    public static Where create(String field, Object param, Rel type, String queryType, String operator) {
        Where where = create();
        where.field = field;
        where.param = param;
        where.type = type;
        where.queryType = queryType;
        where.operator = operator;
        return where;
    }

    public static Where create(String field, Object param, String paramType, Rel type, String paramFormat) {
        Where where = create();
        where.field = field;
        where.param = param;
        where.paramType = paramType;
        where.type = type;
        where.paramFormat = paramFormat;
        return where;
    }

    public static Where create(String field, Object param, String paramType, Rel type, String paramFormat, int boost) {
        Where where = create();
        where.field = field;
        where.param = param;
        where.paramType = paramType;
        where.type = type;
        where.paramFormat = paramFormat;
        where.boost = boost;
        return where;
    }

    public static List<Where> createParams() {
        return new ArrayList<>();
    }

    public void addWhere(String field, Object param, Rel type) {
        addWhere(field, param, null, type);
    }

    public void addWhere(String field, Object param, String paramType, Rel type) {
        addWhere(field, param, paramType, type, null);
    }

    public void addWhere(String field, Object param, Rel type, String paramFormat) {
        addWhere(field, param, null, type, paramFormat);
    }

    public void addWhere(String field, Object param, String paramType, Rel type, String paramFormat) {
        Where where = create(field, param, paramType, type, paramFormat);
        if (params == null) {
            params = createParams();
        }
        params.add(where);
    }

    public void addWhere(String field, Object param, Rel type, String queryType, String operator) {
        Where where = create(field, param, type, queryType, operator);
        if (params == null) {
            params = createParams();
        }
        params.add(where);
    }

    public void addWhere(String field, Object param, Rel type, String paramFormat, int boost) {
        addWhere(field, param, null, type, paramFormat, boost);
    }

    public void addWhere(String field, Object param, String paramType, Rel type, String paramFormat, int boost) {
        Where where = create(field, param, paramType, type, paramFormat, boost);
        if (params == null) {
            params = createParams();
        }
        params.add(where);
    }

    /**
     * 创建范围查询
     * @param filed
     * @param startVal
     * @param endVal
     * @return
     */
    public static Where createRange(String filed, Object startVal, Object endVal) {
        List<Where> rangeWhere = createParams();
        Where gtWhere = create(filed, startVal, Rel.GT);
        rangeWhere.add(gtWhere);
        Where ltWhere = create(filed, endVal, Rel.LT);
        rangeWhere.add(ltWhere);
        return create(rangeWhere, Rel.AND);
    }

    /**
     * 创建范围查询
     * @param filed
     * @param startVal
     * @param endVal
     * @return
     */
    public static Where createRange(String filed, Object startVal, Object endVal, String paramFormat) {
        List<Where> rangeWhere = createParams();
        Where gtWhere = create(filed, startVal, Rel.GT, paramFormat);
        rangeWhere.add(gtWhere);
        Where ltWhere = create(filed, endVal, Rel.LT, paramFormat);
        rangeWhere.add(ltWhere);
        return create(rangeWhere, Rel.AND);
    }

    /**
     * 创建分片时间
     * @param startVal
     * @param endVal
     * @return
     */
    public static Where createBurstZone(Object startVal, Object endVal) {
        return createRange(CommonConstant.BURST_ZONE, startVal, endVal);
    }

    /**
     * 创建 facet.query 范围查询
     * @param filed
     * @param startVal
     * @param endVal
     * @return
     */
    public static Where createRangeForFacetQuery(String filed, Object startVal, Object endVal) {
        List<Where> rangeWhere = createParams();
        Where gtWhere = create(filed, startVal, Rel.GT);
        rangeWhere.add(gtWhere);
        Where ltWhere = create(filed, endVal, Rel.LT);
        rangeWhere.add(ltWhere);
        return create(rangeWhere, Rel.FACET_QUERY);
    }

    /**
     * 创建 过滤 范围查询
     * @param filed
     * @param startVal
     * @param endVal
     * @return
     */
    public static Where createRangeForFilterQuery(String filed, Object startVal, Object endVal) {
        List<Where> rangeWhere = createParams();
        Where gtWhere = create(filed, startVal, Rel.GT);
        rangeWhere.add(gtWhere);
        Where ltWhere = create(filed, endVal, Rel.LT);
        rangeWhere.add(ltWhere);
        return create(rangeWhere, Rel.FQ);
    }

    public Where params(List<Where> params) {
        this.params = params;
        return this;
    }

    public Where field(String field) {
        this.field = field;
        return this;
    }

    public Where param(Object param) {
        this.param = param;
        return this;
    }

    public Where type(Rel type) {
        this.type = type;
        return this;
    }

    public List<Where> getParams() {
        return params;
    }

    public void setParams(List<Where> params) {
        this.params = params;
    }

    public Rel getType() {
        return type;
    }

    public void setType(Rel type) {
        this.type = type;
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public Object getParam() {
        return param;
    }

    public void setParam(Object param) {
        this.param = param;
    }

    public String getParamType() {
        return paramType;
    }

    public void setParamType(String paramType) {
        this.paramType = paramType;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public String getParamFormat() {
        return paramFormat;
    }

    public void setParamFormat(String paramFormat) {
        this.paramFormat = paramFormat;
    }

    public int getBoost() {
        return boost;
    }

    public void setBoost(int boost) {
        this.boost = boost;
    }

    public String getOperator() {
        return operator;
    }

    public void setOperator(String operator) {
        this.operator = operator;
    }

    public String getQueryType() {
        return queryType;
    }

    public void setQueryType(String queryType) {
        this.queryType = queryType;
    }

    @Override
    public String toString() {
        return "Where{" +
                "params=" + params +
                ", field='" + field + '\'' +
                ", param=" + param +
                ", type=" + type +
                '}';
    }

    @Override
    protected Object clone() throws CloneNotSupportedException {
        String objectToStr = JsonUtil.objectToStr(this);
        return JsonUtil.jsonStrToObject(objectToStr, Where.class);
    }
}
