package com.meiya.whalex.interior.db.operation.in;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.meiya.whalex.interior.base.BaseQuery;
import com.meiya.whalex.interior.db.search.in.QueryParamCondition;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import javax.validation.constraints.NotBlank;
import java.io.IOException;

/**
 * 根据 数据库表名称查询资源 请求报文
 *
 * @author 黄河森
 * @date 2019/9/26
 * @project whale-cloud-platformX
 */
@ApiModel(value = "数据库表名称查询资源请求报文")
public class TableNameQueryCondition extends BaseQuery {

    /**
     * 数据集
     */
    @NotBlank(message = "数据库表名不能为空")
    @ApiModelProperty(value = "数据库表名", required = true)
    private String tableName;

    /**
     * 查询参数
     */
    @ApiModelProperty(value = "查询条件")
    private QueryParamCondition params;

    /**
     * 资源目录存在多个同组建部署定义，标识组件类型
     * 01 接入 02 计算 03抽取 04下载 00未指定
     */
    @ApiModelProperty(value = "服务类型", allowableValues = "01 接入 02 计算 03抽取 04下载 00未指定")
    private String serverType;

    /**
     * 资源目录存在多个同组建部署定义，标识查询时候对哪个组建进行查询
     * 00 为默认查询组件
     */
    @ApiModelProperty(value = "查询类型", allowableValues = "00 默认查询组件")
    private String queryType;

    protected final static ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .setSerializationInclusion(JsonInclude.Include.NON_NULL)
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    public static TableNameQueryCondition create() {
        return new TableNameQueryCondition();
    }

    public TableNameQueryCondition role(String role) {
        super.role = role;
        return this;
    }

    public TableNameQueryCondition token(String token) {
        super.token = token;
        return this;
    }

    public TableNameQueryCondition params(QueryParamCondition params) {
        this.params = params;
        return this;
    }

    public TableNameQueryCondition tableName(String tableName) {
        this.tableName = tableName;
        return this;
    }


    public static TableNameQueryCondition fromJson(String jsonStr) {
        try {
            return OBJECT_MAPPER.readValue(jsonStr, TableNameQueryCondition.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public String toJsonStr() {
        try {
            return OBJECT_MAPPER.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public QueryParamCondition getParams() {
        return params;
    }

    public void setParams(QueryParamCondition params) {
        this.params = params;
    }

    public String getServerType() {
        return serverType;
    }

    public void setServerType(String serverType) {
        this.serverType = serverType;
    }

    public String getQueryType() {
        return queryType;
    }

    public void setQueryType(String queryType) {
        this.queryType = queryType;
    }

    @Override
    public String toString() {
        return "DbTableQueryCondition{" +
                "tableName='" + tableName + '\'' +
                ", params=" + params +
                ", serverType='" + serverType + '\'' +
                ", queryType='" + queryType + '\'' +
                ", role='" + role + '\'' +
                ", token='" + token + '\'' +
                '}';
    }
}
