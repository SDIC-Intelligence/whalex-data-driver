package com.meiya.whalex.interior.db.search.in;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.meiya.whalex.interior.base.BaseQuery;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import javax.validation.constraints.NotBlank;
import java.io.IOException;

/**
 * 根据 数据来源查询资源 请求报文
 *
 * @author 黄河森
 * @date 2019/9/26
 * @project whale-cloud-platformX
 */
@ApiModel(value = "数据来源查询报文")
public class DataSourceQueryCondition extends BaseQuery {
    /**
     * 数据来源
     */
    @NotBlank(message = "数据来源不能为空")
    @ApiModelProperty(value = "数据来源", required = true)
    private String dataSource;

    /**
     * 存储组件
     */
    @ApiModelProperty(value = "组件类型")
    private String dbType;

    /**
     * 存储组件编号
     */
    @ApiModelProperty(value = "组件标识符")
    private String dbId;

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

    /**
     * 数据项类型
     * 01 英文编码
     *  02 标准编码
     *  03 英文缩写
     */
    @ApiModelProperty(value = "数据项类型", allowableValues = "01 英文编码, 02 标准编码, 03 英文缩写")
    private String itemType;

    protected final static ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .setSerializationInclusion(JsonInclude.Include.NON_NULL)
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    public static DataSourceQueryCondition create() {
        return new DataSourceQueryCondition();
    }

    public DataSourceQueryCondition dbType(String dbType) {
        this.dbType = dbType;
        return this;
    }

    public DataSourceQueryCondition role(String role) {
        super.role = role;
        return this;
    }

    public DataSourceQueryCondition token(String token) {
        super.token = token;
        return this;
    }

    public DataSourceQueryCondition params(QueryParamCondition params) {
        this.params = params;
        return this;
    }

    public static DataSourceQueryCondition fromJson(String jsonStr) {
        try {
            return OBJECT_MAPPER.readValue(jsonStr, DataSourceQueryCondition.class);
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

    public String getDataSource() {
        return dataSource;
    }

    public void setDataSource(String dataSource) {
        this.dataSource = dataSource;
    }

    public QueryParamCondition getParams() {
        return params;
    }

    public void setParams(QueryParamCondition params) {
        this.params = params;
    }

    public String getDbType() {
        return dbType;
    }

    public void setDbType(String dbType) {
        this.dbType = dbType;
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

    public String getDbId() {
        return dbId;
    }

    public void setDbId(String dbId) {
        this.dbId = dbId;
    }

    public String getItemType() {
        return itemType;
    }

    public void setItemType(String itemType) {
        this.itemType = itemType;
    }

    @Override
    public String toString() {
        return "DataSetQueryCondition{" +
                "dataSource='" + dataSource + '\'' +
                ", dbType='" + dbType + '\'' +
                ", dbId='" + dbId + '\'' +
                ", params=" + params +
                ", serverType='" + serverType + '\'' +
                ", queryType='" + queryType + '\'' +
                ", role='" + role + '\'' +
                ", token='" + token + '\'' +
                '}';
    }
}
