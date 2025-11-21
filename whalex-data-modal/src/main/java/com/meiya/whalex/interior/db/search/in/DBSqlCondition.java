package com.meiya.whalex.interior.db.search.in;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.meiya.whalex.interior.base.BaseQuery;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.Serializable;

/**
 * 根据 资源标识符 请求报文
 *
 * @author 黄河森
 * @date 2019/9/26
 * @project whale-cloud-platformX
 */
@ApiModel(value = "SQL请求报文")
@Data
public class DBSqlCondition extends BaseQuery<DBSqlCondition> implements Serializable {

    public final static String ITEM_TYPE_ITEM_NAME = "01";
    public final static String ITEM_TYPE_ITEM_CODE = "02";
    public final static String ITEM_TYPE_STORE_COLUMN = "03";
    public final static String ITEM_TYPE_IDENTIFIER = "04";

    /**
     * 资源编码
     */
    @ApiModelProperty(value = "资源标识符")
    private String resourceId;

    /**
     * 存储组件
     */
    @ApiModelProperty(value = "查询组件类型")
    private String dbType;

    /**
     * 存储组件编号
     */
    @ApiModelProperty(value = "查询组件标识符")
    private String dbId;

    @ApiModelProperty("SQL语句")
    private String sql;

    /**
     * 查询参数
     */
    @ApiModelProperty(value = "查询条件")
    private Object[] params;

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

    @ApiModelProperty(value = "事务id")
    private String transactionId;

    @ApiModelProperty(value = "是否开启事务")
    private boolean openTransaction;

    @ApiModelProperty(value = "提交事务")
    private boolean commitTransaction;

    protected final static ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .setSerializationInclusion(JsonInclude.Include.NON_NULL)
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    public static DBSqlCondition create() {
        return new DBSqlCondition();
    }

    public DBSqlCondition resourceId(String resourceId) {
        this.resourceId = resourceId;
        return this;
    }

    public DBSqlCondition dbType(String dbType) {
        this.dbType = dbType;
        return this;
    }

    public DBSqlCondition role(String role) {
        super.role = role;
        return this;
    }

    public DBSqlCondition token(String token) {
        super.token = token;
        return this;
    }

    public DBSqlCondition params(Object[] params) {
        this.params = params;
        return this;
    }

    public static DBSqlCondition fromJson(String jsonStr) {
        try {
            return OBJECT_MAPPER.readValue(jsonStr, DBSqlCondition.class);
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

    public String getResourceId() {
        return this.resourceId;
    }

    @Override
    public String toString() {
        return "DBCondition{" +
                "resourceId='" + resourceId + '\'' +
                ", role='" + role + '\'' +
                ", token='" + token + '\'' +
                ", params=" + params +
                '}';
    }

    public DBSqlCondition setResourceId(String resourceId) {
        this.resourceId = resourceId;
        return this;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }
}
