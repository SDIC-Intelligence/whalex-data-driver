package com.meiya.whalex.interior.db.search.in;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.meiya.whalex.interior.base.BaseQuery;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import org.apache.commons.lang3.StringUtils;

import javax.validation.constraints.NotBlank;
import java.io.IOException;

/**
 * 根据 数据库信息查询数据 请求报文
 *
 * @author 黄河森
 * @date 2019/9/26
 * @project whale-cloud-platformX
 */
@ApiModel(value = "通过数据库信息查询数据")
public class DbConfQueryCondition extends BaseQuery {

    @ApiModelProperty(value = "数据库标识符")
    private String bigResourceId;

    @ApiModelProperty(value = "兼容旧版本，同 bigResourceId", hidden = true)
    private String bigdataResourceId;

    /**
     * 数据集
     */
    @NotBlank(message = "数据库表名不能为空")
    @ApiModelProperty(value = "数据库表名", required = true)
    private String tableName;

    @ApiModelProperty(value = "数据库类型")
    private String dbType;

    @ApiModelProperty(value = "数据库配置信息")
    private String connSetting;

    @ApiModelProperty(value = "数据库原生查询语句")
    private Object dsl;

    @ApiModelProperty(value = "表配置信息，默认单表")
    private String tableJson = "{\"periodType\":\"only_one\"}";

    @ApiModelProperty(value = "数据库版本")
    private String version;

    @ApiModelProperty(value = "云厂商")
    private String cloudCode;

    @ApiModelProperty(value = "标签")
    private String tag;

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public String getCloudCode() {
        return cloudCode;
    }

    public void setCloudCode(String cloudCode) {
        this.cloudCode = cloudCode;
    }

    /**
     * 查询参数
     */
    @ApiModelProperty(value = "查询条件")
    private QueryParamCondition params;

    @ApiModelProperty(value = "事务id")
    private String transactionId;

    @ApiModelProperty(value = "是否开启事务")
    private boolean openTransaction;

    @ApiModelProperty(value = "提交事务")
    private boolean commitTransaction;

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    protected final static ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .setSerializationInclusion(JsonInclude.Include.NON_NULL)
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    public static DbConfQueryCondition create() {
        return new DbConfQueryCondition();
    }

    public String getTableJson() {
        return tableJson;
    }

    public void setTableJson(String tableJson) {
        this.tableJson = tableJson;
    }

    public Object getDsl() {
        return dsl;
    }

    public void setDsl(Object dsl) {
        this.dsl = dsl;
    }

    public DbConfQueryCondition role(String role) {
        super.setRole(role);
        return this;
    }

    public DbConfQueryCondition token(String token) {
        super.setToken(token);
        return this;
    }

    public DbConfQueryCondition params(QueryParamCondition params) {
        this.params = params;
        return this;
    }

    public static DbConfQueryCondition fromJson(String jsonStr) {
        try {
            return OBJECT_MAPPER.readValue(jsonStr, DbConfQueryCondition.class);
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

    public String getBigdataResourceId() {
        return bigdataResourceId;
    }

    public void setBigdataResourceId(String bigdataResourceId) {
        this.bigdataResourceId = bigdataResourceId;
        if (StringUtils.isBlank(this.bigResourceId) && StringUtils.isNotBlank(bigdataResourceId)) {
            this.bigResourceId = bigdataResourceId;
        }
    }

    public String getDbType() {
        return dbType;
    }

    public void setDbType(String dbType) {
        this.dbType = dbType;
    }

    public String getConnSetting() {
        return connSetting;
    }

    public void setConnSetting(String connSetting) {
        this.connSetting = connSetting;
    }

    public String getBigResourceId() {
        if (StringUtils.isBlank(this.bigResourceId)) {
            return this.bigdataResourceId;
        }
        return bigResourceId;
    }

    public void setBigResourceId(String bigResourceId) {
        this.bigResourceId = bigResourceId;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public boolean isOpenTransaction() {
        return openTransaction;
    }

    public void setOpenTransaction(boolean openTransaction) {
        this.openTransaction = openTransaction;
    }

    public boolean isCommitTransaction() {
        return commitTransaction;
    }

    public void setCommitTransaction(boolean commitTransaction) {
        this.commitTransaction = commitTransaction;
    }
}
