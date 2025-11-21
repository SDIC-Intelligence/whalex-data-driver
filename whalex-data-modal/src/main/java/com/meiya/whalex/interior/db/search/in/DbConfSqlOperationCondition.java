package com.meiya.whalex.interior.db.search.in;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.meiya.whalex.interior.base.BaseQuery;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

import java.io.IOException;

/**
 * 根据 数据库信息查询数据 请求报文
 *
 * @author 黄河森
 * @date 2019/9/26
 * @project whale-cloud-platformX
 */
@ApiModel(value = "通过数据库信息SQL操作")
@Data
public class DbConfSqlOperationCondition extends BaseQuery {

    @ApiModelProperty(value = "数据库标识符")
    private String bigResourceId;


    @ApiModelProperty(value = "数据库类型")
    private String dbType;

    @ApiModelProperty(value = "数据库配置信息")
    private String connSetting;

    @ApiModelProperty(value = "SQL语句")
    private String sql;

    @ApiModelProperty(value = "数据库版本")
    private String version;

    @ApiModelProperty(value = "云厂商")
    private String cloudCode;

    /**
     * 查询参数
     */
    @ApiModelProperty(value = "查询条件")
    private Object[] params;

    @ApiModelProperty(value = "事务id")
    private String transactionId;

    @ApiModelProperty(value = "是否开启事务")
    private boolean openTransaction;

    @ApiModelProperty(value = "提交事务")
    private boolean commitTransaction;

    @ApiModelProperty(value = "标签")
    private String tag;

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    protected final static ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .setSerializationInclusion(JsonInclude.Include.NON_NULL)
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    public static DbConfSqlOperationCondition create() {
        return new DbConfSqlOperationCondition();
    }



    public DbConfSqlOperationCondition role(String role) {
        super.setRole(role);
        return this;
    }

    public DbConfSqlOperationCondition token(String token) {
        super.setToken(token);
        return this;
    }

    public DbConfSqlOperationCondition params(Object[] params) {
        this.params = params;
        return this;
    }

    public static DbConfSqlOperationCondition fromJson(String jsonStr) {
        try {
            return OBJECT_MAPPER.readValue(jsonStr, DbConfSqlOperationCondition.class);
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

}
