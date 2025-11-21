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
@ApiModel(value = "数据查询请求报文")
@Data
public class DBQueryCondition extends BaseQuery<DBQueryCondition> implements Serializable {

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
     * 资源编码(为了兼容 后续全面使用resourceId后删掉)
     */
    private String resourceid;

    /**
     * 子集的资源编码
     */
    @ApiModelProperty(value = "子资源标识符")
    private String childResourceId;

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

    @ApiModelProperty(value = "是否执行联合查询", allowableValues = "true, false", notes = "对于 ES + HBase 或者 Solr + HBase 的资源，进行联合查询")
    private boolean uniteQuery;

    @ApiModelProperty(value = "事务id")
    private String transactionId;

    @ApiModelProperty(value = "是否开启事务")
    private boolean openTransaction;

    @ApiModelProperty(value = "提交事务")
    private boolean commitTransaction;

    protected final static ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .setSerializationInclusion(JsonInclude.Include.NON_NULL)
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    public static DBQueryCondition create() {
        return new DBQueryCondition();
    }

    public DBQueryCondition resourceId(String resourceId) {
        this.resourceId = resourceId;
        return this;
    }

    public DBQueryCondition dbType(String dbType) {
        this.dbType = dbType;
        return this;
    }

    public DBQueryCondition role(String role) {
        super.role = role;
        return this;
    }

    public DBQueryCondition token(String token) {
        super.token = token;
        return this;
    }

    public DBQueryCondition params(QueryParamCondition params) {
        this.params = params;
        return this;
    }

    public static DBQueryCondition fromJson(String jsonStr) {
        try {
            return OBJECT_MAPPER.readValue(jsonStr, DBQueryCondition.class);
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
        //为了兼容已有旧的调用者使用resourceid的情况
        return StringUtils.isNotBlank(this.resourceId) ? this.resourceId : this.resourceid;
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

    public String getResourceid() {
        return resourceid;
    }

    public DBQueryCondition setResourceid(String resourceid) {
        this.resourceid = resourceid;
        return this;
    }

    public DBQueryCondition setResourceId(String resourceId) {
        this.resourceId = resourceId;
        return this;
    }

    public boolean isUniteQuery() {
        return uniteQuery;
    }

    public void setUniteQuery(boolean uniteQuery) {
        this.uniteQuery = uniteQuery;
    }
}
