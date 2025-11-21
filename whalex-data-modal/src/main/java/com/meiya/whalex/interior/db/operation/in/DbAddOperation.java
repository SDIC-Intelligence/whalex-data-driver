package com.meiya.whalex.interior.db.operation.in;

import com.meiya.whalex.interior.base.BaseQuery;
import com.meiya.whalex.interior.db.search.out.FieldEntity;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.Size;
import java.util.List;

/**
 * 数据新增请求报文
 *
 * @author 黄河森
 * @date 2019/11/28
 * @project whale-cloud-platformX
 */
@ApiModel("数据新增请求报文")
public class DbAddOperation extends BaseQuery {

    @NotBlank(message = "资源标识符不能为空")
    @ApiModelProperty(value = "资源标识符", required = true)
    private String resourceId;

    @ApiModelProperty(value = "组件类型")
    private String dbType;

    @ApiModelProperty(value = "数据库表标识符")
    private String dbId;

    @ApiModelProperty(value = "新增数据", required = true)
    @Size(message = "新增数据不能为空", min = 1)
    private List<List<FieldEntity>> record;

    /**
     * solr 特殊参数
     */
    @ApiModelProperty(value = "新增数据时间段", notes = "仅以时间作为分表的组件需要使用该字段")
    private Long captureTime;

    /**
     * 是否异步操作（默认实时）
     */
    @ApiModelProperty(value = "操作是否异步")
    private Boolean isAsync = Boolean.FALSE;

    @ApiModelProperty(value = "是否立即提交", notes = "默认为false，建议由客户端自主决定")
    private Boolean commitNow = Boolean.FALSE;

    @ApiModelProperty(value = "是否允许插入覆盖已存在的数据", notes = "默认为true")
    private boolean overWrite = Boolean.TRUE;

    @ApiModelProperty(value = "事务id")
    private String transactionId;

    @ApiModelProperty(value = "是否开启事务")
    private boolean openTransaction;

    @ApiModelProperty(value = "提交事务")
    private boolean commitTransaction;

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

    public String getResourceId() {
        return resourceId;
    }

    public void setResourceId(String resourceId) {
        this.resourceId = resourceId;
    }

    public String getDbType() {
        return dbType;
    }

    public void setDbType(String dbType) {
        this.dbType = dbType;
    }

    public String getDbId() {
        return dbId;
    }

    public void setDbId(String dbId) {
        this.dbId = dbId;
    }

    public List<List<FieldEntity>> getRecord() {
        return record;
    }

    public void setRecord(List<List<FieldEntity>> record) {
        this.record = record;
    }

    public Long getCaptureTime() {
        return captureTime;
    }

    public void setCaptureTime(Long captureTime) {
        this.captureTime = captureTime;
    }

    public Boolean getAsync() {
        return isAsync;
    }

    public void setAsync(Boolean async) {
        isAsync = async;
    }

    public Boolean getCommitNow() {
        return commitNow;
    }

    public void setCommitNow(Boolean commitNow) {
        this.commitNow = commitNow;
    }

    public boolean isOverWrite() {
        return overWrite;
    }

    public void setOverWrite(boolean overWrite) {
        this.overWrite = overWrite;
    }
}
