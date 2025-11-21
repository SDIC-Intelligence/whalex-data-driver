package com.meiya.whalex.interior.db.operation.in;

import com.meiya.whalex.interior.base.BaseQuery;
import com.meiya.whalex.interior.db.search.in.Where;
import com.meiya.whalex.interior.db.search.out.FieldEntity;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import javax.validation.constraints.NotBlank;
import java.util.List;

/**
 * 数据更新请求报文
 *
 * @author 黄河森
 * @date 2019/11/28
 * @project whale-cloud-platformX
 */
@ApiModel("数据更新请求报文")
public class DbUpdateOperation extends BaseQuery {

    @ApiModelProperty(value = "资源标识符", required = true)
    @NotBlank(message = "资源标识符不能为空")
    private String resourceId;

    @ApiModelProperty(value = "组件类型")
    private String dbType;

    @ApiModelProperty(value = "数据库表标识符")
    private String dbId;

    @ApiModelProperty(value = "新增数据", required = true)
    @NotBlank(message = "新增数据不能为空")
    private List<FieldEntity> record;

    /**
     * 查询参数
     */
    @ApiModelProperty(value = "更新条件")
    private List<Where> where;

    /**
     * 当更新记录不存在时，是否插入
     * true 为 不存在时就插入
     * false 为 不存在时不插入（默认）
     */
    @ApiModelProperty(value = "更新操作", notes = "当更新记录不存在时，是否插入")
    private Boolean upsert = Boolean.FALSE;

    /**
     * 当有多条匹配的更新行时，是否只更新第一条
     * true 为 全部更新
     * false 为只更新第一天（默认）
     */
    @ApiModelProperty(value = "更新操作", notes = "当有多条匹配的更新行时，是否只更新第一条")
    private Boolean multi = Boolean.FALSE;

    /**
     * 是否异步操作（默认实时）
     */
    @ApiModelProperty(value = "是否异步操作")
    private Boolean isAsync = Boolean.FALSE;

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

    public List<FieldEntity> getRecord() {
        return record;
    }

    public void setRecord(List<FieldEntity> record) {
        this.record = record;
    }

    public List<Where> getWhere() {
        return where;
    }

    public void setWhere(List<Where> where) {
        this.where = where;
    }

    public Boolean getUpsert() {
        return upsert;
    }

    public void setUpsert(Boolean upsert) {
        this.upsert = upsert;
    }

    public Boolean getMulti() {
        return multi;
    }

    public void setMulti(Boolean multi) {
        this.multi = multi;
    }

    public Boolean getAsync() {
        return isAsync;
    }

    public void setAsync(Boolean async) {
        isAsync = async;
    }
}
