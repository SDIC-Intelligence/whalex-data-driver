package com.meiya.whalex.db.entity;

import com.meiya.whalex.interior.base.BaseResult;
import com.meiya.whalex.interior.db.constant.ReturnCodeEnum;
import com.meiya.whalex.interior.db.search.out.DbQueryDataResult;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import java.util.List;

/**
 * 增删改响应结果
 *
 * @author 黄河森
 * @date 2020/1/8
 * @project whale-cloud-platformX
 */
@ApiModel(value = "数据变更接口返参实体")
public class DbMutateResult extends BaseResult {

    @ApiModelProperty("详细数据")
    private List<DbQueryDataResult.DataEntity> data;

    @ApiModelProperty(value = "事务id")
    private String transactionId;

    public DbMutateResult() {
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public DbMutateResult(int returnCode, String errorMsg) {
        super(returnCode, errorMsg);
    }

    public DbMutateResult(ReturnCodeEnum returnCodeEnum) {
        super(returnCodeEnum);
    }

    public DbMutateResult(List<DbQueryDataResult.DataEntity> data) {
        this.data = data;
    }

    public List<DbQueryDataResult.DataEntity> getData() {
        return data;
    }

    public DbMutateResult(int returnCode, String errorMsg, List<DbQueryDataResult.DataEntity> data) {
        super(returnCode, errorMsg);
        this.data = data;
    }
}
