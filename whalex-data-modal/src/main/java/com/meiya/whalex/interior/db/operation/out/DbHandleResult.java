package com.meiya.whalex.interior.db.operation.out;


import com.meiya.whalex.interior.base.BaseResult;
import com.meiya.whalex.interior.db.constant.ReturnCodeEnum;

import java.util.List;
import java.util.Map;

/**
 * API 接口组件操作统一返回实体
 *
 * @author Huanghesen
 * @date 2018/9/10
 * @project whale-cloud-api
 */
public class DbHandleResult extends BaseResult {

    private long total;

    private List<Map<String, Object>> data;

    private String transactionId;

    public List<Map<String, Object>> getData() {
        return data;
    }

    public void setData(List<Map<String, Object>> data) {
        this.data = data;
    }

    public long getTotal() {
        return total;
    }

    public void setTotal(long total) {
        this.total = total;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public DbHandleResult() {
    }

    public DbHandleResult(int returnCode, String errorMsg) {
        super(returnCode, errorMsg);
    }

    public DbHandleResult(ReturnCodeEnum returnCodeEnum) {
        super(returnCodeEnum);
    }
}
