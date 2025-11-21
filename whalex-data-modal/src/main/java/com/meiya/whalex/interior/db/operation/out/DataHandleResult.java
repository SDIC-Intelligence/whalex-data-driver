package com.meiya.whalex.interior.db.operation.out;

import com.meiya.whalex.interior.base.BaseResult;
import com.meiya.whalex.interior.db.constant.ReturnCodeEnum;
import lombok.Data;

@Data
public class DataHandleResult<T> extends BaseResult {

    private long total;
    private T data;
    private String transactionId;


    public DataHandleResult() {
    }

    public DataHandleResult(int returnCode, String errorMsg) {
        super(returnCode, errorMsg);
    }

    public DataHandleResult(ReturnCodeEnum returnCodeEnum) {
        super(returnCodeEnum);
    }
}

