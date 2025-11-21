package com.meiya.whalex.interior.db.operation.out;


import com.meiya.whalex.interior.db.constant.ReturnCodeEnum;

/**
 * API 接口组件操作统一返回实体
 *
 * @author Huanghesen
 * @date 2018/9/10
 * @project whale-cloud-api
 */
public class DbCursorResult extends DbHandleResult {

    /**
     * 游标ID
     */
    private String cursorId;

    public String getCursorId() {
        return cursorId;
    }

    public void setCursorId(String cursorId) {
        this.cursorId = cursorId;
    }

    public DbCursorResult() {
    }

    public DbCursorResult(int returnCode, String errorMsg) {
        super(returnCode, errorMsg);
    }

    public DbCursorResult(ReturnCodeEnum returnCodeEnum) {
        super(returnCodeEnum);
    }
}
