package com.meiya.whalex.db.entity;


import com.meiya.whalex.interior.db.constant.ReturnCodeEnum;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * 组件服务统一返回参数
 *
 * @author 黄河森
 * @date 2019/9/13
 * @project whale-cloud-platformX
 */
public class PageResult implements Serializable {

    private long total;
    private List rows = new ArrayList(1);
    private Boolean success = true;
    private int code = 0;
    private String message = "成功";

    /**
     * 游标ID
     */
    private String cursorId;

    public PageResult(Boolean success) {
        this.success = success;
        if (!success) {
            this.code = 400;
        }
        this.total = 0;
        this.rows = new ArrayList();
    }

    public PageResult(Boolean success, String message) {
        this.success = success;
        if (!success) {
            this.code = 400;
        }
        this.message = message;
        this.total = 0;
        this.rows = new ArrayList();
    }

    public PageResult(Boolean success, int code, String message) {
        this.success = success;
        this.code = code;
        this.message = message;
        this.total = 0;
        this.rows = new ArrayList();
    }

    public PageResult(Boolean success, ReturnCodeEnum statusCodeEnum) {
        this.success = success;
        this.code = statusCodeEnum.getCode();
        this.message = statusCodeEnum.getMsg();
    }

    public PageResult() {
        this.success = true;
        this.total = 0;
        this.rows = new ArrayList();
    }

    public PageResult(long total, List rows) {
        this.success = true;
        this.total = total;
        this.rows = rows;
    }

    public PageResult(long total, List rows, Boolean success, ReturnCodeEnum statusCodeEnum) {
        this.total = total;
        this.rows = rows;
        this.success = success;
        this.code = statusCodeEnum.getCode();
        this.message = statusCodeEnum.getMsg();
    }

    public PageResult(long total, List rows, Boolean success, ReturnCodeEnum statusCodeEnum, String msg) {
        this.total = total;
        this.rows = rows;
        this.success = success;
        this.code = statusCodeEnum.getCode();
        this.message = String.format(statusCodeEnum.getMsg(), msg);
    }

    public long getTotal() {
        return total;
    }

    public void setTotal(long total) {
        this.total = total;
    }

    public List getRows() {
        return rows;
    }

    public void setRows(List rows) {
        this.rows = rows;
    }

    public Boolean getSuccess() {
        return success;
    }

    public void setSuccess(Boolean success) {
        this.success = success;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getCursorId() {
        return cursorId;
    }

    public void setCursorId(String cursorId) {
        this.cursorId = cursorId;
    }

    @Override
    public String toString() {
        return "PageResult{" +
                "total=" + total +
                ", rows=" + rows +
                ", success=" + success +
                ", code=" + code +
                ", message='" + message + '\'' +
                '}';
    }
}
