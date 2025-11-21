package com.meiya.whalex.db.entity;

import java.util.List;
import java.util.Map;

/**
 * 组件内部查询方法统一返参
 *
 * @author 黄河森
 * @date 2019/9/14
 * @project whale-cloud-platformX
 */
public class QueryMethodResult {

    /**
     * 执行结果
     */
    private boolean success = Boolean.TRUE;

    /**
     * 统计数据量
     */
    private long total;

    /**
     * 查询结果
     */
    private List<Map<String, Object>> rows;

    public long getTotal() {
        return total;
    }

    public void setTotal(long total) {
        this.total = total;
    }

    public List<Map<String, Object>> getRows() {
        return rows;
    }

    public void setRows(List<Map<String, Object>> rows) {
        this.rows = rows;
    }

    public QueryMethodResult() {
    }

    public QueryMethodResult(long total, List<Map<String, Object>> rows) {
        this.total = total;
        this.rows = rows;
    }
    public QueryMethodResult(boolean success, long total, List<Map<String, Object>> rows) {
        this.success = success;
        this.total = total;
        this.rows = rows;
    }

    public QueryMethodResult(boolean success) {
        this.success = success;
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }
}
