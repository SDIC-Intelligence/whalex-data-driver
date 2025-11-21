package com.meiya.whalex.db.entity;

/**
 * 基础组件操作类
 *
 * @author 黄河森
 * @date 2019/10/18
 * @project whale-cloud-platformX
 */
public abstract class AbstractDbHandler {

    /**
     * 查询语句（用于操作记录，日志打印）
     */
    private String queryStr;

    public String getQueryStr() {
        return queryStr;
    }

    public void setQueryStr(String queryStr) {
        this.queryStr = queryStr;
    }
}
