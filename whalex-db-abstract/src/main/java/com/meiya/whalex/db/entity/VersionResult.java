package com.meiya.whalex.db.entity;

/**
 * 字段版本查询结果封装
 *
 * @author 黄河森
 * @date 2019/12/26
 * @project whale-cloud-platformX
 */
public class VersionResult {

    /**
     * 原始值
     */
    private Object value;

    /**
     * 版本号
     */
    private Long version;

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public Long getVersion() {
        return version;
    }

    public void setVersion(Long version) {
        this.version = version;
    }

    public VersionResult() {
    }

    public VersionResult(Object value, Long version) {
        this.value = value;
        this.version = version;
    }
}
