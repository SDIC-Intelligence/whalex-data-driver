package com.meiya.whalex.db.constant;

/**
 *
 * 默认业务字段枚举
 *
 * @author 黄河森
 * @date 2020/3/20
 * @project whale-cloud-platformX
 */
public enum DefaultBusinessEnum {

    LAST_TIME("latm"),

    CAPTURE_TIME("catm");

    private String field;

    DefaultBusinessEnum(String field) {
        this.field = field;
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }
}
