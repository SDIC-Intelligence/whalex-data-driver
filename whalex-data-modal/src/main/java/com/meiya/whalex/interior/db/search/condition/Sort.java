package com.meiya.whalex.interior.db.search.condition;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * 排序枚举
 *
 * @author 黄河森
 * @date 2019/9/10
 * @project whale-cloud-platformX
 */
public enum Sort {

    ASC("asc"), DESC("desc");

    private final String name;

    Sort(String name) {
        this.name = name;
    }

    @JsonValue
    public String getName() {
        return name;
    }

    @JsonCreator
    public static Sort parse(String name) {
        switch (name) {
            case "asc":
            case "ASC":
                return Sort.ASC;
            case "desc":
            case "DESC":
                return Sort.DESC;
            default:
                return Sort.DESC;
        }
    }

    @Override
    public String toString() {
        return getName();
    }
}

