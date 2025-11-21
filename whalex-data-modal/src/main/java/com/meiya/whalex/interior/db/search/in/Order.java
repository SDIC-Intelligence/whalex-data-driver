package com.meiya.whalex.interior.db.search.in;

import com.meiya.whalex.interior.db.search.condition.Sort;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import java.io.Serializable;

/**
 * 排序参数，默认升序
 */
@ApiModel(value = "排序参数")
public class Order implements Serializable {

    @ApiModelProperty(value = "排序字段")
    private String field;
    @ApiModelProperty(value = "排序类型")
    private Sort sort = Sort.ASC;
    @ApiModelProperty(value = "nulls排序")
    private Sort nullSort;

    public Order() {
    }

    public Order(String field) {
        this.field = field;
    }

    public Order(String field, Sort sort) {
        this.field = field;
        this.sort = sort;
    }

    public Order(String field, Sort sort, Sort nullSort) {
        this.field = field;
        this.sort = sort;
        this.nullSort = nullSort;
    }

    public static Order create(String field) {
        return new Order(field);
    }

    public static Order create(String field, Sort sort) {
        return new Order(field, sort);
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public Sort getSort() {
        return sort;
    }

    public void setSort(Sort sort) {
        this.sort = sort;
    }

    public Sort getNullSort() {
        return nullSort;
    }

    public void setNullSort(Sort nullSort) {
        this.nullSort = nullSort;
    }

    @Override
    public String toString() {
        return "Order{" +
                "field='" + field + '\'' +
                ", sort=" + sort +
                '}';
    }
}
