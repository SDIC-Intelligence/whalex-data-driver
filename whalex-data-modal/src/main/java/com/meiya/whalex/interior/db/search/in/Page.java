package com.meiya.whalex.interior.db.search.in;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import java.io.Serializable;

/**
 * 分页
 *
 * @author 黄河森
 * @date 2019/9/10
 * @project whale-cloud-platformX
 */
@ApiModel(value = "分页参数")
public class Page implements Serializable {

    /**
     * 查询全部数据
     */
    public static Integer LIMIT_ALL_DATA = -1;

    /**
     * 偏移
     */
    @ApiModelProperty(value = "偏移量", notes = "默认偏移量 0")
    private Integer offset;

    /**
     * 条数
     */
    @ApiModelProperty(value = "返回数据量", notes = "默认条数 10")
    private Integer limit;

    public Page() {
        this.offset = 0;
        this.limit = 10;
    }

    public Page(Integer offset, Integer limit) {
        this.offset = offset;
        this.limit = limit;
    }

    public Page(Integer limit) {
        this.offset = 0;
        this.limit = limit;
    }

    public static Page create() {
        return new Page();
    }

    public static Page create(Integer offset, Integer limit) {
        return new Page(offset, limit);
    }

    public Page offset(Integer offset) {
        this.offset = offset;
        return this;
    }

    public Page limit(Integer limit) {
        this.limit = limit;
        return this;
    }

    public Integer getOffset() {
        return offset;
    }

    public void setOffset(Integer offset) {
        this.offset = offset;
    }

    public Integer getLimit() {
        return limit;
    }

    public void setLimit(Integer limit) {
        this.limit = limit;
    }

    @Override
    public String toString() {
        return "Page{" +
                "offset=" + offset +
                ", limit=" + limit +
                '}';
    }
}
