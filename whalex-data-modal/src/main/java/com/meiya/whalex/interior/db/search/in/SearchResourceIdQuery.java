package com.meiya.whalex.interior.db.search.in;

import com.meiya.whalex.interior.base.BaseQuery;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import javax.validation.constraints.NotBlank;
import java.util.Arrays;

/**
 * 指定资源目录获取明细
 *
 * @author 黄河森
 * @date 2019/12/4
 * @project whale-cloud-platformX
 */
@ApiModel(value = "指定资源目录获取明细")
public class SearchResourceIdQuery extends BaseQuery {

    @ApiModelProperty(value = "查询值", required = true)
    @NotBlank(message = "查询值为空")
    private String value;

    @ApiModelProperty(value = "标准编码", required = true)
    @NotBlank(message = "标准编码为空")
    private String itemCode;

    @ApiModelProperty(value = "资源目录编码", required = true)
    @NotBlank(message = "资源目录编码为空")
    private String resourceId;

    @ApiModelProperty(value = "指定查询的存储来源编号")
    private String dbType;

    @ApiModelProperty(value = "排序字段，多个之间都好分割，排序和字段空格分割。默认asc，例如:a asc,b desc")
    private String order;

    @ApiModelProperty(value = "偏移量，默认0")
    private Integer offset;

    @ApiModelProperty(value = "查询条数，默认10，最大1000")
    private Integer limit;

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getItemCode() {
        return itemCode;
    }

    public void setItemCode(String itemCode) {
        this.itemCode = itemCode;
    }

    public String getResourceId() {
        return resourceId;
    }

    public void setResourceId(String resourceId) {
        this.resourceId = resourceId;
    }

    public String getOrder() {
        return order;
    }

    public void setOrder(String order) {
        this.order = order;
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
    public int hashCode() {
        return Arrays.deepHashCode(new Object[]{this.value, this.itemCode, this.resourceId, this.dbType, this.getRole(), this.order, this.offset, this.limit});
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof SearchResourceIdQuery)) {
            return false;
        }
        SearchResourceIdQuery other = (SearchResourceIdQuery) obj;
        return cusEquals(this.value, other.value)
                && cusEquals(this.itemCode, other.itemCode)
                && cusEquals(this.resourceId, other.resourceId)
                && cusEquals(this.dbType, other.dbType)
                && cusEquals(this.getRole(), this.getRole())
                && cusEquals(this.order, this.order)
                && cusEquals(this.offset, other.offset)
                && cusEquals(this.limit, other.limit);
    }

    private boolean cusEquals(String a, String b) {
        if (a == null && b == null) {
            return true;
        }
        if (a == null) {
            return false;
        }
        return a.equals(b);
    }

    private boolean cusEquals(Integer a, Integer b) {
        if (a == null) {
            if (b == null || b == 0) {
                return true;
            }
            return false;
        }
        if (b == null && a == 0) {
            return true;
        }
        return a.equals(b);
    }

    public String getDbType() {
        return dbType;
    }

    public void setDbType(String dbType) {
        this.dbType = dbType;
    }

}
