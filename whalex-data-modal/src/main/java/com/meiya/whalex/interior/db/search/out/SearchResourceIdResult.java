package com.meiya.whalex.interior.db.search.out;

import com.meiya.whalex.interior.base.BaseResult;
import com.meiya.whalex.interior.db.constant.ReturnCodeEnum;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import java.io.Serializable;
import java.util.List;

/**
 * 指定资源目录获取明细响应报文
 *
 * @author 黄河森
 * @date 2019/12/4
 * @project whale-cloud-platformX
 */
@ApiModel(value = "指定资源目录获取明细响应报文")
public class SearchResourceIdResult extends BaseResult {

    @ApiModelProperty("命中数据总大小")
    private Long total;

    @ApiModelProperty("data数据大小")
    private Long rows;

    @ApiModelProperty("数据明细")
    private List<List<Data>> data;

    public SearchResourceIdResult() {
    }

    public SearchResourceIdResult(Long total, List<List<Data>> data) {
        this.total = total;
        this.data = data;
        if (data != null) {
            this.rows = (long) data.size();
        }
    }

    public SearchResourceIdResult(int returnCode, String errorMsg) {
        super(returnCode, errorMsg);
    }

    public SearchResourceIdResult(ReturnCodeEnum returnCodeEnum) {
        super(returnCodeEnum);
    }

    public Long getTotal() {
        return total;
    }

    public void setTotal(Long total) {
        this.total = total;
    }

    public Long getRows() {
        return rows;
    }

    public void setRows(Long rows) {
        this.rows = rows;
    }

    public List<List<Data>> getData() {
        return data;
    }

    public void setData(List<List<Data>> data) {
        this.data = data;
        if (data != null) {
            this.rows = (long) data.size();
        }
    }

    @ApiModel("QuickSearchResourceIdProtocolOut.Data")
    public static class Data implements Serializable {

        @ApiModelProperty("字段")
        private String field;

        @ApiModelProperty("值")
        private Object value;

        @ApiModelProperty("说明字段值类型")
        private String fieldType;

        @ApiModelProperty("标准编码")
        private String code;

        @ApiModelProperty(value = "字段权限状态，0：无权限；1：有权限；2：字段未注册", notes = "0：无权限；1：有权限；2：字段未注册")
        private Integer rightOf;

        public String getField() {
            return field;
        }

        public void setField(String field) {
            this.field = field;
        }

        public Object getValue() {
            return value;
        }

        public void setValue(Object value) {
            this.value = value;
        }

        public String getFieldType() {
            return fieldType;
        }

        public void setFieldType(String fieldType) {
            this.fieldType = fieldType;
        }

        public String getCode() {
            return code;
        }

        public void setCode(String code) {
            this.code = code;
        }

        public Integer getRightOf() {
            return rightOf;
        }

        public void setRightOf(Integer rightOf) {
            this.rightOf = rightOf;
        }
    }

}
