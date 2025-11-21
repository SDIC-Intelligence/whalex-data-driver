package com.meiya.whalex.interior.db.operation.out;

import com.meiya.whalex.interior.base.BaseResult;
import com.meiya.whalex.interior.db.constant.ReturnCodeEnum;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import java.util.LinkedList;
import java.util.List;

/**
 * 校验资源索引，与数据库索引匹配情况响应报文
 *
 * @author 黄河森
 * @date 2019/11/28
 * @project whale-cloud-platformX
 */
@ApiModel(value = "索引校验查询响应报文")
public class CheckIndexResult extends BaseResult {
    @ApiModelProperty(value = "总结果数")
    private Integer total;
    @ApiModelProperty(value = "返回结果数")
    private Integer rows;
    @ApiModelProperty(value = "索引校验结果集合")
    private List<Data> data;

    public CheckIndexResult() {
    }

    public CheckIndexResult(Integer total, Integer rows) {
        this.total = total;
        this.rows = rows;
    }

    public CheckIndexResult(Integer total, Integer rows, List<Data> data) {
        this.total = total;
        this.rows = rows;
        this.data = data;
    }

    public CheckIndexResult(int returnCode, String errorMsg) {
        super(returnCode, errorMsg);
    }

    public CheckIndexResult(ReturnCodeEnum returnCodeEnum) {
        super(returnCodeEnum);
    }

    public Integer getTotal() {
        return total;
    }

    public void setTotal(Integer total) {
        this.total = total;
    }

    public Integer getRows() {
        if (this.rows == null && this.data != null) {
            rows = data.size();
        }
        return rows;
    }

    public void setRows(Integer rows) {
        this.rows = rows;
    }

    public List<Data> getData() {
        return data;
    }

    public void setData(List<Data> data) {
        this.data = data;
    }

    @ApiModel(value = "索引校验查询单数据库校验结果")
    public static class Data extends BaseResult {
        @ApiModelProperty("资源编码")
        private String resourceId;

        @ApiModelProperty("需要建立索引的字段")
        private List<String> needIndexColumn;

        @ApiModelProperty("数据库已建立索引的字段")
        private List<String> hasIndexColumn;

        @ApiModelProperty("缺失的索引")
        private List<String> lackIndexColumn;

        @ApiModelProperty("数据库表编码")
        private String dbId;

        @ApiModelProperty("存储来源编号")
        private String dbType;

        public Data() {
        }

        public Data(int returnCode, String errorMsg) {
            super(returnCode, errorMsg);
        }

        public Data(ReturnCodeEnum returnCodeEnum) {
            super(returnCodeEnum);
        }

        public Data(String resourceId, String dbId, String dbType) {
            this.resourceId = resourceId;
            this.needIndexColumn = new LinkedList<>();
            this.hasIndexColumn = new LinkedList<>();
            this.lackIndexColumn = new LinkedList<>();
            this.dbId = dbId;
            this.dbType = dbType;
        }

        public String getResourceId() {
            return resourceId;
        }

        public void setResourceId(String resourceId) {
            this.resourceId = resourceId;
        }

        public List<String> getNeedIndexColumn() {
            return needIndexColumn;
        }

        public void setNeedIndexColumn(List<String> needIndexColumn) {
            this.needIndexColumn = needIndexColumn;
        }

        public List<String> getHasIndexColumn() {
            return hasIndexColumn;
        }

        public void setHasIndexColumn(List<String> hasIndexColumn) {
            this.hasIndexColumn = hasIndexColumn;
        }

        public List<String> getLackIndexColumn() {
            return lackIndexColumn;
        }

        public void setLackIndexColumn(List<String> lackIndexColumn) {
            this.lackIndexColumn = lackIndexColumn;
        }

        public String getDbId() {
            return dbId;
        }

        public void setDbId(String dbId) {
            this.dbId = dbId;
        }

        public String getDbType() {
            return dbType;
        }

        public void setDbType(String dbType) {
            this.dbType = dbType;
        }

    }
}
