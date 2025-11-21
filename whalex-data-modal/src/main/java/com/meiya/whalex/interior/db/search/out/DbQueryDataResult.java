package com.meiya.whalex.interior.db.search.out;

import cn.hutool.core.collection.CollectionUtil;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.meiya.whalex.interior.base.BaseResult;
import com.meiya.whalex.interior.db.constant.FieldType;
import com.meiya.whalex.interior.db.constant.ReturnCodeEnum;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.ToString;

import java.util.ArrayList;
import java.util.List;

/**
 * API 接口数据查询统一返回实体
 *
 * @author Huanghesen
 * @date 2018/9/10
 * @project whale-cloud-api
 */
@ApiModel(value = "数据查询接口返参实体")
@ToString
public class DbQueryDataResult extends BaseResult {

    @ApiModelProperty(value = "游标ID", notes = "游标接口查询时候会携带")
    private String cursorId;

    @ApiModelProperty(value = "数据结果集")
    private List<DataEntity> result = new ArrayList<>();

    @ApiModelProperty(value = "事务id")
    private String transactionId;

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public List<DataEntity> getResult() {
        return result;
    }

    public void setResult(List<DataEntity> result) {
        this.result = result;
    }

    public String getCursorId() {
        return cursorId;
    }

    public void setCursorId(String cursorId) {
        this.cursorId = cursorId;
    }

    public void addResult(DataEntity dataEntity) {
        if (this.result == null) {
            this.result = new ArrayList<>();
        }
        this.result.add(dataEntity);
    }

    public void addResultList(List<DataEntity>... dataEntityList) {
        if (this.result == null) {
            this.result = new ArrayList<>();
        }
        if (dataEntityList != null) {
            for (List<DataEntity> dataEntities : dataEntityList) {
                if (CollectionUtil.isNotEmpty(dataEntities)) {
                    this.result.addAll(dataEntities);
                }
            }
        }
    }

    public DbQueryDataResult() {
        super();
    }

    public DbQueryDataResult(int returnCode, String errorMsg) {
        super(returnCode, errorMsg);
    }

    public DbQueryDataResult(ReturnCodeEnum returnCodeEnum) {
        super(returnCodeEnum);
    }

    /**
     * 包装来源与结果集合
     */
    @ApiModel(value = "数据结果返参实体")
    public static class DataEntity {
        @ApiModelProperty(value = "资源标识符")
        private String resourceId;
        @ApiModelProperty(value = "数据库表标识符")
        private String dbId;
        @ApiModelProperty(value = "数据库类型")
        private String dbType;
        @ApiModelProperty(value = "数据库类型名称")
        private String dbTypeName;
        @ApiModelProperty(value = "数据总量")
        private long total;
        @ApiModelProperty(value = "返回数据行数")
        private long rows;
        @JsonProperty
        @ApiModelProperty(value = "数据结果")
        private List<List<FieldEntity>> data = new ArrayList<>();
        @ApiModelProperty(value = "查询状态")
        private Boolean success = true;
        @ApiModelProperty(value = "查询状态码")
        private int code = 0;
        @ApiModelProperty(value = "查询状态信息")
        private String message = "成功";

        public String getResourceId() {
            return resourceId;
        }

        public void setResourceId(String resourceId) {
            this.resourceId = resourceId;
        }

        public String getDbType() {
            return dbType;
        }

        public void setDbType(String dbType) {
            this.dbType = dbType;
        }

        public long getTotal() {
            return total;
        }

        public void setTotal(long total) {
            this.total = total;
        }

        public long getRows() {
            return rows;
        }

        public void setRows(long rows) {
            this.rows = rows;
        }

        public List<List<FieldEntity>> getData() {
            return data;
        }

        public String getDbId() {
            return dbId;
        }

        public void setDbId(String dbId) {
            this.dbId = dbId;
        }

        public void setData(List<List<FieldEntity>> data) {
            this.data = data;
        }

        public void addData(List<List<FieldEntity>> data) {
            this.data.addAll(data);
        }

        public Boolean getSuccess() {
            return success;
        }

        public void setSuccess(Boolean success) {
            this.success = success;
        }

        public int getCode() {
            return code;
        }

        public void setCode(int code) {
            this.code = code;
        }

        public String getMessage() {
            return message;
        }

        public void setMessage(String message) {
            this.message = message;
        }

        public String getDbTypeName() {
            return dbTypeName;
        }

        public void setDbTypeName(String dbTypeName) {
            this.dbTypeName = dbTypeName;
        }
    }

    /**
     * 参数信息实体
     */
    @ApiModel(value = "单字段数据返参实体")
    public static class FieldEntity {
        @ApiModelProperty(value = "字段名")
        private String field;
        @ApiModelProperty(value = "字段值")
        private Object value;
        @ApiModelProperty(value = "翻译字段值")
        private String dicValue;
        @ApiModelProperty(value = "字段原始类型")
        private String fieldType = FieldType.STRING.getName();
        @ApiModelProperty(value = "字段版本号")
        private Long version;

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

        public String getDicValue() {
            return dicValue;
        }

        public void setDicValue(String dicValue) {
            this.dicValue = dicValue;
        }

        public Long getVersion() {
            return version;
        }

        public void setVersion(Long version) {
            this.version = version;
        }
    }
}
