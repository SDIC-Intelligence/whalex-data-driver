package com.meiya.whalex.interior.db.operation.in;

import com.meiya.whalex.interior.db.constant.ForeignKeyActionEnum;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

import javax.validation.constraints.NotBlank;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * 修改表实体
 */
@ApiModel(value = "组件创建表参数")
public class AlterTableParamCondition {

    @ApiModelProperty(value = "表字段新增信息")
    private List<AddTableFieldParam> addTableFieldParamList = new LinkedList<>();


    @ApiModelProperty(value = "修改表字段")
    private List<UpdateTableFieldParam> updateTableFieldParamList = new LinkedList<>();

    @ApiModelProperty(value = "表字段删除信息")
    private List<String> delTableFieldParamList;

    @ApiModelProperty(value = "新增外建")
    private List<ForeignParam>  addForeignParamList;

    @ApiModelProperty(value = "删除外建")
    private List<ForeignParam>  delForeignParamList;

    @ApiModelProperty(value = "新增分区")
    private PartitionInfo addPartition;

    @ApiModelProperty(value = "新增多级分区")
    private MultiLevelPartitionInfo addMultiLevelPartition;

    @ApiModelProperty(value = "删除分区")
    private PartitionInfo delPartition;

    @ApiModelProperty(value = "删除多级分区")
    private MultiLevelPartitionInfo delMultiLevelPartition;

    @ApiModelProperty(value = "新表名称")
    private String newTableName;

    @ApiModelProperty(value = "是否删除主键")
    private boolean delPrimaryKey;

    @ApiModelProperty(value = "表描述")
    private String tableComment;

    @ApiModelProperty(value = "表类型(内部表/外部表)", notes = "true:外部表;false:内部表, 默认为true")
    private boolean external = true;


    public PartitionInfo getAddPartition() {
        return addPartition;
    }

    public void setAddPartition(PartitionInfo addPartition) {
        this.addPartition = addPartition;
    }

    public PartitionInfo getDelPartition() {
        return delPartition;
    }

    public void setDelPartition(PartitionInfo delPartition) {
        this.delPartition = delPartition;
    }

    public boolean isDelPrimaryKey() {
        return delPrimaryKey;
    }

    public void setDelPrimaryKey(boolean delPrimaryKey) {
        this.delPrimaryKey = delPrimaryKey;
    }

    public List<ForeignParam> getAddForeignParamList() {
        return addForeignParamList;
    }

    public void setAddForeignParamList(List<ForeignParam> addForeignParamList) {
        this.addForeignParamList = addForeignParamList;
    }

    public List<ForeignParam> getDelForeignParamList() {
        return delForeignParamList;
    }

    public void setDelForeignParamList(List<ForeignParam> delForeignParamList) {
        this.delForeignParamList = delForeignParamList;
    }

    public boolean isExternal() {
        return external;
    }

    public void setExternal(boolean external) {
        this.external = external;
    }

    public List<String> getDelTableFieldParamList() {
        return delTableFieldParamList;
    }

    public void setDelTableFieldParamList(List<String> delTableFieldParamList) {
        this.delTableFieldParamList = delTableFieldParamList;
    }

    public List<AddTableFieldParam> getAddTableFieldParamList() {
        return addTableFieldParamList;
    }

    public void setAddTableFieldParamList(List<AddTableFieldParam> addTableFieldParamList) {
        this.addTableFieldParamList = addTableFieldParamList;
    }

    public MultiLevelPartitionInfo getAddMultiLevelPartition() {
        return addMultiLevelPartition;
    }

    public void setAddMultiLevelPartition(MultiLevelPartitionInfo addMultiLevelPartition) {
        this.addMultiLevelPartition = addMultiLevelPartition;
    }

    public MultiLevelPartitionInfo getDelMultiLevelPartition() {
        return delMultiLevelPartition;
    }

    public void setDelMultiLevelPartition(MultiLevelPartitionInfo delMultiLevelPartition) {
        this.delMultiLevelPartition = delMultiLevelPartition;
    }

    public AlterTableParamCondition() {
    }

    public void addFiledParamList(String fieldName, String fieldType, String fieldComment, Integer fieldLength, boolean notNull) {
        addTableFieldParamList.add(new AddTableFieldParam(fieldName, fieldComment, fieldType, fieldLength, notNull));
    }

    public List<UpdateTableFieldParam> getUpdateTableFieldParamList() {
        return updateTableFieldParamList;
    }

    public void setUpdateTableFieldParamList(List<UpdateTableFieldParam> updateTableFieldParamList) {
        this.updateTableFieldParamList = updateTableFieldParamList;
    }

    public String getNewTableName() {
        return newTableName;
    }

    public void setNewTableName(String newTableName) {
        this.newTableName = newTableName;
    }

    public String getTableComment() {
        return tableComment;
    }

    public void setTableComment(String tableComment) {
        this.tableComment = tableComment;
    }

    @Data
    @ApiModel(value = "外键信息")
    public static class ForeignParam {
        @ApiModelProperty(value = "外键名称")
        private String foreignName;
        @ApiModelProperty(value = "外键字段")
        private String foreignKey;
        @ApiModelProperty(value = "关联的表名")
        private String referencesTableName;
        @ApiModelProperty(value = "关联的字段")
        private String referencesField;
        @ApiModelProperty(value = "删除时")
        private ForeignKeyActionEnum onDeletion;
        @ApiModelProperty(value = "更新时")
        private ForeignKeyActionEnum onUpdate;
    }

    @ApiModel(value = "表字段信息")
    public static class AddTableFieldParam {
        @ApiModelProperty(value = "字段名")
        @NotBlank(message = "字段名不能为空")
        private String fieldName;
        @ApiModelProperty(value = "字段描述")
        private String fieldComment;
        @ApiModelProperty(value = "字段类型")
        @NotBlank(message = "字段类型不能为空")
        private String fieldType;
        @ApiModelProperty(value = "字段长度")
        private Integer fieldLength;
        @ApiModelProperty(value = "字段小数点")
        private Integer fieldDecimalPoint = 0;
        @ApiModelProperty(value = "是否分布键", allowableValues = "true, false", notes = "需要分布键的组件才需要, 例如 Libra、PostGrep")
        private boolean distributed;
        @ApiModelProperty(value = "是否主键", allowableValues = "true, false", notes = "可设置主键的组件，例如 MySql、LibRa")
        private boolean primaryKey;
        @ApiModelProperty(value = "不能为空", allowableValues = "true, false", notes = "可设置主键的组件，例如 MySql、LibRa")
        private Boolean notNull;
        @ApiModelProperty(value = "es的text字段的分词配置，如果是true默认使用my_analyzer分词器", allowableValues = "true, false", notes = "es的text字段的分词配置，如果是true默认使用my_analyzer分词器")
        private boolean isAnalyzer = false;
        @ApiModelProperty(value = "分词器设置", notes = "设置指定分词器")
        private String tokenizer;
        @ApiModelProperty(value = "将当前字段加入的字段")
        private String copyToField;
        @ApiModelProperty(value = "在设置 isAnalyzer 为 true 的情况下，可以设置扩展 keyword 字段", allowableValues = "true, false")
        private boolean extendKeyword = false;
        @ApiModelProperty(value = "设置 extendKeyword 字段为 true 时生效，大于这个长度的值会被阉割")
        private Integer keywordIgnoreAbove = 256;
        @ApiModelProperty(value = "默认值")
        private String defaultValue;
        @ApiModelProperty(value = "更新值")
        private String onUpdate;
        @ApiModelProperty(value = "字段类型为OBJECT时，为当前字段配置object信息，如es的nested")
        private List<AddTableFieldParam> objectFieldParamList;

        @ApiModelProperty(value = "是否无符号")
        private Boolean unsigned;

        @ApiModelProperty(value = "字段不存在才执行")
        private boolean isNotExists;

        public boolean isNotExists() {
            return isNotExists;
        }

        public void setNotExists(boolean notExists) {
            isNotExists = notExists;
        }

        public String getOnUpdate() {
            return onUpdate;
        }

        public void setOnUpdate(String onUpdate) {
            this.onUpdate = onUpdate;
        }

        public String getDefaultValue() {
            return defaultValue;
        }

        public void setDefaultValue(String defaultValue) {
            this.defaultValue = defaultValue;
        }

        public String getCopyToField() {
            return copyToField;
        }

        public void setCopyToField(String copyToField) {
            this.copyToField = copyToField;
        }

        public boolean isAnalyzer() {
            return isAnalyzer;
        }

        public void setAnalyzer(boolean analyzer) {
            isAnalyzer = analyzer;
        }

        public String getFieldName() {
            return fieldName;
        }

        public void setFieldName(String fieldName) {
            this.fieldName = fieldName;
        }

        public String getFieldType() {
            return fieldType;
        }

        public Integer getFieldDecimalPoint() {
            return fieldDecimalPoint;
        }

        public void setFieldDecimalPoint(Integer fieldDecimalPoint) {
            this.fieldDecimalPoint = fieldDecimalPoint;
        }

        public void setFieldType(String fieldType) {
            this.fieldType = fieldType;
        }

        public Integer getFieldLength() {
            return fieldLength;
        }

        public void setFieldLength(Integer fieldLength) {
            this.fieldLength = fieldLength;
        }

        public String getFieldComment() {
            return fieldComment;
        }

        public void setFieldComment(String fieldComment) {
            this.fieldComment = fieldComment;
        }

        public Boolean getNotNull() {
            return notNull;
        }

        public void setNotNull(Boolean notNull) {
            this.notNull = notNull;
        }

        public boolean isExtendKeyword() {
            return extendKeyword;
        }

        public void setExtendKeyword(boolean extendKeyword) {
            this.extendKeyword = extendKeyword;
        }

        public Integer getKeywordIgnoreAbove() {
            return keywordIgnoreAbove;
        }

        public void setKeywordIgnoreAbove(Integer keywordIgnoreAbove) {
            this.keywordIgnoreAbove = keywordIgnoreAbove;
        }

        public List<AddTableFieldParam> getObjectFieldParamList() {
            return objectFieldParamList;
        }

        public void setObjectFieldParamList(List<AddTableFieldParam> objectFieldParamList) {
            this.objectFieldParamList = objectFieldParamList;
        }

        public boolean isDistributed() {
            return distributed;
        }

        public void setDistributed(boolean distributed) {
            this.distributed = distributed;
        }

        public String getTokenizer() {
            return tokenizer;
        }

        public void setTokenizer(String tokenizer) {
            this.tokenizer = tokenizer;
        }

        public AddTableFieldParam() {
        }

        public AddTableFieldParam(String fieldName, String fieldComment, String fieldType, Integer fieldLength, boolean notNull) {
            this.fieldName = fieldName;
            this.fieldComment = fieldComment;
            this.fieldType = fieldType;
            this.fieldLength = fieldLength;
            this.notNull = notNull;
        }

        public boolean isPrimaryKey() {
            return primaryKey;
        }

        public void setPrimaryKey(boolean primaryKey) {
            this.primaryKey = primaryKey;
        }

        public Boolean getUnsigned() {
            return unsigned;
        }

        public void setUnsigned(Boolean unsigned) {
            this.unsigned = unsigned;
        }
    }

    @ApiModel(value = "修改表字段信息")
    public static class UpdateTableFieldParam extends AddTableFieldParam {

        @ApiModelProperty(value = "新字段名")
        private String newFieldName;

        @ApiModelProperty(value = "drop标识")
        private boolean dropFlag;

        @ApiModelProperty(value = "是否删除默认值")
        private boolean dropDefaultValue;

        @ApiModelProperty(value = "是否删除非空")
        private boolean dropNotNull;

        public boolean isDropNotNull() {
            return dropNotNull;
        }

        public void setDropNotNull(boolean dropNotNull) {
            this.dropNotNull = dropNotNull;
        }

        public boolean isDropDefaultValue() {
            return dropDefaultValue;
        }

        public void setDropDefaultValue(boolean dropDefaultValue) {
            this.dropDefaultValue = dropDefaultValue;
        }

        public boolean isDropFlag() {
            return dropFlag;
        }

        public void setDropFlag(boolean dropFlag) {
            this.dropFlag = dropFlag;
        }

        public String getNewFieldName() {
            return newFieldName;
        }

        public void setNewFieldName(String newFieldName) {
            this.newFieldName = newFieldName;
        }
    }
}
