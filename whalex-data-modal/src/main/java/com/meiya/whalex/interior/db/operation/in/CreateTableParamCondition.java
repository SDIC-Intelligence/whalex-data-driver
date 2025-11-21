package com.meiya.whalex.interior.db.operation.in;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

import javax.validation.constraints.NotBlank;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * 建表实体
 *
 * @author Huanghesen
 * @date 2019/3/5
 */
@ApiModel(value = "组件创建表参数")
public class CreateTableParamCondition {

    @ApiModelProperty(value = "表描述")
    private String tableComment;

    @ApiModelProperty(value = "表字段信息")
    private List<CreateTableFieldParam> createTableFieldParamList = new LinkedList<>();

    @ApiModelProperty(value = "建表开始时间", notes = "周期性表可设置创建周期")
    private String startTime;
    @ApiModelProperty(value = "建表结束时间", notes = "周期性表可设置创建周期")
    private String endTime;
    @ApiModelProperty(value = "表类型(内部表/外部表)", notes = "true:外部表;false:内部表, 默认为true")
    private boolean external = true;
    @ApiModelProperty(value = "是否进行修复表操作", notes = "一些组件分区表创建表之后，若入库新的数据，需要执行修复表操作", allowableValues = "true,false")
    private boolean repair = false;

    @ApiModelProperty(value = "表模型信息", notes = "表模型信息")
    private TableModelParam tableModelParam;

    @ApiModelProperty(value = "分布键信息", notes = "分布键信息")
    private DistributedParam distributedParam;

    @ApiModelProperty(value = "es分词器定义", notes = "es分词器定义")
    private Map<String, Object> analyzerDefine;

    @ApiModelProperty(value = "es分词器包括（analyzer, tokenizer）", notes = "es分词器包括（analyzer, tokenizer）")
    private Map<String, Object> analysis;

    /**
     * titan图新增顶点参数
     */
    @ApiModelProperty(value = "titan图新增顶点参数", notes = "titan图新增顶点参数,")
    private List<Map<String, Object>> titanVertexJsonList;

    /**
     * titan图新增边参数
     */
    @ApiModelProperty(value = "titan图新增边参数", notes = "titan图新增边参数")
    private List<Map<String, Object>> titanEdgeJsonList;

    @Deprecated
    @ApiModelProperty(value = "kafka分区数", notes = "kafka分区数")
    private Integer numPartitions;
    @Deprecated
    @ApiModelProperty(value = "kafka副本数", notes = "kafka副本数")
    private Integer replicationFactor;
    @Deprecated
    @ApiModelProperty(value = "kafka数据保存时间", notes = "kafka数据保存时间")
    private Integer retentionTime;

    @ApiModelProperty(value = "是否添加默认配置，默认是true", notes = "是否添加默认配置，默认是true")
    private boolean defaultConf = true;

    private boolean isNotExists;

    @ApiModelProperty(value = "rowKey分区信息， HBase专用", notes = "rowKey分区信息， HBase专用")
    private List<String> rowKeyPartition;

    @ApiModelProperty(value = "复制表配置，仅支持关系型数据库")
    private CreateTableLikeParam createTableLike;

    public TableModelParam getTableModelParam() {
        return tableModelParam;
    }

    public void setTableModelParam(TableModelParam tableModelParam) {
        this.tableModelParam = tableModelParam;
    }

    public DistributedParam getDistributedParam() {
        return distributedParam;
    }

    public void setDistributedParam(DistributedParam distributedParam) {
        this.distributedParam = distributedParam;
    }

    public boolean isNotExists() {
        return isNotExists;
    }

    public void setNotExists(boolean notExists) {
        isNotExists = notExists;
    }

    public List<String> getRowKeyPartition() {
        return rowKeyPartition;
    }

    public void setRowKeyPartition(List<String> rowKeyPartition) {
        this.rowKeyPartition = rowKeyPartition;
    }

    public Map<String, Object> getAnalysis() {
        return analysis;
    }

    public void setAnalysis(Map<String, Object> analysis) {
        this.analysis = analysis;
    }

    public Integer getNumPartitions() {
        return numPartitions;
    }

    public void setNumPartitions(Integer numPartitions) {
        this.numPartitions = numPartitions;
    }

    public Integer getReplicationFactor() {
        return replicationFactor;
    }

    public void setReplicationFactor(Integer replicationFactor) {
        this.replicationFactor = replicationFactor;
    }

    public Integer getRetentionTime() {
        return retentionTime;
    }

    public void setRetentionTime(Integer retentionTime) {
        this.retentionTime = retentionTime;
    }

    public List<Map<String, Object>> getTitanVertexJsonList() {
        return titanVertexJsonList;
    }

    public void setTitanVertexJsonList(List<Map<String, Object>> titanVertexJsonList) {
        this.titanVertexJsonList = titanVertexJsonList;
    }

    public List<Map<String, Object>> getTitanEdgeJsonList() {
        return titanEdgeJsonList;
    }

    public void setTitanEdgeJsonList(List<Map<String, Object>> titanEdgeJsonList) {
        this.titanEdgeJsonList = titanEdgeJsonList;
    }

    public String getTableComment() {
        return tableComment;
    }

    public void setTableComment(String tableComment) {
        this.tableComment = tableComment;
    }

    public List<CreateTableFieldParam> getCreateTableFieldParamList() {
        return createTableFieldParamList;
    }

    public void setCreateTableFieldParamList(List<CreateTableFieldParam> createTableFieldParamList) {
        this.createTableFieldParamList = createTableFieldParamList;
    }

    public void addFiledParamList(String fieldName, String fieldType, String fieldComment, Integer fieldLength, boolean isDistributed) {
        createTableFieldParamList.add(new CreateTableFieldParam(fieldName, fieldComment, fieldType, fieldLength, isDistributed));
    }

    public CreateTableLikeParam getCreateTableLike() {
        return createTableLike;
    }

    public void setCreateTableLike(CreateTableLikeParam createTableLike) {
        this.createTableLike = createTableLike;
    }

    public CreateTableParamCondition() {
    }

    public CreateTableParamCondition(String tableComment, List<CreateTableFieldParam> createTableFieldParamList) {
        this.tableComment = tableComment;
        this.createTableFieldParamList = createTableFieldParamList;
    }

    public CreateTableParamCondition(String tableComment, List<CreateTableFieldParam> createTableFieldParamList, boolean external) {
        this.tableComment = tableComment;
        this.createTableFieldParamList = createTableFieldParamList;
        this.external = external;
    }

    public String getStartTime() {
        return startTime;
    }

    public void setStartTime(String startTime) {
        this.startTime = startTime;
    }

    public String getEndTime() {
        return endTime;
    }

    public void setEndTime(String endTime) {
        this.endTime = endTime;
    }

    public boolean isExternal() {
        return external;
    }

    public CreateTableParamCondition setExternal(boolean external) {
        this.external = external;
        return this;
    }

    public boolean isRepair() {
        return repair;
    }

    public void setRepair(boolean repair) {
        this.repair = repair;
    }

    public Map<String, Object> getAnalyzerDefine() {
        return analyzerDefine;
    }

    public void setAnalyzerDefine(Map<String, Object> analyzerDefine) {
        this.analyzerDefine = analyzerDefine;
    }

    public boolean isDefaultConf() {
        return defaultConf;
    }

    public void setDefaultConf(boolean defaultConf) {
        this.defaultConf = defaultConf;
    }

    public static enum DistributedType {
        HASH
    }

    public static enum TableModel {
        DUPLICATE_KEY, PRIMARY_KEY, AGGREGATE_KEY, UNIQUE_KEY
    }

    @Data
    @ApiModel(value = "表模型信息")
    public static class TableModelParam {
        private TableModel tableModel = TableModel.PRIMARY_KEY;
        private List<String> fieldList;
        private List<AggTableModelField> aggFieldList;
    }

    @Data
    @ApiModel(value = "聚合表模型字段信息")
    public static class AggTableModelField {
        private String fieldName;
        private String funcName;
    }


    @Data
    @ApiModel(value = "分布键信息")
    public static class DistributedParam {
        private DistributedType distributedType = DistributedType.HASH;
        private List<String> fieldList;
    }



    @ApiModel(value = "表字段信息")
    public static class CreateTableFieldParam {
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
        private Integer fieldDecimalPoint;
        @ApiModelProperty(value = "是否分布键", allowableValues = "true, false", notes = "需要分布键的组件才需要, 例如 Libra、PostGrep")
        private boolean distributed;
        @ApiModelProperty(value = "是否主键", allowableValues = "true, false", notes = "可设置主键的组件，例如 MySql、LibRa")
        private boolean primaryKey;
        @ApiModelProperty(value = "不能为空", allowableValues = "true, false", notes = "可设置主键的组件，例如 MySql、LibRa")
        private boolean notNull;
        @ApiModelProperty(value = "es的text字段的分词配置", allowableValues = "true, false", notes = "es的text字段的分词配置")
        private boolean isAnalyzer = false;
        @ApiModelProperty(value = "分词器设置", notes = "设置指定分词器")
        private String tokenizer;
        @ApiModelProperty(value = "将当前字段加入的字段")
        private String copyToField;
        @ApiModelProperty(value = "在设置 isAnalyzer 为 true 的情况下，可以设置扩展 keyword 字段", allowableValues = "true, false")
        private boolean extendKeyword = false;
        @ApiModelProperty(value = "设置 extendKeyword 字段为 true 时生效，大于这个长度的值会被阉割")
        private Integer keywordIgnoreAbove = 256;
        @ApiModelProperty(value = "字段类型为OBJECT时，为当前字段配置object信息，如es的nested")
        private List<CreateTableFieldParam> objectFieldParamList;
        @ApiModelProperty(value = "是否分区", allowableValues = "true, false", notes = "可设置主键的组件，例如 adb ")
        private boolean partition;
        @ApiModelProperty(value = "分区信息")
        private PartitionInfo partitionInfo;
        @ApiModelProperty(value = "分区字段格式化")
        @Deprecated
        private String partitionFormat;
        @ApiModelProperty(value = "默认值")
        private String defaultValue;
        @ApiModelProperty(value = "是否自增")
        private AutoIncrementInfo autoIncrement;

        @ApiModelProperty(value = "是否无符号")
        private Boolean unsigned;

        @ApiModelProperty(value = "列簇压缩方式 LZO, GZ, NONE, SNAPPY, LZ4, BZIP2, ZSTD")
        private String compressionType;

        private String onUpdate;

        public String getOnUpdate() {
            return onUpdate;
        }

        public void setOnUpdate(String onUpdate) {
            this.onUpdate = onUpdate;
        }

        public PartitionInfo getPartitionInfo() {
            return partitionInfo;
        }

        public void setPartitionInfo(PartitionInfo partitionInfo) {
            this.partitionInfo = partitionInfo;
        }

        public List<CreateTableFieldParam> getObjectFieldParamList() {
            return objectFieldParamList;
        }

        public void setObjectFieldParamList(List<CreateTableFieldParam> objectFieldParamList) {
            this.objectFieldParamList = objectFieldParamList;
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

        public boolean isNotNull() {
            return notNull;
        }

        public void setNotNull(boolean notNull) {
            this.notNull = notNull;
        }

        public String getDefaultValue() {
            return defaultValue;
        }

        public void setDefaultValue(String defaultValue) {
            this.defaultValue = defaultValue;
        }

        public String getCompressionType() {
            return compressionType;
        }

        public void setCompressionType(String compressionType) {
            this.compressionType = compressionType;
        }

        public boolean isAutoIncrement() {
            if (autoIncrement != null) {
                return autoIncrement.isAutoIncrement();
            } else {
                return false;
            }
        }

        public void setAutoIncrement(boolean autoIncrement) {
            if (this.autoIncrement == null) {
                this.autoIncrement = new AutoIncrementInfo(autoIncrement);
            } else {
                this.autoIncrement.setAutoIncrement(autoIncrement);
            }
        }

        public Long getStartIncrement() {
            if (autoIncrement != null) {
                return autoIncrement.getStartIncrement();
            } else {
                return null;
            }
        }

        public void setStartIncrement(Long startIncrement) {
            if (this.autoIncrement == null) {
                this.autoIncrement = new AutoIncrementInfo(false, startIncrement, 1L);
            } else {
                this.autoIncrement.setStartIncrement(startIncrement);
            }
        }

        public Long getIncrementBy() {
            if (autoIncrement != null) {
                return autoIncrement.getIncrementBy();
            } else {
                return null;
            }
        }

        public void setIncrementBy(Long incrementBy) {
            if (this.autoIncrement == null) {
                this.autoIncrement = new AutoIncrementInfo(false, 1L, incrementBy);
            } else {
                this.autoIncrement.setIncrementBy(incrementBy);
            }
        }

        public CreateTableFieldParam() {
        }

        public CreateTableFieldParam(String fieldName, String fieldComment, String fieldType, Integer fieldLength, boolean isDistributed) {
            this.fieldName = fieldName;
            this.fieldComment = fieldComment;
            this.fieldType = fieldType;
            this.fieldLength = fieldLength;
            this.distributed = isDistributed;
        }

        public boolean isDistributed() {
            return distributed;
        }

        public void setDistributed(boolean distributed) {
            this.distributed = distributed;
        }

        public boolean isPrimaryKey() {
            return primaryKey;
        }

        public void setPrimaryKey(boolean primaryKey) {
            this.primaryKey = primaryKey;
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

        public String getTokenizer() {
            return tokenizer;
        }

        public void setTokenizer(String tokenizer) {
            this.tokenizer = tokenizer;
        }

        public boolean isPartition() {
            return partition;
        }

        public void setPartition(boolean partition) {
            this.partition = partition;
        }

        public String getPartitionFormat() {
            return partitionFormat;
        }

        public void setPartitionFormat(String partitionFormat) {
            this.partitionFormat = partitionFormat;
        }

        public Boolean getUnsigned() {
            return unsigned;
        }

        public void setUnsigned(Boolean unsigned) {
            this.unsigned = unsigned;
        }


    }
}
