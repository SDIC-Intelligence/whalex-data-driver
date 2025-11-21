package com.meiya.whalex.db.builder;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.DateUtil;
import com.meiya.whalex.interior.db.constant.ItemFieldTypeEnum;
import com.meiya.whalex.interior.db.operation.in.CreateTableLikeParam;
import com.meiya.whalex.interior.db.operation.in.CreateTableParamCondition;
import com.meiya.whalex.interior.db.operation.in.PartitionInfo;

import java.util.*;

/**
 * @author 黄河森
 * @date 2021/8/3
 * @project whalex-data-driver-back
 */
public class CreateTableBuilder {

    private CreateTableParamCondition createTableParamCondition;

    private CreateTableBuilder() {
        this.createTableParamCondition = new CreateTableParamCondition();
    }

    public static CreateTableBuilder builder() {
        return new CreateTableBuilder();
    }

    public CreateTableParamCondition build() {
        return this.createTableParamCondition;
    }

    public CreateTableBuilder copyTable(String copyTableName) {
        CreateTableLikeParam createTableLike = new CreateTableLikeParam();
        createTableLike.setCopyTableName(copyTableName);
        this.createTableParamCondition.setCreateTableLike(createTableLike);
        return this;
    }

    public CreateTableBuilder isNotExists(boolean isNotExists) {
        this.createTableParamCondition.setNotExists(isNotExists);
        return this;
    }

    public CreateTableBuilder rowKeyPartition(List<String> rowKeyPartition) {
        this.createTableParamCondition.setRowKeyPartition(rowKeyPartition);
        return this;
    }

    public CreateTableBuilder tableComment(String tableComment) {
        this.createTableParamCondition.setTableComment(tableComment);
        return this;
    }

    public CreateTableBuilder defaultConf(boolean defaultConf) {
        this.createTableParamCondition.setDefaultConf(defaultConf);
        return this;
    }

    public CreateTableBuilder startTime(Date startTime) {
        this.createTableParamCondition.setStartTime(DateUtil.format(startTime, DatePattern.NORM_DATE_PATTERN));
        return this;
    }

    public CreateTableBuilder endTime(Date endTime) {
        this.createTableParamCondition.setEndTime(DateUtil.format(endTime, DatePattern.NORM_DATE_PATTERN));
        return this;
    }

    public SchemaBuilder schema() {
        List<CreateTableParamCondition.CreateTableFieldParam> schema = new ArrayList<>();
        createTableParamCondition.setCreateTableFieldParamList(schema);
        return SchemaBuilder.builder(this, schema);
    }

    public DistributedBuilder distributed() {
        CreateTableParamCondition.DistributedParam distributedParam = new CreateTableParamCondition.DistributedParam();
        createTableParamCondition.setDistributedParam(distributedParam);
        return DistributedBuilder.builder(this, distributedParam);
    }

    public TableModelBuilder tableModel() {
        CreateTableParamCondition.TableModelParam tableModelParam = new CreateTableParamCondition.TableModelParam();
        createTableParamCondition.setTableModelParam(tableModelParam);
        return TableModelBuilder.builder(this, tableModelParam);
    }

    public AnalysisBuilder analysis() {
        Map<String, Object> analysisMap = new HashMap<>();
        this.createTableParamCondition.setAnalysis(analysisMap);
        return new AnalysisBuilder(analysisMap, this);
    }

    public static class TableModelBuilder {
        private CreateTableParamCondition.TableModelParam tableModelParam;

        private CreateTableBuilder createTableBuilder;

        private TableModelBuilder(CreateTableBuilder createTableBuilder, CreateTableParamCondition.TableModelParam tableModelParam) {
            this.createTableBuilder = createTableBuilder;
            this.tableModelParam = tableModelParam;
        }

        public static TableModelBuilder builder(CreateTableBuilder createTableBuilder, CreateTableParamCondition.TableModelParam tableModelParam) {
            return new TableModelBuilder(createTableBuilder, tableModelParam);
        }

        public CreateTableBuilder returned() {
            return createTableBuilder;
        }

        public TableModelBuilder TableModelType(CreateTableParamCondition.TableModel tableModel) {
            tableModelParam.setTableModel(tableModel);
            return this;
        }

        public TableModelBuilder keys(String ...keys) {
            tableModelParam.setFieldList(Arrays.asList(keys));
            return this;
        }

        public TableModelBuilder addAggField(String fieldName, String funcName) {
            List<CreateTableParamCondition.AggTableModelField> aggFieldList = tableModelParam.getAggFieldList();
            if(aggFieldList == null) {
                aggFieldList = new ArrayList<>();
                tableModelParam.setAggFieldList(aggFieldList);
            }
            CreateTableParamCondition.AggTableModelField aggTableModelField = new CreateTableParamCondition.AggTableModelField();
            aggTableModelField.setFieldName(fieldName);
            aggTableModelField.setFuncName(funcName);
            aggFieldList.add(aggTableModelField);
            return this;
        }
    }

    public static class DistributedBuilder {
        private CreateTableParamCondition.DistributedParam distributedParam;

        private CreateTableBuilder createTableBuilder;

        private DistributedBuilder(CreateTableBuilder createTableBuilder, CreateTableParamCondition.DistributedParam distributedParam) {
            this.createTableBuilder = createTableBuilder;
            this.distributedParam = distributedParam;
        }

        public static DistributedBuilder builder(CreateTableBuilder createTableBuilder, CreateTableParamCondition.DistributedParam distributedParam) {
            return new DistributedBuilder(createTableBuilder, distributedParam);
        }

        public CreateTableBuilder returned() {
            return createTableBuilder;
        }

        public DistributedBuilder distributedType(CreateTableParamCondition.DistributedType distributedType) {
            distributedParam.setDistributedType(distributedType);
            return this;
        }

        public DistributedBuilder distributedKey(String ...distributedKey) {
            distributedParam.setFieldList(Arrays.asList(distributedKey));
            return this;
        }
    }

    public static class SchemaBuilder {
        private List<CreateTableParamCondition.CreateTableFieldParam> schema;

        private CreateTableBuilder createTableBuilder;

        private SchemaBuilder(CreateTableBuilder createTableBuilder, List<CreateTableParamCondition.CreateTableFieldParam> schema) {
            this.createTableBuilder = createTableBuilder;
            this.schema = schema;
        }

        public static SchemaBuilder builder(CreateTableBuilder createTableBuilder, List<CreateTableParamCondition.CreateTableFieldParam> schema) {
            return new SchemaBuilder(createTableBuilder, schema);
        }

        public CreateTableBuilder returned() {
            return createTableBuilder;
        }

        public ColumnBuilder column() {
            CreateTableParamCondition.CreateTableFieldParam createTableFieldParam = new CreateTableParamCondition.CreateTableFieldParam();
            schema.add(createTableFieldParam);
            return ColumnBuilder.builder(this, createTableFieldParam);
        }
    }

    /**
     * 字段构造器
     */
    public static class ColumnBuilder {
        private CreateTableParamCondition.CreateTableFieldParam column;

        private SchemaBuilder schemaBuilder;

        private ColumnBuilder(SchemaBuilder schemaBuilder, CreateTableParamCondition.CreateTableFieldParam column) {
            this.schemaBuilder = schemaBuilder;
            this.column = column;
        }

        public static ColumnBuilder builder(SchemaBuilder schemaBuilder, CreateTableParamCondition.CreateTableFieldParam column) {
            return new ColumnBuilder(schemaBuilder, column);
        }

        public SchemaBuilder returned() {
            return schemaBuilder;
        }

        public ColumnBuilder fieldName(String fieldName) {
            this.column.setFieldName(fieldName);
            return this;
        }

        public ColumnBuilder fieldComment(String fieldComment) {
            this.column.setFieldComment(fieldComment);
            return this;
        }

        public ColumnBuilder objectFieldParamList(List<CreateTableParamCondition.CreateTableFieldParam> objectFieldParamList) {
            this.column.setObjectFieldParamList(objectFieldParamList);
            return this;
        }


        public ColumnBuilder fieldType(ItemFieldTypeEnum itemFieldType) {
            this.column.setFieldType(itemFieldType.getVal());
            return this;
        }

        public ColumnBuilder fieldLength(Integer fieldLength) {
            this.column.setFieldLength(fieldLength);
            return this;
        }

        public ColumnBuilder fieldDecimalPoint(Integer fieldDecimalPoint) {
            this.column.setFieldDecimalPoint(fieldDecimalPoint);
            return this;
        }

        public ColumnBuilder distributed(boolean distributed) {
            this.column.setDistributed(distributed);
            return this;
        }

        public ColumnBuilder primaryKey(boolean primaryKey) {
            this.column.setPrimaryKey(primaryKey);
            return this;
        }

        public ColumnBuilder partition(boolean partition) {
            this.column.setPartition(partition);
            return this;
        }

        public ColumnBuilder partitionInfo(PartitionInfo partitionInfo) {
            this.column.setPartitionInfo(partitionInfo);
            return this;
        }

        public ColumnBuilder notNull(boolean notNull) {
            this.column.setNotNull(notNull);
            return this;
        }

        public ColumnBuilder defaultValue(String defaultValue) {
            this.column.setDefaultValue(defaultValue);
            return this;
        }

        public ColumnBuilder onUpdate(String onUpdate) {
            this.column.setOnUpdate(onUpdate);
            return this;
        }

        public ColumnBuilder autoIncrement(boolean autoIncrement) {
            this.column.setAutoIncrement(autoIncrement);
            return this;
        }

        public ColumnBuilder startIncrement(long startIncrement) {
            this.column.setStartIncrement(startIncrement);
            return this;
        }

        public ColumnBuilder incrementBy(long incrementBy) {
            if (incrementBy <= 0) {
                throw new IllegalArgumentException("自增值必须大于0");
            }
            this.column.setIncrementBy(incrementBy);
            return this;
        }

        public ColumnBuilder isAnalyzer(boolean isAnalyzer) {
            this.column.setAnalyzer(isAnalyzer);
            return this;
        }

        public ColumnBuilder copyToField(String copyToField) {
            this.column.setCopyToField(copyToField);
            return this;
        }

        public ColumnBuilder extendKeyword(boolean extendKeyword) {
            this.column.setExtendKeyword(extendKeyword);
            return this;
        }

        public ColumnBuilder keywordIgnoreAbove(Integer keywordIgnoreAbove) {
            if (keywordIgnoreAbove != null) {
                this.column.setKeywordIgnoreAbove(keywordIgnoreAbove);
            }
            return this;
        }

        public ColumnBuilder tokenizer(String tokenizer) {
            this.column.setTokenizer(tokenizer);
            return this;
        }

        public ColumnBuilder unsigned(boolean unsigned) {
            this.column.setUnsigned(unsigned);
            return this;
        }

        public ColumnBuilder compressionType(String compressionType) {
            this.column.setCompressionType(compressionType);
            return this;
        }
    }

    /**
     * 分词器定义构造器
     */
    public static class AnalysisBuilder {

        private Map<String, Object> analysisMap;

        private CreateTableBuilder createTableBuilder;

        private AnalysisBuilder(Map<String, Object> analysisMap, CreateTableBuilder createTableBuilder) {
            this.analysisMap = analysisMap;
            this.createTableBuilder = createTableBuilder;
            this.analysisMap.put("analyzer", new HashMap<>());
            this.analysisMap.put("char_filter", new HashMap<>());
            this.analysisMap.put("tokenizer", new HashMap<>());
        }

        public AnalyzerBuilder analyzer(String analyzer) {
            Map<String, Object> analyzerConfMap = new HashMap<>();
            Map<String, Object> analyzerMap = (Map<String, Object>) this.analysisMap.get("analyzer");
            analyzerMap.put(analyzer, analyzerConfMap);
            return new AnalyzerBuilder(analyzerConfMap, this);
        }

        public CharFilterBuilder charFilter(String charFilter) {
            Map<String, Object> charFilterConfMap = new HashMap<>();
            Map<String, Object> charFilterMap = (Map<String, Object>) this.analysisMap.get("char_filter");
            charFilterMap.put(charFilter, charFilterConfMap);
            return new CharFilterBuilder(charFilterConfMap, this);
        }

        public TokenizerBuilder tokenizer(String tokenizer) {
            Map<String, Object> tokenizerConfMap = new HashMap<>();
            Map<String, Object> tokenizerMap = (Map<String, Object>) this.analysisMap.get("tokenizer");
            tokenizerMap.put(tokenizer, tokenizerConfMap);
            return new TokenizerBuilder(tokenizerConfMap, this);
        }

        public CreateTableBuilder returned() {
            return createTableBuilder;
        }
    }

    public static class AnalyzerBuilder {
        private Map<String, Object> analyzerMap;

        private AnalysisBuilder builder;

        private AnalyzerBuilder(Map<String, Object> analyzerMap, AnalysisBuilder builder) {
            this.analyzerMap = analyzerMap;
            this.builder = builder;
        }

        public AnalyzerBuilder filter(String... filter) {
            this.analyzerMap.put("filter", CollectionUtil.newArrayList(filter));
            return this;
        }

        public AnalyzerBuilder tokenizer(String tokenizer) {
            this.analyzerMap.put("tokenizer", tokenizer);
            return this;
        }

        public AnalyzerBuilder charFilter(String... charFilter) {
            this.analyzerMap.put("char_filter", CollectionUtil.newArrayList(charFilter));
            return this;
        }

        public AnalysisBuilder returned() {
            return builder;
        }
    }

    public static class CharFilterBuilder {
        private Map<String, Object> charFilterMap;

        private AnalysisBuilder analysisBuilder;

        private CharFilterBuilder(Map<String, Object> charFilterMap, AnalysisBuilder analysisBuilder) {
            this.charFilterMap = charFilterMap;
            this.analysisBuilder = analysisBuilder;
        }

        public CharFilterBuilder pattern(String pattern) {
            this.charFilterMap.put("pattern", pattern);
            return this;
        }

        public CharFilterBuilder type(String type) {
            this.charFilterMap.put("type", type);
            return this;
        }

        public CharFilterBuilder replacement(String replacement) {
            this.charFilterMap.put("replacement", replacement);
            return this;
        }

        public AnalysisBuilder returned() {
            return analysisBuilder;
        }
    }

    public static class TokenizerBuilder {
        private Map<String, Object> tokenizerMap;

        private AnalysisBuilder analysisBuilder;

        public TokenizerBuilder(Map<String, Object> tokenizerMap, AnalysisBuilder analysisBuilder) {
            this.tokenizerMap = tokenizerMap;
            this.analysisBuilder = analysisBuilder;
        }

        public TokenizerBuilder pattern(String pattern) {
            this.tokenizerMap.put("pattern", pattern);
            return this;
        }

        public TokenizerBuilder type(String type) {
            this.tokenizerMap.put("type", type);
            return this;
        }

        public AnalysisBuilder returned() {
            return analysisBuilder;
        }
    }

}
