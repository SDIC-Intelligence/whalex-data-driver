package com.meiya.whalex.db.builder;

import com.meiya.whalex.interior.db.constant.ForeignKeyActionEnum;
import com.meiya.whalex.interior.db.constant.ItemFieldTypeEnum;
import com.meiya.whalex.interior.db.operation.in.AlterTableParamCondition;
import com.meiya.whalex.interior.db.operation.in.MultiLevelPartitionInfo;
import com.meiya.whalex.interior.db.operation.in.PartitionInfo;

import java.util.ArrayList;
import java.util.List;

/**
 * @author 黄河森
 * @date 2021/11/15
 * @package com.meiya.whalex.db.builder
 * @project whalex-data-driver
 */
public class AlterTableBuilder {

    private AlterTableParamCondition alterTableParamCondition;

    private AlterTableBuilder() {
        this.alterTableParamCondition = new AlterTableParamCondition();
        this.alterTableParamCondition.setDelTableFieldParamList(new ArrayList<>());
        this.alterTableParamCondition.setAddTableFieldParamList(new ArrayList<>());
        this.alterTableParamCondition.setUpdateTableFieldParamList(new ArrayList<>());
        this.alterTableParamCondition.setAddForeignParamList(new ArrayList<>());
        this.alterTableParamCondition.setDelForeignParamList(new ArrayList<>());
    }

    public static AlterTableBuilder builder() {
        return new AlterTableBuilder();
    }

    public AlterTableParamCondition build() {
        return this.alterTableParamCondition;
    }

    public AlterTableBuilder external(boolean isExternal) {
        this.alterTableParamCondition.setExternal(isExternal);
        return this;
    }

    public AlterTableBuilder delColumn(String fieldName) {
        List<String> delTableFieldParamList = this.alterTableParamCondition.getDelTableFieldParamList();
        delTableFieldParamList.add(fieldName);
        return this;
    }

    public AlterTableBuilder delPrimaryKey(boolean delPrimaryKey) {
        this.alterTableParamCondition.setDelPrimaryKey(delPrimaryKey);
        return this;
    }

    public AlterTableBuilder renameTableName(String newTableName) {
        this.alterTableParamCondition.setNewTableName(newTableName);
        return this;
    }

    public AlterTableBuilder tableComment(String tableComment) {
        this.alterTableParamCondition.setTableComment(tableComment);
        return this;
    }

    public UpdateColumnBuilder updateColumn(String newFieldName) {
        AlterTableParamCondition.UpdateTableFieldParam column = new AlterTableParamCondition.UpdateTableFieldParam();
        column.setNewFieldName(newFieldName);
        alterTableParamCondition.getUpdateTableFieldParamList().add(column);
        return UpdateColumnBuilder.builder(this, column);
    }

    public ColumnBuilder addColumn() {
        AlterTableParamCondition.AddTableFieldParam column = new AlterTableParamCondition.AddTableFieldParam();
        alterTableParamCondition.getAddTableFieldParamList().add(column);
        return ColumnBuilder.builder(this, column);
    }

    public ForeignKeyBuilder addForeignKey() {
        AlterTableParamCondition.ForeignParam foreignParam = new AlterTableParamCondition.ForeignParam();
        alterTableParamCondition.getAddForeignParamList().add(foreignParam);
        return ForeignKeyBuilder.builder(this, foreignParam);
    }

    public AlterTableBuilder delPartition(PartitionInfo partitionInfo) {
        alterTableParamCondition.setDelPartition(partitionInfo);
        return this;
    }

    public AlterTableBuilder delMultiLevelPartition(MultiLevelPartitionInfo partitionInfo) {
        alterTableParamCondition.setDelMultiLevelPartition(partitionInfo);
        return this;
    }

    public AlterTableBuilder addPartition(PartitionInfo partitionInfo) {
        alterTableParamCondition.setAddPartition(partitionInfo);
        return this;
    }

    public AlterTableBuilder addMultiLevelPartition(MultiLevelPartitionInfo partitionInfo) {
        alterTableParamCondition.setAddMultiLevelPartition(partitionInfo);
        return this;
    }

    public ForeignKeyBuilder delForeignKey() {
        AlterTableParamCondition.ForeignParam foreignParam = new AlterTableParamCondition.ForeignParam();
        alterTableParamCondition.getDelForeignParamList().add(foreignParam);
        return ForeignKeyBuilder.builder(this, foreignParam);
    }

    /**
     * 字段构造器
     */
    public static class ForeignKeyBuilder {
        private AlterTableParamCondition.ForeignParam foreignParam;

        private AlterTableBuilder alterTableBuilder;

        private ForeignKeyBuilder(AlterTableBuilder alterTableBuilder, AlterTableParamCondition.ForeignParam foreignParam) {
            this.alterTableBuilder = alterTableBuilder;
            this.foreignParam = foreignParam;
        }

        public static AlterTableBuilder.ForeignKeyBuilder builder(AlterTableBuilder alterTableBuilder, AlterTableParamCondition.ForeignParam foreignParam) {
            return new AlterTableBuilder.ForeignKeyBuilder(alterTableBuilder, foreignParam);
        }

        public AlterTableBuilder returned() {
            return alterTableBuilder;
        }

        public AlterTableBuilder.ForeignKeyBuilder foreignName(String foreignName) {
            this.foreignParam.setForeignName(foreignName);
            return this;
        }

        public AlterTableBuilder.ForeignKeyBuilder foreignKey(String foreignKey) {
            this.foreignParam.setForeignKey(foreignKey);
            return this;
        }

        public AlterTableBuilder.ForeignKeyBuilder referencesTableName(String referencesTableName) {
            this.foreignParam.setReferencesTableName(referencesTableName);
            return this;
        }

        public AlterTableBuilder.ForeignKeyBuilder referencesField(String referencesField) {
            this.foreignParam.setReferencesField(referencesField);
            return this;
        }

        public AlterTableBuilder.ForeignKeyBuilder onDeletion(ForeignKeyActionEnum onDeletion) {
            this.foreignParam.setOnDeletion(onDeletion);
            return this;
        }

        public AlterTableBuilder.ForeignKeyBuilder onUpdate(ForeignKeyActionEnum onUpdate) {
            this.foreignParam.setOnUpdate(onUpdate);
            return this;
        }
    }

    public static class UpdateColumnBuilder extends ColumnBuilder {

        private AlterTableParamCondition.UpdateTableFieldParam column;

        private UpdateColumnBuilder(AlterTableBuilder alterTableBuilder, AlterTableParamCondition.UpdateTableFieldParam column) {
            super(alterTableBuilder, column);
            this.column = column;
        }

        public AlterTableBuilder.ColumnBuilder dropDefaultValue(boolean dropDefaultValue) {
            this.column.setDropDefaultValue(dropDefaultValue);
            return this;
        }

        public AlterTableBuilder.ColumnBuilder dropNotNull(boolean dropNotNull) {
            this.column.setDropNotNull(dropNotNull);
            return this;
        }

        public AlterTableBuilder.ColumnBuilder dropFlag(boolean dropFlag) {
            this.column.setDropFlag(dropFlag);
            return this;
        }

        public static UpdateColumnBuilder builder(AlterTableBuilder alterTableBuilder, AlterTableParamCondition.UpdateTableFieldParam column) {
            return new UpdateColumnBuilder(alterTableBuilder, column);
        }

    }

    /**
     * 字段构造器
     */
    public static class ColumnBuilder {
        private AlterTableParamCondition.AddTableFieldParam column;

        private AlterTableBuilder alterTableBuilder;

        private ColumnBuilder(AlterTableBuilder alterTableBuilder, AlterTableParamCondition.AddTableFieldParam column) {
            this.alterTableBuilder = alterTableBuilder;
            this.column = column;
        }

        public static AlterTableBuilder.ColumnBuilder builder(AlterTableBuilder alterTableBuilder, AlterTableParamCondition.AddTableFieldParam column) {
            return new AlterTableBuilder.ColumnBuilder(alterTableBuilder, column);
        }

        public AlterTableBuilder returned() {
            return alterTableBuilder;
        }

        public AlterTableBuilder.ColumnBuilder fieldName(String fieldName) {
            this.column.setFieldName(fieldName);
            return this;
        }

        public AlterTableBuilder.ColumnBuilder fieldComment(String fieldComment) {
            this.column.setFieldComment(fieldComment);
            return this;
        }

        public AlterTableBuilder.ColumnBuilder fieldType(ItemFieldTypeEnum itemFieldType) {
            this.column.setFieldType(itemFieldType.getVal());
            return this;
        }

        public AlterTableBuilder.ColumnBuilder objectFieldParamList(List<AlterTableParamCondition.AddTableFieldParam> objectFieldParamList) {
            this.column.setObjectFieldParamList(objectFieldParamList);
            return this;
        }

        public AlterTableBuilder.ColumnBuilder fieldLength(Integer fieldLength) {
            this.column.setFieldLength(fieldLength);
            return this;
        }

        public AlterTableBuilder.ColumnBuilder fieldDecimalPoint(Integer fieldDecimalPoint) {
            this.column.setFieldDecimalPoint(fieldDecimalPoint);
            return this;
        }

        public AlterTableBuilder.ColumnBuilder primaryKey(boolean primaryKey) {
            this.column.setPrimaryKey(primaryKey);
            return this;
        }

        public AlterTableBuilder.ColumnBuilder notNull(boolean notNull) {
            this.column.setNotNull(notNull);
            return this;
        }

        public AlterTableBuilder.ColumnBuilder tokenizer(String tokenizer) {
            this.column.setTokenizer(tokenizer);
            return this;
        }

        public AlterTableBuilder.ColumnBuilder isAnalyzer(boolean isAnalyzer) {
            this.column.setAnalyzer(isAnalyzer);
            return this;
        }

        public AlterTableBuilder.ColumnBuilder copyToField(String copyToField) {
            this.column.setCopyToField(copyToField);
            return this;
        }

        public AlterTableBuilder.ColumnBuilder defaultValue(String defaultValue) {
            this.column.setDefaultValue(defaultValue);
            return this;
        }

        public AlterTableBuilder.ColumnBuilder onUpdate(String onUpdate) {
            this.column.setOnUpdate(onUpdate);
            return this;
        }

        public AlterTableBuilder.ColumnBuilder extendKeyword(boolean extendKeyword) {
            this.column.setExtendKeyword(extendKeyword);
            return this;
        }

        public AlterTableBuilder.ColumnBuilder keywordIgnoreAbove(Integer keywordIgnoreAbove) {
            if (keywordIgnoreAbove != null) {
                this.column.setKeywordIgnoreAbove(keywordIgnoreAbove);
            }
            return this;
        }

        public AlterTableBuilder.ColumnBuilder unsigned(boolean unsigned) {
            this.column.setUnsigned(unsigned);
            return this;
        }

        public AlterTableBuilder.ColumnBuilder isNotExists(boolean isNotExists) {
            this.column.setNotExists(isNotExists);
            return this;
        }
    }

}
