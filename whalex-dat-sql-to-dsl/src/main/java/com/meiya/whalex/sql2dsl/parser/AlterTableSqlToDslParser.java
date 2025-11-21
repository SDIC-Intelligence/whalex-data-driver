package com.meiya.whalex.sql2dsl.parser;

import com.meiya.whalex.db.builder.AlterTableBuilder;
import com.meiya.whalex.db.entity.IndexParamCondition;
import com.meiya.whalex.interior.db.constant.ForeignKeyActionEnum;
import com.meiya.whalex.interior.db.constant.PartitionType;
import com.meiya.whalex.interior.db.operation.in.AlterTableParamCondition;
import com.meiya.whalex.interior.db.operation.in.PartitionInfo;
import com.meiya.whalex.sql2dsl.entity.DatColumn;
import com.meiya.whalex.sql2dsl.util.DataTypeUtil;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.stream.Collectors;

/**
 * sql转dsl
 *
 * @author 蔡荣桂
 * @date 2023/02/02
 * @project whalex-dat-sql
 */
public class AlterTableSqlToDslParser extends AbstractSqlToDslParser<Object> {

    private String sql;

    private String tableName;

    private String newTableName;

    private ColumnSqlHandler  columnSqlHandler;

    public AlterTableSqlToDslParser(String sql) {
        this.sql = sql;
        this.columnSqlHandler = new ColumnSqlHandler(sql);
    }

    private String operator;

    private String indexName;

    private boolean delPrimaryKey;

    private String delForeignName;

    private List<String> indexColumns;

    private DatColumn columnObject = new DatColumn();

    private AddForeignKeyInfo addForeignKeyInfo;

    private boolean isNotExists;

    /**
     * 新增分区
     */
    private PartitionInfo partitionInfo;

    /**
     * 删除分区
     */
    private PartitionInfo delPartitionInfo;

    private static class AddForeignKeyInfo {
        public String foreignKey;
        public String referencesTableName;
        public String referencesField;
        public ForeignKeyActionEnum onDelete;
        public ForeignKeyActionEnum onUpdate;
        private String foreignKeyName;
    }


    /**
     * 新增字段：alter table name add column data_type [set {default value, not null}]
     * 删除字段： alter table name drop column
     * 修改字段类型： alter table name alter column type data_type
     * 修改字段默认值： alter table name alter column {drop default, drop not null, set default value, set not null}
     * 修改表名：alter table name rename to new_name
     * 修改字段名：alter table name rename column to new_column
     * @throws SqlParseException
     */
    public void _handle() throws SqlParseException {

        char[] chars = sql.toCharArray();

        Queue<String> queue = parserWord(chars);

        // alter table name
        String alter = queue.poll();
        if (!alter.equalsIgnoreCase("alter")) {
            throw new RuntimeException("未知的sql:" + sql + ", 无法识别关键字" + alter + ", 请使用alter关键字");
        }


        String table = queue.poll();
        if (!table.equalsIgnoreCase("table")) {
            throw new RuntimeException("未知的sql:" + sql + ", 无法识别关键字" + table + ", 请使用table关键字");
        }

        //表名
        String[] split = queue.poll().split("\\.");
        tableName = removeQuoting(split[split.length - 1]);

        //操作符
        operator = queue.poll().toLowerCase();

        switch (operator) {
            case "alter":
                alter(queue);
                break;
            case "modify":
                modify(queue);
                break;
            case "rename":
                rename(queue);
                break;
            case "drop":
                drop(queue);
                break;
            case "add":
                add(queue);
                break;
            default:
                throw new RuntimeException("未知的sql:" + sql + ", 无法识别操作符[" + operator + "], " +
                        "目前支持操作符[alter, modify, rename, drop, add]");

        }
    }

    private void modify(Queue<String> queue) {
        String column = queue.poll();
        if (!column.equalsIgnoreCase("column")) {
            throw new RuntimeException("未知的sql:" + sql + ", 无法识别关键字[" + column + "], " +
                    "modify缺少关键字[column]");
        }

        //字段
        columnObject = columnSqlHandler.getColumn(queue);
    }

    /**
     * 新增字段：alter table name add column data_type [set {default value, not null}]
     * 删除字段： alter table name drop column
     * 修改字段类型： alter table name alter column type data_type
     * 修改字段默认值： alter table name alter column {drop default, drop not null, set default value, set not null}
     * 修改表名：alter table name rename to new_name
     * 修改字段名：alter table name rename column to new_column
     * @throws SqlParseException
     */
    @Override
    public Object handle() throws SqlParseException {

        _handle();

        if(StringUtils.isBlank(tableName)) {
            throw new RuntimeException("表名不能为空");
        }

        if(StringUtils.isBlank(operator)) {
            throw new RuntimeException("操作类型不能为空");
        }

        AlterTableBuilder alterTableBuilder  = AlterTableBuilder.builder();

        String defaultValue = null;
        String onUpdate = null;
        if(columnObject.defaultValue != null) {
            defaultValue = columnObject.defaultValue.toString();
        }

        if(columnObject.onUpdate != null) {
            onUpdate = columnObject.onUpdate.toString();
        }

        switch (operator) {
            case "rename":
            case "alter":
            case "modify":

                if(newTableName == null) {

                    AlterTableBuilder.UpdateColumnBuilder columnBuilder = alterTableBuilder.updateColumn(columnObject.newName);
                    columnBuilder.fieldName(columnObject.name);
                    columnBuilder.fieldLength(columnObject.length);
                    columnBuilder.fieldDecimalPoint(columnObject.decimalPointLength);
                    columnBuilder.primaryKey(columnObject.primaryKey);
                    columnBuilder.defaultValue(defaultValue);
                    columnBuilder.dropFlag(columnObject.isAlterDropFlag);
                    columnBuilder.dropNotNull(columnObject.dropNotNull);
                    columnBuilder.dropDefaultValue(columnObject.removeDefault);
                    columnBuilder.onUpdate(onUpdate);
                    columnBuilder.notNull(!columnObject.nullable);
                    columnBuilder.fieldComment(columnObject.comment);
                    if(columnObject.dataType != null) {
                        columnBuilder.fieldType(DataTypeUtil.getItemFieldTypeEnum(columnObject.dataType));
                    }
                }else {
                    alterTableBuilder.renameTableName(newTableName);
                }

                break;


            case "drop":
                if(delPrimaryKey) {
                    alterTableBuilder.delPrimaryKey(delPrimaryKey);
                }else if(StringUtils.isNotBlank(delForeignName)){
                    alterTableBuilder.delForeignKey().foreignName(delForeignName);
                }else if(delPartitionInfo != null){
                    alterTableBuilder.delPartition(delPartitionInfo);
                }else {
                    alterTableBuilder.delColumn(columnObject.name);
                }
                break;


            case "add":

                //新增索引
                if(StringUtils.isNotBlank(indexName)) {
                    IndexParamCondition indexParamCondition = new IndexParamCondition();
                    indexParamCondition.setIndexName(indexName);
                    List<IndexParamCondition.IndexColumn> indexColumnList = indexColumns.stream().map(field -> {
                        IndexParamCondition.IndexColumn indexColumn = new IndexParamCondition.IndexColumn();
                        indexColumn.setColumn(field);
                        return indexColumn;
                    }).collect(Collectors.toList());
                    indexParamCondition.setColumns(indexColumnList);
                    return indexParamCondition;
                }

                //新增外键
                if(addForeignKeyInfo != null) {
                    alterTableBuilder.addForeignKey()
                            .foreignName(addForeignKeyInfo.foreignKeyName)
                            .foreignKey(addForeignKeyInfo.foreignKey)
                            .referencesTableName(addForeignKeyInfo.referencesTableName)
                            .referencesField(addForeignKeyInfo.referencesField)
                            .onDeletion(addForeignKeyInfo.onDelete)
                            .onUpdate(addForeignKeyInfo.onUpdate)
                            .returned();
                }else if(partitionInfo != null) {
                    alterTableBuilder.addPartition(partitionInfo);
                }else {
                    alterTableBuilder.addColumn()
                            .fieldName(columnObject.name)
                            .fieldLength(columnObject.length)
                            .fieldDecimalPoint(columnObject.decimalPointLength)
                            .primaryKey(columnObject.primaryKey)
                            .fieldType(DataTypeUtil.getItemFieldTypeEnum(columnObject.dataType))
                            .fieldComment(columnObject.comment)
                            .defaultValue(defaultValue)
                            .isNotExists(isNotExists)
                            .onUpdate(onUpdate)
                            .notNull(!columnObject.nullable).returned();
                }

                break;
            default:
                throw new RuntimeException("未知的sql:" + sql + ", 无法识别操作符[" + operator + "], " +
                        "目前支持操作符[alter, modify, rename, drop, add]");
        }

        AlterTableParamCondition tableParamCondition = alterTableBuilder.build();

        return tableParamCondition;
    }

    @Override
    public String getTableName() {
        return tableName;
    }

    /**
     * 修改表名：alter table name rename to new_name
     * 修改字段名：alter table name rename [column] column_field to new_column_field
     * @param queue
     */
    private void rename(Queue<String> queue) {
        String unKnow = queue.poll();

        //修改表名
        if(unKnow.equalsIgnoreCase("to")) {
            newTableName = removeQuoting(queue.poll());
            return;
        }

        //修改字段名
        if (unKnow.equalsIgnoreCase("column")) {
            unKnow = removeQuoting(queue.poll());
        }

        //字段
        columnObject.name = unKnow;

        String to = queue.poll();
        if (StringUtils.isBlank(to)) {
            throw new RuntimeException("未知的sql:" + sql + ", 无法识别关键字[" + to + "], " +
                    "字段名[" + columnObject.name + "]后缺少关键字[to]");
        } else {
            if (!to.equalsIgnoreCase("to")) {
                throw new RuntimeException("未知的sql:" + sql + ", 无法识别关键字[" + to + "], " +
                        "字段名[" + columnObject.name + "]后缺少关键字[to]");
            }
        }

        columnObject.newName = removeQuoting(queue.poll());

    }

    /**
     * 删除字段： alter table name drop column
     * 删除主键： alter table name drop primary key
     * @param queue
     */
    private void drop(Queue<String> queue) {
        String word = queue.poll();

        if(word.equalsIgnoreCase("primary")) {
            word = queue.poll();
            if(!word.equalsIgnoreCase("key")) {
                throw new RuntimeException("未知的sql:" + sql + ", 无法识别关键字[" + word + "], " +
                        "primary缺少关键字[key]");
            }
            if(!queue.isEmpty()) {
                throw new RuntimeException("未知的sql:" + sql + ", 未知的结束符[" + queue.element() + "]");
            }
            delPrimaryKey = true;
            return;
        }else if(word.equalsIgnoreCase("foreign")) {
            word = queue.poll();
            if(!word.equalsIgnoreCase("key")) {
                throw new RuntimeException("未知的sql:" + sql + ", 无法识别关键字[" + word + "], " +
                        "foreign缺少关键字[key]");
            }

            delForeignName = removeQuoting(queue.poll());

            if(!queue.isEmpty()) {
                throw new RuntimeException("未知的sql:" + sql + ", 未知的结束符[" + queue.element() + "]");
            }

            return;
        }else if(word.equalsIgnoreCase("partition")){
            delPartitionInfo = new PartitionInfo();
            //(`create_date`='2023-05-04')
            // "dat_del_partition_0"
            word = queue.poll();
            if(word.equalsIgnoreCase("(")) {
                delPartitionInfo.setPartitionField(removeQuoting(queue.poll()));
                delPartitionInfo.setType(PartitionType.EQ);
                word = queue.poll();
                if(!word.equalsIgnoreCase("=")) {
                    throw new RuntimeException("未知的sql:" + sql + ", 分区解析， 无法识别操作符[" + word + "], " +
                            "目前支持操作符[=]");
                }

                PartitionInfo.PartitionEq partitionEq = new PartitionInfo.PartitionEq();
                partitionEq.setValue(removeApostrophe(queue.poll()));
                if(!queue.poll().equalsIgnoreCase(")")) {
                    throw new RuntimeException("未知的sql:" + sql + ", 分区解析， 缺少), 在"  + partitionEq.getValue() + "附近");
                }

                if(!queue.isEmpty()) {
                    throw new RuntimeException("未知的sql:" + sql + ", 未知的结束符[" + queue.element() + "]");
                }
                delPartitionInfo.setEq(Arrays.asList(partitionEq));
            }else {
                PartitionInfo.PartitionHash partitionHash = new PartitionInfo.PartitionHash();
                partitionHash.setPartitionName(removeQuoting(word));
                delPartitionInfo.setHash(Arrays.asList(partitionHash));
                delPartitionInfo.setType(PartitionType.HASH);
                if(!queue.isEmpty()) {
                    throw new RuntimeException("未知的sql:" + sql + ", 未知的结束符[" + queue.element() + "]");
                }
            }
            return;
        } else if (!word.equalsIgnoreCase("column")) {
            throw new RuntimeException("未知的sql:" + sql + ", 无法识别关键字[" + word + "], " +
                    "partition缺少关键字[column]");
        }

        //字段
        columnObject.name = removeQuoting(queue.poll());
    }

    /**
     * 新增字段：alter table name add column data_type [default value] [not null] [comment 'desc']
     * 新增字段：alter table name add column if not exists data_type [default value] [not null] [comment 'desc']
     * 新增外键： ALTER TABLE `%s` ADD CONSTRAINT `%s` FOREIGN KEY(`%s`) REFERENCES `%s`(`%s`) ON DELETE %s ON UPDATE %s
     * 新增分区：
     *  ALTER TABLE `addPartition` ADD PARTITION `create_date` values eq ('2023-05-04') location '/dat_test/test'
     *  ALTER TABLE `dat_add_partition` ADD PARTITION `dat_add_partition_0` VALUES IN ('1', '2', '3', '4')
     *  ALTER TABLE `dat_add_partition` ADD PARTITION `dat_add_partition_0` VALUES start ('100000') end(MAXVALUE)
     *  ALTER TABLE `dat_add_partition` ADD PARTITION `dat_add_partition_0` VALUES WITH (MODULUS 2, REMAINDER 1)
     * @param queue
     */
    private void add(Queue<String> queue) {
        String column = queue.poll();

        //新增外键
        if(column.equalsIgnoreCase("constraint")) {
            addForeignKey(queue);
            return;
        }

        //新增分区
        if(column.equalsIgnoreCase("PARTITION")) {
            addPartition(queue);
            return;
        }

        //新增索引
        if(column.equalsIgnoreCase("index")) {

            indexName = removeQuoting(queue.poll());

            if(!queue.poll().equalsIgnoreCase("(")) {
                throw new RuntimeException("未知的sql:" + sql + ", 新增索引解析， 缺少(, 在"  + indexName + "附近");
            }

            indexColumns = new ArrayList<>();

            while (true) {
                if(queue.isEmpty()) {
                    throw new RuntimeException("未知的sql:" + sql + ", 新增索引解析， 缺少)");
                }

                String word = removeQuoting(queue.poll());

                indexColumns.add(word);

                word = queue.poll();

                if(word.equalsIgnoreCase(")")) {
                    return;
                }
            }
        }


        if (!column.equalsIgnoreCase("column")) {
            throw new RuntimeException("未知的sql:" + sql + ", 无法识别关键字[" + column + "], " +
                    "add缺少关键字[column]");
        }


        // if not exists
        if(queue.element().equalsIgnoreCase("if")) {
            queue.poll();
            String not = queue.poll();
            String exists = queue.poll();
            if(!(not.equalsIgnoreCase("not")
                    && exists.equalsIgnoreCase("exists"))) {
                throw new RuntimeException("未知的sql:" + sql + ", 解析约束条件[IF NOT EXISTS], 无法识别" + not + " " + exists);
            }
            isNotExists = true;
        }

        columnObject = columnSqlHandler.getColumn(queue);
    }

    /**
     *  ALTER TABLE `addPartition` ADD PARTITION `create_date` values eq ('2023-05-04') location '/dat_test/test'
     *  ALTER TABLE `dat_add_partition` ADD PARTITION `dat_add_partition_0` VALUES IN ('1', '2', '3', '4')
     *  ALTER TABLE `dat_add_partition` ADD PARTITION `dat_add_partition_0` VALUES start ('100000') end(MAXVALUE)
     * @param queue
     */
    private void addPartition(Queue<String> queue) {
        partitionInfo = new PartitionInfo();
        String partitionName = removeQuoting(queue.poll());
        if(!queue.poll().equalsIgnoreCase("values")) {
            throw new RuntimeException("未知的sql:" + sql + ", 新增分区缺少关键字[values], 在"  + partitionName + "附近");
        }

        String word = queue.poll();
        PartitionType partitionType = null;
        if(word.equalsIgnoreCase("eq")){

            partitionType = PartitionType.EQ;
            PartitionInfo.PartitionEq partitionEq = new PartitionInfo.PartitionEq();

            if(!queue.poll().equalsIgnoreCase("(")) {
                throw new RuntimeException("未知的sql:" + sql + ", 新增分区缺少(, 在"  + word + "附近");
            }
            partitionEq.setValue(removeApostrophe(queue.poll()));
            if(!queue.poll().equalsIgnoreCase(")")) {
                throw new RuntimeException("未知的sql:" + sql + ", 新增分区缺少), 在"  + partitionEq.getValue() + "附近");
            }

            if(!queue.isEmpty()) {
                word = queue.poll();
                if(!word.equalsIgnoreCase("location")) {
                    throw new RuntimeException("未知的sql:" + sql + ", 新增分区， 无法识别关键字[" + word + "], " +
                            "支持关键字[location]");
                }
                partitionEq.setPath(removeApostrophe(queue.poll()));
            }

            if(!queue.isEmpty()) {
                throw new RuntimeException("未知的sql:" + sql + ", 未知的结束符[" + queue.element() + "]");
            }

            partitionInfo.setPartitionField(partitionName);
            partitionInfo.setEq(Arrays.asList(partitionEq));
            partitionInfo.setType(partitionType);
        }else if (word.equalsIgnoreCase("IN")){
            partitionType = PartitionType.LIST;

            if(!queue.poll().equalsIgnoreCase("(")) {
                throw new RuntimeException("未知的sql:" + sql + ", 新增分区缺少(, 在"  + word + "附近");
            }

            List<String> list = new ArrayList<>();

            boolean isFinish = false;
            while (!queue.isEmpty()) {
                list.add(removeApostrophe(queue.poll()));
                word = queue.poll();
                if(word.equalsIgnoreCase(",")) {
                    continue;
                }else if(word.equalsIgnoreCase(")")) {
                    isFinish = true;
                    break;
                }
            }

            if(!isFinish) {
                throw new RuntimeException("未知的sql:" + sql + ", 新增分区缺少), 在"  + word + "附近");
            }

            if(!queue.isEmpty()) {
                throw new RuntimeException("未知的sql:" + sql + ", 未知的结束符[" + queue.element() + "]");
            }

            PartitionInfo.PartitionList partitionList = new PartitionInfo.PartitionList();
            partitionList.setPartitionName(partitionName);
            partitionList.setList(list);

            partitionInfo.setIn(Arrays.asList(partitionList));
            partitionInfo.setType(partitionType);
        }else if(word.equalsIgnoreCase("start") || word.equalsIgnoreCase("end")) {
            partitionType = PartitionType.RANGE;
            PartitionInfo.PartitionRange partitionRange = new PartitionInfo.PartitionRange();
            partitionRange.setPartitionName(partitionName);
            //start ('100000') end(MAXVALUE)
            if(word.equalsIgnoreCase("start")) {
                if(!queue.poll().equalsIgnoreCase("(")) {
                    throw new RuntimeException("未知的sql:" + sql + ", 新增分区缺少(, 在"  + word + "附近");
                }

                partitionRange.setLeft(removeApostrophe(queue.poll()));

                if(!queue.poll().equalsIgnoreCase(")")) {
                    throw new RuntimeException("未知的sql:" + sql + ", 新增分区缺少), 在"  + partitionRange.getLeft() + "附近");
                }

                if(!queue.isEmpty()) {
                    word = queue.poll();
                }
            }

            if(word.equalsIgnoreCase("end")) {
                if(!queue.poll().equalsIgnoreCase("(")) {
                    throw new RuntimeException("未知的sql:" + sql + ", 新增分区缺少(, 在"  + word + "附近");
                }

                partitionRange.setRight(removeApostrophe(queue.poll()));

                if(!queue.poll().equalsIgnoreCase(")")) {
                    throw new RuntimeException("未知的sql:" + sql + ", 新增分区缺少), 在"  + partitionRange.getRight() + "附近");
                }
            }

            if(!queue.isEmpty()) {
                throw new RuntimeException("未知的sql:" + sql + ", 未知的结束符[" + queue.element() + "]");
            }

            partitionInfo.setType(partitionType);
            partitionInfo.setRange(Arrays.asList(partitionRange));
        }

        if(partitionType == null) {
            throw new RuntimeException("未知的sql:" + sql + ", 解析新增分区, 未知的分区类型[" + word + "]");
        }
    }

    /**
     * ALTER TABLE `%s` ADD CONSTRAINT `%s` FOREIGN KEY(`%s`) REFERENCES `%s`(`%s`) ON DELETE %s ON UPDATE %s
     * 新增外键
     * @param queue
     */
    private void addForeignKey(Queue<String> queue) {

        addForeignKeyInfo = new AddForeignKeyInfo();

        addForeignKeyInfo.foreignKeyName = removeQuoting(queue.poll());


        String foreign = queue.poll();
        String key = queue.poll();

        if(!foreign.equalsIgnoreCase("FOREIGN")
                || !key.equalsIgnoreCase("KEY")) {
            throw new RuntimeException("未知的sql:" + sql + ", 解析新增外键， 无法识别关键字[" + foreign + " " + key + "], " +
                    "支持关键字[FOREIGN KEY]");
        }

        if(!queue.poll().equalsIgnoreCase("(")) {
            throw new RuntimeException("未知的sql:" + sql + ", 新增外键缺少(, 在" + key + "附近");
        }

        addForeignKeyInfo.foreignKey = removeQuoting(queue.poll());

        if(!queue.poll().equalsIgnoreCase(")")) {
            throw new RuntimeException("未知的sql:" + sql + ", 新增外键缺少), 在" + addForeignKeyInfo.foreignKey + "附近");
        }


        String references = queue.poll();

        if(!references.equalsIgnoreCase("REFERENCES")) {
            throw new RuntimeException("未知的sql:" + sql + ", 解析新增外键， 无法识别关键字[" + references + "], " +
                    "支持关键字[REFERENCES]");
        }

        addForeignKeyInfo.referencesTableName = removeQuoting(queue.poll());

        if(!queue.poll().equalsIgnoreCase("(")) {
            throw new RuntimeException("未知的sql:" + sql + ", 新增外键缺少(, 在" + references + "附近");
        }

        addForeignKeyInfo.referencesField = removeQuoting(queue.poll());

        if(!queue.poll().equalsIgnoreCase(")")) {
            throw new RuntimeException("未知的sql:" + sql + ", 新增外键缺少), 在" + addForeignKeyInfo.referencesField + "附近");
        }

        while(!queue.isEmpty()) {

            String word = queue.poll();

            if(!word.equalsIgnoreCase("ON")) {
                throw new RuntimeException("未知的sql:" + sql + ", 解析新增外键， 无法识别关键字[" + word + "], " +
                        "支持关键字[ON]");
            }

            word = queue.poll();

            if(word.equalsIgnoreCase("DELETE")) {
                addForeignKeyInfo.onDelete = getForeignKeyActionEnum(queue);
            }else if(word.equalsIgnoreCase("UPDATE")) {
                addForeignKeyInfo.onUpdate = getForeignKeyActionEnum(queue);
            }else {
                throw new RuntimeException("未知的sql:" + sql + ", 解析新增外键， 无法识别关键字[" + word + "], " +
                        "支持关键字[DELETE, UPDATE]");
            }
        }
    }

    // CASCADE("CASCADE"),
    // NO_ACTION("NO ACTION"),
    // RESTRICT("RESTRICT"),
    // SET_NULL("SET NULL");
    private ForeignKeyActionEnum getForeignKeyActionEnum(Queue<String> queue) {
        String word = queue.poll();
        if(word.equalsIgnoreCase("CASCADE")) {
            return ForeignKeyActionEnum.CASCADE;
        }else if(word.equalsIgnoreCase("NO")) {
            if(queue.poll().equalsIgnoreCase("ACTION")) {
                return ForeignKeyActionEnum.NO_ACTION;
            }
        }else if(word.equalsIgnoreCase("SET")) {
            if(queue.poll().equalsIgnoreCase("NULL")) {
                return ForeignKeyActionEnum.SET_NULL;
            }
        }
        throw new RuntimeException("未知的sql:" + sql + ", 解析新增外键， 无法识别关键字[" + word + "], " +
                "支持关键字[CASCADE, NO ACTION, SET NULL]");
    }

    /**
     * 修改字段类型： alter table name alter column type data_type
     * 修改字段默认值： alter table name alter column {drop default, drop not null, set default value, set not null}
     * @param queue
     */
    private void alter(Queue<String> queue) {
        String column = queue.poll();
        if (!column.equalsIgnoreCase("column")) {
            throw new RuntimeException("未知的sql:" + sql + ", 解析修改字段， 无法识别关键字[" + column + "], " +
                    "支持关键字[COLUMN]");
        }

        //字段
        columnObject.name = removeQuoting(queue.poll());

        String operator = queue.element();
        if(operator.equalsIgnoreCase("type")) {
            queue.poll();
            columnSqlHandler.handleColumn(columnObject, queue);
            return;
        }

        while (queue.size() > 0) {
            constraintHandle(queue);
        }

    }

    //约束处理
    private void constraintHandle(Queue<String> queue) {

        String operator = queue.poll().toLowerCase();
        String value = queue.poll();
        switch (operator) {
            case "drop":

                columnObject.isAlterDropFlag = true;

                if (value.equalsIgnoreCase("default")) {
                    columnObject.removeDefault = true;
                    return;
                }

                String nullStr = queue.poll();
                if (value.equalsIgnoreCase("not")
                        && nullStr.equalsIgnoreCase("null")) {
                    columnObject.nullable = true;
                    columnObject.dropNotNull =  true;
                    return;
                }
                throw new RuntimeException("未知的sql:" + sql + ", 解析修改字段， 无法识别约束条件[" + operator + " " + value + " " + nullStr + "], " +
                        "支持约束条件[DROP DEFAULT, DROP NOT NULL]");
            case "set":
                if (value.equalsIgnoreCase("default")) {
                    columnObject.defaultValue = removeApostrophe(queue.poll());
                    return;
                }

                String nullStr2 = queue.poll();
                if (value.equalsIgnoreCase("not")
                        && nullStr2.equalsIgnoreCase("null")) {
                    columnObject.nullable = false;
                    return;
                }
                throw new RuntimeException("未知的sql:" + sql + ", 解析修改字段， 无法识别约束条件[" + operator + " " + value + " " + nullStr2 + "], " +
                        "支持约束条件[SET DEFAULT VALUE, SET NOT NULL]");
            default:
                throw new RuntimeException("未知的sql:" + sql + ", 解析修改字段， 无法识别约束条件[" + operator + "], " +
                        "支持关键字[DROP, SET]");
        }
    }

}
