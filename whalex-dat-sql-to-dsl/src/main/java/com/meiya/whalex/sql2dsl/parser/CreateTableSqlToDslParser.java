package com.meiya.whalex.sql2dsl.parser;

import cn.hutool.core.map.MapUtil;
import com.meiya.whalex.db.builder.CreateTableBuilder;
import com.meiya.whalex.db.constant.IndexType;
import com.meiya.whalex.db.entity.IndexParamCondition;
import com.meiya.whalex.interior.db.constant.PartitionType;
import com.meiya.whalex.interior.db.operation.in.CreateTableParamCondition;
import com.meiya.whalex.interior.db.operation.in.PartitionInfo;
import com.meiya.whalex.interior.db.search.condition.Sort;
import com.meiya.whalex.sql2dsl.entity.DatColumn;
import com.meiya.whalex.sql2dsl.entity.SqlCreateTable;
import com.meiya.whalex.sql2dsl.util.DataTypeUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.*;

/**
 * sql转dsl
 *
 * @author 蔡荣桂
 * @date 2023/02/02
 * @project whalex-dat-sql
 */
@Slf4j
public class CreateTableSqlToDslParser extends AbstractSqlToDslParser<SqlCreateTable> {


    private String tableName;

    private String tableNameComment;

    private LinkedHashMap<String, DatColumn> columns;

    private String sql;

    private LinkedHashMap<String, String> properties;

    private PartitionInfo partitionInfo;

    private boolean isNotExists;

    private ColumnSqlHandler columnSqlHandler;

    private List<IndexParamCondition> indexParamConditions = new ArrayList<>();

    private String likeCopyTableName;

    public CreateTableSqlToDslParser(String sql) {
        this.sql = sql;
        this.columnSqlHandler = new ColumnSqlHandler(sql);
    }

    @Override
    public SqlCreateTable handle() throws SqlParseException {
        _handle();

        if (StringUtils.isBlank(tableName)) {
            throw new RuntimeException("表名不能为空");
        }

        if (MapUtil.isEmpty(columns) && StringUtils.isBlank(likeCopyTableName)) {
            throw new RuntimeException("字段列表不能为空或复制目标表名不能为空");
        }

        CreateTableBuilder createTableBuilder = CreateTableBuilder.builder();
        createTableBuilder.isNotExists(isNotExists);
        CreateTableParamCondition tableParamCondition;
        if (StringUtils.isNotBlank(likeCopyTableName)) {
            createTableBuilder.copyTable(likeCopyTableName);
        } else {
            CreateTableBuilder.SchemaBuilder schema = createTableBuilder.schema();
            for (DatColumn column : columns.values()) {
                String defaultValue = null;
                String onUpdate = null;
                if (column.defaultValue != null) {
                    defaultValue = column.defaultValue.toString();
                }
                if(column.onUpdate != null) {
                    onUpdate = column.onUpdate.toString();
                }
                CreateTableBuilder.ColumnBuilder columnBuilder = schema.column()
                        .primaryKey(column.primaryKey)
                        .fieldLength(column.length)
                        .fieldName(column.name)
                        .notNull(!column.nullable)
                        .partition(column.partition)
                        .fieldDecimalPoint(column.decimalPointLength)
                        .fieldComment(column.comment)
                        .fieldType(DataTypeUtil.getItemFieldTypeEnum(column.dataType))
                        .autoIncrement(column.autoIncrement)
                        .startIncrement(column.initAutoIncrementValue)
                        .defaultValue(defaultValue)
                        .onUpdate(onUpdate)
                        .distributed(column.distributed)
                        .unsigned(column.unsigned);

                if(partitionInfo != null && column.name.equalsIgnoreCase(partitionInfo.getPartitionField())) {
                    columnBuilder.partitionInfo(partitionInfo);
                    columnBuilder.partition(true);
                }
            }

            createTableBuilder.tableComment(tableNameComment);
        }
        tableParamCondition = createTableBuilder.build();
        SqlCreateTable.SqlCreateTableBuilder builder = SqlCreateTable.builder().createTable(tableParamCondition);

        if (CollectionUtils.isNotEmpty(indexParamConditions)) {
            builder.createIndex(this.indexParamConditions);
        }

        return builder.build();
    }

    public LinkedHashMap getProperties() {
        return properties;
    }

    @Override
    public String getTableName() {
        return tableName;
    }

    /**
     * SQL 解析
     *
     * @throws SqlParseException
     */
    public void _handle() throws SqlParseException {
//        String sql = "CREATE TABLE test12345 (  id int ( 11 ) NOT NULL ,  num float(4, 0) NOT NULL ,  aaaa double4 NOT NULL ,  name varchar(  255 ) DEFAULT NULL,  create_date datetime DEFAULT NULL,  create_date2 timestamp DEFAULT NULL,  PRIMARY KEY (id))";

        char[] chars = sql.toCharArray();

        Queue<String> queue = parserWord(chars);

        //表名
        tableName = getTableName(queue);

        // 判断 CREATE TABLE LIKE 情况
        likeCopyTableName = getLikeCopyTableName(queue);

        if (StringUtils.isNotBlank(likeCopyTableName)) {
            return;
        }

        //字段
        columns = getColumns(queue);

        while (!queue.isEmpty()) {

            String word = queue.element();

            if(word.equalsIgnoreCase("DISTRIBUTED")) {
                // 获取分布键
                getDistributes(queue);
            }else if(word.equalsIgnoreCase("PARTITION")){
                //获取分区信息
                getPartition(queue);
            }else if(word.equalsIgnoreCase("PARTITIONED")){
                //hive分区
                getHivePartition(queue);
            } else if(word.equalsIgnoreCase("COMMENT")) {
                //获取表的注释
                tableNameComment = getTableComment(queue);
            }else if(word.equalsIgnoreCase("PROPERTIES")) {
                properties = getProperties(queue);
            }else if(word.equalsIgnoreCase("ENGINE")){
                //ENGINE=InnoDB
                queue.poll();
                queue.poll();
                queue.poll();
            }else if(word.equalsIgnoreCase("DEFAULT")) {
                //DEFAULT CHARSET=utf8;
                queue.poll();
                queue.poll();
                queue.poll();
                queue.poll();
            }else if(word.equalsIgnoreCase("CHARACTER")){
                queue.poll();
                queue.poll();
                queue.poll();
                queue.poll();
            }else if(word.equalsIgnoreCase("COLLATE")){
                queue.poll();
                queue.poll();
                queue.poll();
            }else if(word.equalsIgnoreCase("ROW_FORMAT")) {
                queue.poll();
                queue.poll();
                queue.poll();
            }else if(word.equalsIgnoreCase("AUTO_INCREMENT")) {
                queue.poll();
                queue.poll();
                Integer autoIncrement = Integer.valueOf(queue.poll());
                Collection<DatColumn> columns = this.columns.values();
                for (DatColumn column : columns) {
                    if(column.primaryKey && column.autoIncrement) {
                        column.initAutoIncrementValue = autoIncrement;
                    }
                }
            }else {
                throw new RuntimeException("未知的sql:" + sql + ", 无法识别关键字[" + word + "], " +
                        "支持关键字[AUTO_INCREMENT, ROW_FORMAT, COLLATE, CHARACTER, DEFAULT, " +
                        "ENGINE, PROPERTIES, COMMENT, PARTITIONED, PARTITION, DISTRIBUTED]");
            }
        }
    }

    private LinkedHashMap<String, String> getProperties(Queue<String> queue) {
        LinkedHashMap<String, String> propertiesMap = new LinkedHashMap<>();
        queue.poll();
        String word = queue.poll();
        if (!word.equalsIgnoreCase("(")) {
            throw new RuntimeException("未知的sql:" + sql + ", 解析属性缺少(, 在" + word + "附近");
        }
        while (!queue.isEmpty()) {

            word = queue.element();

            //结束标识
            if(word.equalsIgnoreCase(")")) {
                queue.poll();
                break;
            }

            if(isColumnEnd(word)) {
                queue.poll();
            }

            getProperty(queue, propertiesMap);

        }

        return propertiesMap;
    }

    private void getProperty(Queue<String> queue, LinkedHashMap<String, String> propertiesMap) {

        String field = removeDoubleQuotationMarks(queue.poll());
        String sign = queue.poll();
        String value = removeDoubleQuotationMarks(queue.poll());
        if(!sign.equalsIgnoreCase("=")) {
            throw new RuntimeException("未知的sql:" + sql + ", 解析属性, 未知操作符[" + sign + "], 支持操作符[=]");
        }
        if(!isColumnEnd(queue.element())) {
            throw new RuntimeException("未知的sql:" + sql + ", 解析属性, 未知结束符[" + queue.element() + "]");
        }
        propertiesMap.put(field, value);
    }

    /**
     * 解析分布键
     *
     * @param queue
     * @return
     */
    private void getDistributes(Queue<String> queue) {

        queue.poll();
        String by = queue.poll();
        if (!"BY".equalsIgnoreCase(by)) {
            throw new RuntimeException("未知的sql:" + sql + ", 解析分布键, 无法识别关键字[" + by + "], " +
                    "支持关键字[BY]");
        }

        String word = queue.poll();
        if(word.equalsIgnoreCase("HASH")) {
            word = queue.poll();
        }

        if(!"(".equalsIgnoreCase(word)) {
            throw new RuntimeException("未知的sql:" + sql + ", 解析分布键缺少(, 在" + word + "附近");
        }

        while (!queue.isEmpty()) {
            word = queue.element();

            //结束标识
            if (word.equalsIgnoreCase(")")) {
                queue.poll();
                return;
            }

            if(isColumnEnd(word)) {
                queue.poll();
            }

            String distributed = getDistributed(queue);
            DatColumn column = columns.get(distributed);
            if (column == null) {
                throw new RuntimeException("DISTRIBUTE BY 指定分布键不存在: " + distributed);
            }
            column.distributed = true;
        }
    }

    private void getHivePartition(Queue<String> queue) {
        queue.poll();
        String word = queue.poll();
        if (!"BY".equalsIgnoreCase(word)) {
            throw new RuntimeException("未知的sql:" + sql + ", 解析分区, 无法识别关键字[" + word + "], " +
                    "支持关键字[BY]");
        }

        word = queue.poll();
        if(!"(".equalsIgnoreCase(word)) {
            throw new RuntimeException("未知的sql:" + sql + ", 解析分区缺少(, 在" + word + "附近");
        }

        while (!queue.isEmpty()) {
            word = queue.element();
            //结束标识
            if (word.equalsIgnoreCase(")")) {
                queue.poll();
                return;
            }

            if(isColumnEnd(word)) {
                queue.poll();
            }
            //字段处理
            DatColumn column = columnSqlHandler.getColumn(queue);
            column.partition = true;
            columns.put(column.name, column);
        }
    }

    private void getPartition(Queue<String> queue) {

        partitionInfo = new PartitionInfo();

        queue.poll();
        String word = queue.poll();
        if (!"BY".equalsIgnoreCase(word)) {
            throw new RuntimeException("未知的sql:" + sql + ", 解析分区, 无法识别关键字[" + word + "], " +
                    "支持关键字[BY]");
        }

        String partitionTypeStr = queue.poll();
        PartitionType partitionType = PartitionType.valueOf(partitionTypeStr.toUpperCase());
        partitionInfo.setType(partitionType);

        word = queue.poll();
        if(!"(".equalsIgnoreCase(word)) {
            throw new RuntimeException("未知的sql:" + sql + ", 解析分区缺少(, 在" + word + "附近");
        }

        String partitionField = removeQuoting(queue.poll());
        partitionInfo.setPartitionField(partitionField);

        word = queue.poll();
        if(!")".equalsIgnoreCase(word)) {
            throw new RuntimeException("未知的sql:" + sql + ", 解析分区缺少), 在" + word + "附近");
        }

        if(!queue.isEmpty()) {
            word = queue.element();
            if ("(".equalsIgnoreCase(word)) {
                queue.poll();
                while (!queue.isEmpty()) {
                    word = queue.poll();
                    if(!word.equalsIgnoreCase("PARTITION")) {
                        throw new RuntimeException("未知的sql:" + sql + ", 解析分区, 无法识别关键字[" + word + "], " +
                                "支持关键字[PARTITION]");
                    }

                    String partitionName = removeQuoting(queue.poll());
                    if(PartitionType.RANGE.equals(partitionType)) {

                        PartitionInfo.PartitionRange partitionRange = new PartitionInfo.PartitionRange();
                        partitionRange.setPartitionName(partitionName);

                        if(queue.element().equalsIgnoreCase("START")) {
                            queue.poll();
                            word = queue.poll();
                            if(!"(".equalsIgnoreCase(word)) {
                                throw new RuntimeException("未知的sql:" + sql + ", 解析分区缺少(, 在" + word + "附近");
                            }
                            String start = removeApostrophe(queue.poll());
                            partitionRange.setLeft(start);
                            word = queue.poll();
                            if(!")".equalsIgnoreCase(word)) {
                                throw new RuntimeException("未知的sql:" + sql + ", 解析分区缺少), 在" + word + "附近");
                            }
                        }

                        if(queue.element().equalsIgnoreCase("END")) {
                            queue.poll();
                            word = queue.poll();
                            if(!"(".equalsIgnoreCase(word)) {
                                throw new RuntimeException("未知的sql:" + sql + ", 解析分区缺少(, 在" + word + "附近");
                            }
                            String end = removeQuoting(queue.poll());
                            partitionRange.setRight(end);
                            word = queue.poll();
                            if(!")".equalsIgnoreCase(word)) {
                                throw new RuntimeException("未知的sql:" + sql + ", 解析分区缺少), 在" + word + "附近");
                            }
                        }

                        List<PartitionInfo.PartitionRange> range = partitionInfo.getRange();
                        if(range == null){
                            range = new ArrayList<>();
                            partitionInfo.setRange(range);
                        }
                        range.add(partitionRange);

                    }else if(PartitionType.LIST.equals(partitionType)) {

                        word = queue.poll();
                        if(!word.equalsIgnoreCase("IN")) {
                            throw new RuntimeException("未知的sql:" + sql + ", 解析分区, 无法识别关键字[" + word + "], " +
                                    "支持关键字[IN]");
                        }

                        word = queue.poll();
                        if(!"(".equalsIgnoreCase(word)) {
                            throw new RuntimeException("未知的sql:" + sql + ", 解析分区缺少(, 在" + word + "附近");
                        }
                        List<String> list = new ArrayList<>();

                        while (!queue.isEmpty()) {
                            list.add(removeApostrophe(queue.poll()));
                            word = queue.poll();
                            if(word.equalsIgnoreCase(",")) {
                                continue;
                            }else if(word.equalsIgnoreCase(")")) {
                                break;
                            }
                        }

                        PartitionInfo.PartitionList partitionList = new PartitionInfo.PartitionList();
                        partitionList.setPartitionName(partitionName);
                        partitionList.setList(list);
                        List<PartitionInfo.PartitionList> in = partitionInfo.getIn();
                        if(in == null){
                            in = new ArrayList<>();
                            partitionInfo.setIn(in);
                        }
                        in.add(partitionList);
                    }else if(PartitionType.HASH.equals(partitionType)) {
                        PartitionInfo.PartitionHash partitionHash = new PartitionInfo.PartitionHash();
                        partitionHash.setPartitionName(partitionName);
                        List<PartitionInfo.PartitionHash> hash = partitionInfo.getHash();
                        if(hash == null){
                            hash = new ArrayList<>();
                            partitionInfo.setHash(hash);
                        }
                        hash.add(partitionHash);
                    }

                    word = queue.poll();
                    if(word.equalsIgnoreCase(",")) {
                        continue;
                    }else if(word.equalsIgnoreCase(")")) {
                        break;
                    }

                    throw new RuntimeException("未知的sql:" + sql + ", 解析分区, 无法识别关键字[" + word + "], " +
                            "支持关键字[',', ')']");
                    //(PARTITION  "aa_dat_partition_range_0" END('100000'), PARTITION  "aa_dat_partition_range_1" START('100000') END('200000'))

                }
            }

        }
    }

    /**
     * 获取分布键
     *
     * @param queue
     * @return
     */
    private String getDistributed(Queue<String> queue) {
        String distributed = removeQuoting(queue.poll());
        String word = queue.element();
        if (isColumnEnd(word)) {
            return distributed;
        } else {
            throw new RuntimeException("未知的sql:" + sql + ", 未知的结束符[" + word + "], 在" + distributed + "附近");
        }
    }

    private String getTableComment(Queue<String> queue) {

        String comment = null;

        queue.poll();
        String word = queue.poll();
        if (word.equalsIgnoreCase("=")) {
            comment = queue.poll();
            comment = comment.substring(1, comment.length() - 1);
        } else {
            comment = word.substring(1, word.length() - 1);
        }

        return comment;
    }

    private boolean isColumnEnd(String word) {
        if (word.equalsIgnoreCase(",") || word.equalsIgnoreCase(")")) {
            return true;
        }
        return false;
    }


    private LinkedHashMap<String, DatColumn> getColumns(Queue<String> queue) {

        LinkedHashMap<String ,DatColumn> columns = new LinkedHashMap<>();

        String word = queue.poll();

        if (!word.equalsIgnoreCase("(")) {
            throw new RuntimeException("未知的sql:" + sql + ", 解析字段缺少(, 在" + word + "附近");
        }

        while (!queue.isEmpty()) {

            word = queue.element();


            //结束标识
            if (word.equalsIgnoreCase(")")) {
                queue.poll();
                return columns;
            }

            if(isColumnEnd(word)) {
                queue.poll();
            } else if (word.equalsIgnoreCase("primary")) {
                //主键处理
                setPrimaryKey(queue, columns);
            } else if (word.equalsIgnoreCase("constraint")) {
                //约束处理
                constraintHandler(queue, columns);
            } else if (word.equalsIgnoreCase("key") || word.equalsIgnoreCase("index") || word.equalsIgnoreCase("unique")) {
                // 建索引处理
                this.indexParamConditions.add(createIndexHandler(queue));
            } else {
                //字段处理
                DatColumn column = columnSqlHandler.getColumn(queue);
                columns.put(column.name, column);
            }

        }

        return columns;
    }

    /**
     * 解析 CREATE TABLE LIKE
     * @param queue
     * @return
     */
    private String getLikeCopyTableName(Queue<String> queue) {
        String element = queue.element();
        if (StringUtils.equalsIgnoreCase(element, "LIKE")) {
            queue.poll();
            String work = queue.poll();
            String[] names = work.split("\\.");

            if (names.length == 2) {
                work = names[1];
            } else {
                work = names[0];
            }

            String likeCopyTableName = removeQuoting(work);
            String poll = queue.poll();
            boolean flag = false;
            while (StringUtils.equalsIgnoreCase(poll, "INCLUDING")) {
                if (!flag) {
                    flag = true;
                }
                queue.poll();
                poll = queue.poll();
            }
            if (flag) {
                log.warn("DAT SQL {} 将忽略 INCLUDING 配置, 默认以 INCLUDING ALL 执行表复制!", sql);
            }
            if (StringUtils.isNotBlank(poll)) {
                throw new RuntimeException("未知的sql:" + sql + ", 解析 LIKE TABLE 不以 ) 结尾, 在 " + poll + " 附近");
            }
            if (!queue.isEmpty()) {
                throw new RuntimeException("未知的sql:" + sql + ", 解析 LIKE TABLE 不以 ) 结尾, 在 " + queue.poll() + " 附近");
            }
            return likeCopyTableName;
        } else if (StringUtils.equalsIgnoreCase(element, "(")) {
            queue.poll();
            String liKeWork = queue.element();
            if (StringUtils.equalsIgnoreCase(liKeWork, "LIKE")) {
                queue.poll();
                String copy = queue.element();
                // 判断是否是数据类型
                if (!DataTypeUtil.checkItemFieldTypeEnum(copy)) {
                    // 非数据类型，而是表
                    queue.poll();
                    String likeCopyTableName = removeQuoting(copy);
                    String poll = queue.poll();
                    boolean flag = false;
                    while (StringUtils.equalsIgnoreCase(poll, "INCLUDING")) {
                        if (!flag) {
                            flag = true;
                        }
                        queue.poll();
                        poll = queue.poll();
                    }
                    if (flag) {
                        log.warn("DAT SQL {} 将忽略 INCLUDING 配置, 默认以 INCLUDING ALL 执行表复制!", sql);
                    }
                    if (!StringUtils.equalsIgnoreCase(poll, ")")) {
                        throw new RuntimeException("未知的sql:" + sql + ", 解析 LIKE TABLE 缺少 ), 在 " + poll + " 附近");
                    }
                    if (!queue.isEmpty()) {
                        throw new RuntimeException("未知的sql:" + sql + ", 解析 LIKE TABLE 不以 ) 结尾, 在 " + queue.poll() + " 附近");
                    }
                    return likeCopyTableName;
                } else {
                    // 属于数据类型，把 LIKE 和 ( 加回队列头部
                    Deque<String> deque = (Deque) queue;
                    deque.addFirst(liKeWork);
                    deque.addFirst(element);
                }
            } else {
                // 非 Like 把 ( 加回去队列头
                Deque<String> deque = (Deque) queue;
                deque.addFirst(element);
            }
        }
        return null;
    }

    /**
     * 索引创建处理
     *
     * @param queue
     */
    private IndexParamCondition createIndexHandler(Queue<String> queue) {
        // key/index关键字
        String poll = queue.poll();

        boolean isUnique = false;

        if (StringUtils.equalsIgnoreCase(poll, "unique")) {
            isUnique = true;
            queue.poll();
        }

        String work = queue.element();

        String indexName = null;

        if (!StringUtils.equalsIgnoreCase(work, "(")) {
            // 带索引别名
            queue.poll();
            indexName = removeQuoting(work);
            work = queue.element();
            // 判断是否衔接 (
            if (!StringUtils.equalsIgnoreCase(work, "(")) {
                throw new RuntimeException("未知的sql:" + sql + ", 解析索引缺少(, 在" + work + "附近");
            }
        }

        List<IndexParamCondition.IndexColumn> indexColumns = parserIndexColumns(queue);

        IndexParamCondition.IndexParamConditionBuilder builder = IndexParamCondition.builder();

        if (isUnique) {
            builder.indexType(IndexType.UNIQUE);
        }

        if (StringUtils.isNotBlank(indexName)) {
            builder.indexName(indexName);
        }

        builder.isNotExists(isNotExists);

        return builder.columns(indexColumns).build();
    }

    /**
     * 解析单索引字段
     *
     * @param queue
     * @return
     */
    private List<IndexParamCondition.IndexColumn> parserIndexColumns(Queue<String> queue) {
        // 索引字段
        List<IndexParamCondition.IndexColumn> indexColumns = new ArrayList<>();

        while (!StringUtils.equalsIgnoreCase(queue.element(), ")")) {

            if (!StringUtils.equalsAnyIgnoreCase(queue.element(), "(", ",")) {
                throw new RuntimeException("未知的sql:" + sql + ", 解析索引, 无法识别关键字[" + queue.element() + "], " +
                        "支持关键字['(', ',']");
            }

            queue.poll();
            String column;
            String sort = null;
            String work = queue.poll();
            column = removeQuoting(work);

            // 判断是否带长度
            work = queue.element();
            if (StringUtils.equalsIgnoreCase(work, "(")) {
                // 带长度
                throw new RuntimeException("create table sql 暂不支持索引指定长度:" + sql);
            }

            if (StringUtils.equalsIgnoreCase(work, "asc") || StringUtils.equalsIgnoreCase(work, "desc")) {
                // 携带索引创建顺序
                queue.poll();
                sort = work;
            }

            IndexParamCondition.IndexColumn.IndexColumnBuilder indexColumnBuilder = IndexParamCondition.IndexColumn.builder().column(column);

            if (StringUtils.isNotBlank(sort)) {
                indexColumnBuilder.sort(Sort.parse(sort));
            }

            indexColumns.add(indexColumnBuilder.build());
        }

        queue.poll();

        return indexColumns;
    }

    //约束处理
    private void constraintHandler(Queue<String> queue, LinkedHashMap<String, DatColumn> columns) {
        //constraint关键字
        queue.poll();
        //约束名称
        String word = queue.poll();
        word = queue.element();
        if (word.equalsIgnoreCase("primary")) {
            //主键处理
            setPrimaryKey(queue, columns);
        } else {
            throw new RuntimeException("未知的sql:" + sql + ", 解析约束处理, 无法识别关键字[" + word + "], " +
                    "支持关键字[PRIMARY]");
        }

    }

    private void setPrimaryKey(Queue<String> queue, LinkedHashMap<String, DatColumn> columns) {


        String primary = queue.poll();
        String key = queue.poll();
        if (!(primary.equalsIgnoreCase("primary")
                && key.equalsIgnoreCase("key"))) {
            throw new RuntimeException("未知的sql:" + sql + ", 解析主键, 无法识别关键字[" + primary + " " + key + "], " +
                    "支持关键字[PRIMARY KEY]");
        }

        String word = queue.poll();
        if (!word.equalsIgnoreCase("(")) {
            throw new RuntimeException("未知的sql:" + sql + ", 解析主键缺少(, 在" + word + "附近");
        }

        while (true) {
            String primaryKey = removeQuoting(queue.poll());

            DatColumn primaryKeyColumn = columns.get(primaryKey);

            if (primaryKeyColumn == null) {
                throw new RuntimeException("PRIMARY KEY 指定字段不存在: " + primaryKey);
            }

            primaryKeyColumn.primaryKey = true;

            if(queue.element().equalsIgnoreCase(",")) {
                queue.poll();
            }else {
                break;
            }
        }

        word = queue.poll();
        if (!word.equalsIgnoreCase(")")) {
            throw new RuntimeException("未知的sql:" + sql + ", 解析主键缺少), 在" + word + "附近");
        }

        while(true) {
            if(queue.element().equalsIgnoreCase("USING")
                    || queue.element().equalsIgnoreCase("BTREE")) {
                queue.poll();
            }else {
                break;
            }
        }

        if (!isColumnEnd(queue.element())) {
            throw new RuntimeException("未知的sql:" + sql + ", 未知结束符[" + queue.element() + "]");
        }

    }

    private String getTableName(Queue<String> queue) {

        String createWord = queue.poll();
        String tableWord = queue.poll();

        if (!createWord.equalsIgnoreCase("create")
                || !tableWord.equalsIgnoreCase("table")) {
            throw new RuntimeException("未知的sql:" + sql + ", 解析建表, 无法识别关键字[" + createWord + " " + tableWord + "], " +
                    "支持关键字[CREATE TABLE]");
        }

        String word = queue.poll();
        if(word.equalsIgnoreCase("if")) {
            String not = queue.poll();
            String exists = queue.poll();
            if(!not.equalsIgnoreCase("not")
                    || !exists.equalsIgnoreCase("exists")) {
                throw new RuntimeException("未知的sql:" + sql + ", 解析约束条件[IF NOT EXISTS], 无法识别" + not + " " + exists);
            }
            isNotExists = true;
            word = queue.poll();
        }

        String tableName = word;


        String[] names = tableName.split("\\.");

        if (names.length == 2) {
            tableName = names[1];
        } else {
            tableName = names[0];
        }

        return removeQuoting(tableName);
    }

}
