package com.meiya.whalex.db.util.param.impl.ani;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.map.MapBuilder;
import cn.hutool.core.map.MapUtil;
import cn.hutool.core.util.StrUtil;
import com.meiya.whalex.db.constant.IndexType;
import com.meiya.whalex.db.entity.AddParamCondition;
import com.meiya.whalex.db.entity.FieldTypeAdapter;
import com.meiya.whalex.db.entity.IndexParamCondition;
import com.meiya.whalex.db.entity.ani.*;
import com.meiya.whalex.exception.BusinessException;
import com.meiya.whalex.exception.ExceptionCode;
import com.meiya.whalex.interior.db.constant.ItemFieldTypeEnum;
import com.meiya.whalex.interior.db.constant.PartitionType;
import com.meiya.whalex.interior.db.operation.in.AlterTableParamCondition;
import com.meiya.whalex.interior.db.operation.in.CreateTableParamCondition;
import com.meiya.whalex.interior.db.operation.in.PartitionInfo;
import com.meiya.whalex.interior.db.operation.in.Point;
import com.meiya.whalex.interior.db.search.condition.Rel;
import com.meiya.whalex.interior.db.search.in.Where;
import com.meiya.whalex.util.JsonUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 关系型数据库通用，解析查询实体转换为SQL
 *
 * @author 黄河森
 * @date 2019/12/23
 * @project whale-cloud-platformX
 */
@Slf4j
public class DorisParserUtil extends BaseMySqlParserUtil {

    /**
     * 建表模板
     */
    protected final static String CREATE_TABLE_TEMPLATE = "CREATE TABLE `%s` ( %s ) ${TABLE_MODEL_INFO} ${PARTITION_INFO} ${DISTRIBUTED_BY}";

    /**
     * 建表LIST分区片段
     */
    protected final static String CREATE_TABLE_PARTITION_LIST = "PARTITION `%s` VALUES IN (%s)";

    /**
     * 建表RANGE分区片段
     */
    protected final static String CREATE_TABLE_PARTITION_RANGE = "PARTITION `%s` VALUES [(%s), (%s))";


    protected final static String CREATE_TABLE_PARTITION_RANGE_LESS = "PARTITION `%s` VALUES LESS THAN (%s)";

    /**
     * 新增分区
     */
    protected final static String ADD_PARTITION_TEMPLATE = "ALTER TABLE %s ADD %s";

    protected final static String DROP_PARTITION_TEMPLATE = "ALTER TABLE %s DROP PARTITION %s";

    protected final static String CREATE_TABLE_PARTITION_RANGE_LESS_MAX = "MAXVALUE";

    /**
     * 修改表名
     */
    protected final static String UPDATE_TABLE_NAME = "ALTER TABLE `%s` RENAME `%s`";

    /**
     * 删除表字段
     */
    protected final static String DROP_TABLE_FIELD = "ALTER TABLE `%s` DROP COLUMN `%s`";


    protected final static String CREATE_BLOOM_FILTER_INDEX_TEMPLATE = "ALTER TABLE %s SET(\"bloom_filter_columns\" = \"%s\")";

    protected final static String CREATE_DORIS_INDEX_TEMPLATE = "CREATE INDEX %s ON %s(%s) USING %s";

    @Override
    protected String getDropTableFieldSQL() {
        return DROP_TABLE_FIELD;
    }

    @Override
    protected String getUpdateTableNameSQL() {
        return UPDATE_TABLE_NAME;
    }

    @Override
    protected String getUpdateTableFieldNameSQL() {
        throw new BusinessException("doris 不支持修改列名!");
    }

    /**
     * 解析 数据插入语句
     *
     * @param addParamCondition
     * @return
     */
    @Override
    public AniHandler.AniInsert parserInsertSql(AddParamCondition addParamCondition, BaseMySqlDatabaseInfo baseMySqlDatabaseInfo, BaseMySqlTableInfo baseMySqlTableInfo) {

        DorisDatabaseInfo dorisDatabaseInfo = (DorisDatabaseInfo) baseMySqlDatabaseInfo;

        List<DorisDatabaseInfo.IpPort> ipPorts = dorisDatabaseInfo.getIpPorts();
        DorisDatabaseInfo.IpPort ipPort = ipPorts.get(0);
        String fePort = ipPort.getFePort();
        if (StrUtil.isBlank(fePort)) {
            // 没有配置 fe http 端口，则走普通sql入库
            return super.parserInsertSql(addParamCondition, baseMySqlDatabaseInfo, baseMySqlTableInfo);
        }

        AniHandler.AniInsert aniInsert = new AniHandler.AniInsert();
        // 入库数据
        List<Map<String, Object>> fieldValueList = addParamCondition.getFieldValueList();
        if (fieldValueList != null && fieldValueList.size() > 0) {
            aniInsert.setSql(JsonUtil.objectToStr(fieldValueList));
        }
        return aniInsert;
    }

    @Override
    public AniHandler.AniCreateIndex parserCreateIndexSql(IndexParamCondition indexParamCondition, BaseMySqlDatabaseInfo databaseInfo, BaseMySqlTableInfo tableInfo) {
        IndexType indexType = indexParamCondition.getIndexType();
        if(indexType == null) {
            indexType = IndexType.INVERTED;
        }
        if (indexType.equals(IndexType.UNIQUE)) {
            log.warn("Doris 不支持创建 unique index, 将忽略当前参数!");
            throw new RuntimeException("Doris 不支持创建 unique index");
        }

        //BLOOM_FILTER索引
        if(indexType.equals(IndexType.BLOOM_FILTER)) {
            StringBuilder columnBuilder = new StringBuilder();
            List<IndexParamCondition.IndexColumn> columns = indexParamCondition.getColumns();
            if (CollectionUtil.isNotEmpty(columns)) {
                for (IndexParamCondition.IndexColumn column : columns) {
                    columnBuilder.append(column.getColumn()).append(",");
                }
                columnBuilder.deleteCharAt(columnBuilder.length() - 1);
            }
            if(columnBuilder.length() == 0) {
                String columnName = indexParamCondition.getColumn();
                if(StringUtils.isNotBlank(columnName)) {
                    columnBuilder.append(columnName);
                }
            }
            AniHandler.AniCreateIndex aniCreateIndex = new AniHandler.AniCreateIndex();
            Boolean isNotExists = indexParamCondition.getIsNotExists();
            if (isNotExists != null && isNotExists) {
                aniCreateIndex.setNotExists(true);
            } else {
                aniCreateIndex.setNotExists(false);
            }
            String sql = String.format(CREATE_BLOOM_FILTER_INDEX_TEMPLATE, tableInfo.getTableName(), columnBuilder.toString());
            aniCreateIndex.setSql(sql);
            return aniCreateIndex;
        }

        //索引字段
        String columnName = null;
        List<IndexParamCondition.IndexColumn> columns = indexParamCondition.getColumns();
        if (CollectionUtil.isNotEmpty(columns)) {

            if (columns.size() > 1) {
                throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "doris 不支持创建组合索引!");
            }

            IndexParamCondition.IndexColumn indexColumn = columns.get(0);
            columnName = indexColumn.getColumn();
        }
        if(columnName == null) {
            columnName = indexParamCondition.getColumn();
        }

        //索引名称
        String indexName = indexParamCondition.getIndexName();
        if (StringUtils.isBlank(indexName)) {
            indexName = "index_" + tableInfo.getTableName() + "_" + columnName;
        }

        String indexTypeName = "INVERTED";
        switch (indexType) {
            case INVERTED:
                indexTypeName = "INVERTED";
                break;
            case NGRAM_BLOOM_FILTER:
                indexTypeName = "NGRAM_BF";
        }

        String sql = String.format(CREATE_DORIS_INDEX_TEMPLATE, indexName, tableInfo.getTableName(), columnName, indexTypeName);
        Map<String, String> properties = indexParamCondition.getProperties();
        if(CollectionUtil.isNotEmpty(properties)) {
            StringBuilder propertiesBuilder = new StringBuilder();
            for (Map.Entry<String, String> entry : properties.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                propertiesBuilder.append("\"").append(key).append("\"").append(" = ").append("\"").append(value).append("\"").append(",");
            }
            propertiesBuilder.deleteCharAt(propertiesBuilder.length() - 1);
            sql = sql + " PROPERTIES(" + propertiesBuilder.toString() + ")";
        }
        //如果是倒排索引，需要对存量数据手动建倒排索引
        if(indexType.equals(IndexType.INVERTED)) {
            String buildIndexSql = String.format("BUILD INDEX %s ON %s", indexName, tableInfo.getTableName());
            sql = sql + ";" + buildIndexSql;
        }
        Boolean isNotExists = indexParamCondition.getIsNotExists();
        AniHandler.AniCreateIndex aniCreateIndex = new AniHandler.AniCreateIndex();
        aniCreateIndex.setSql(sql);

        if (isNotExists != null && isNotExists) {
            aniCreateIndex.setNotExists(true);
        } else {
            aniCreateIndex.setNotExists(false);
        }

        aniCreateIndex.setIndexName(indexName);

        return aniCreateIndex;

    }

    /**
     * 获取表模型
     * @param createTableParamCondition
     * @return
     */
    private CreateTableParamCondition.TableModelParam getTableModelParam(CreateTableParamCondition createTableParamCondition) {
        CreateTableParamCondition.TableModelParam tableModelParam = createTableParamCondition.getTableModelParam();
        if(tableModelParam != null) {
            return tableModelParam;
        }
        List<CreateTableParamCondition.CreateTableFieldParam> createTableFieldParamList = createTableParamCondition.getCreateTableFieldParamList();
        List<String> fieldList = new ArrayList<>();
        for (CreateTableParamCondition.CreateTableFieldParam createTableFieldParam : createTableFieldParamList) {
            if(createTableFieldParam.isPrimaryKey()) {
                fieldList.add(createTableFieldParam.getFieldName());
            }
        }
        if(fieldList.isEmpty()) {
            return null;
        }
        tableModelParam = new CreateTableParamCondition.TableModelParam();
        tableModelParam.setFieldList(fieldList);
        tableModelParam.setTableModel(CreateTableParamCondition.TableModel.PRIMARY_KEY);
        return tableModelParam;
    }

    private CreateTableParamCondition.DistributedParam getDistributedParam(CreateTableParamCondition createTableParamCondition) {
        CreateTableParamCondition.DistributedParam distributedParam = createTableParamCondition.getDistributedParam();
        if(distributedParam != null) {
            return distributedParam;
        }
        List<CreateTableParamCondition.CreateTableFieldParam> createTableFieldParamList = createTableParamCondition.getCreateTableFieldParamList();
        List<String> fieldList = new ArrayList<>();
        for (CreateTableParamCondition.CreateTableFieldParam createTableFieldParam : createTableFieldParamList) {
            if(createTableFieldParam.isDistributed()) {
                fieldList.add(createTableFieldParam.getFieldName());
            }
        }
        if(fieldList.isEmpty()) {
            return null;
        }
        distributedParam = new CreateTableParamCondition.DistributedParam();
        distributedParam.setFieldList(fieldList);
        distributedParam.setDistributedType(CreateTableParamCondition.DistributedType.HASH);
        return distributedParam;
    }
    
    @Override
    public AniHandler.AniCreateTable parserCreateTableSql(CreateTableParamCondition createTableParamCondition, BaseMySqlDatabaseInfo baseMySqlDatabaseInfo, BaseMySqlTableInfo baseMySqlTableInfo) {
        DorisTableInfo dorisTableInfo = (DorisTableInfo) baseMySqlTableInfo;
        AniHandler.AniCreateTable createTable = new AniHandler.AniCreateTable();
        List<CreateTableParamCondition.CreateTableFieldParam> createTableFieldParamList = createTableParamCondition.getCreateTableFieldParamList();
        StringBuilder createSqlBuilder = new StringBuilder();
        List<PartitionInfo> partitionInfos = new ArrayList<>();
        CreateTableParamCondition.TableModelParam tableModelParam = getTableModelParam(createTableParamCondition);

        // 解析字段
        parseColumns(createTableFieldParamList, partitionInfos, tableModelParam, createSqlBuilder);
        String createTableTemplate = CREATE_TABLE_TEMPLATE;

        // 分布键解析
        CreateTableParamCondition.DistributedParam distributedParam = getDistributedParam(createTableParamCondition);
        //模型解析
        if(tableModelParam != null) {
            CreateTableParamCondition.TableModel tableModel = tableModelParam.getTableModel();
            StringBuilder tableModeBuilder = new StringBuilder();
            switch (tableModel) {
                case PRIMARY_KEY:
                case UNIQUE_KEY:
                    tableModeBuilder.append("UNIQUE KEY");
                    if(distributedParam == null) {
                        distributedParam = new CreateTableParamCondition.DistributedParam();
                        distributedParam.setFieldList(Arrays.asList(tableModelParam.getFieldList().get(0)));
                    }
                    break;
                case AGGREGATE_KEY:
                    tableModeBuilder.append("AGGREGATE KEY");
                    break;
                case DUPLICATE_KEY:
                    tableModeBuilder.append("DUPLICATE KEY");
                    break;
            }
            tableModeBuilder.append("(");
            List<String> fieldList = tableModelParam.getFieldList();
            for (String field : fieldList) {
                tableModeBuilder.append(field).append(",");
            }
            tableModeBuilder.deleteCharAt(tableModeBuilder.length() - 1);
            tableModeBuilder.append(")");
            createTableTemplate = StringUtils.replace(createTableTemplate, "${TABLE_MODEL_INFO}", tableModeBuilder.toString());
        }else {
            createTableTemplate = StringUtils.replace(createTableTemplate, "${TABLE_MODEL_INFO}", "");
        }

        // 分区键解析
        if (CollectionUtil.isNotEmpty(partitionInfos)) {
            if (partitionInfos.size() > 1) {
                throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "Doris仅支持一级分区!");
            }
            PartitionInfo partitionInfo = partitionInfos.get(0);
            StringBuilder partitionSb = new StringBuilder();
            boolean autoPartition = partitionInfo.isAutoPartition();
            if(autoPartition) {
                partitionSb.append("AUTO ");
            }
            String partitionObj = partitionInfo.getPartitionFormat();
            if(StringUtils.isBlank(partitionObj)) {
                partitionObj = DOUBLE_QUOTATION_MARKS + partitionInfo.getPartitionField() + DOUBLE_QUOTATION_MARKS;
            }
            partitionSb.append("PARTITION BY ");
            partitionSb.append(partitionInfo.getType().name()).append("(").append(partitionObj).append(")").append("(");
            List<String> parserPartitionSql = parserPartitionSql(partitionInfo);
            // 手动分区有具体的分区信息，动态分区时，没有具体的分区信息
            if(CollectionUtils.isNotEmpty(parserPartitionSql)) {
                for (String sql : parserPartitionSql) {
                    partitionSb.append(sql).append(",");
                }
                partitionSb.deleteCharAt(partitionSb.length() - 1);
            }
            partitionSb.append(")");
            createTableTemplate = StringUtils.replace(createTableTemplate, "${PARTITION_INFO}", partitionSb.toString());
        } else {
            createTableTemplate = StringUtils.replace(createTableTemplate, "${PARTITION_INFO}", "");
        }


        // 分布键解析
        if (distributedParam != null) {
            List<String> fieldList = distributedParam.getFieldList();
            StringBuilder sb = new StringBuilder("DISTRIBUTED BY HASH(");
            for (String distribute : fieldList) {
                sb.append(distribute).append(",");
            }
            sb.deleteCharAt(sb.length() - 1);
            sb.append(")");
            // 数据分桶
            Integer bucketsNum = dorisTableInfo.getBucketsNum();
            if(bucketsNum != null) {
                sb.append(" BUCKETS ").append(bucketsNum);
            }
            createTableTemplate = StringUtils.replace(createTableTemplate, "${DISTRIBUTED_BY}", sb.toString());
        } else {
            createTableTemplate = StringUtils.replace(createTableTemplate, "${DISTRIBUTED_BY}", "");
        }

        // 表属性解析
        Map<String, Object> properties = parserProperties(dorisTableInfo);
        if(CollectionUtil.isNotEmpty(properties)) {
            StringBuilder propertiesBuilder = new StringBuilder();
            propertiesBuilder.append("PROPERTIES").append("(");
            Set<String> keySet = properties.keySet();
            for (String key : keySet) {
                propertiesBuilder.append("\"").append(key).append("\"")
                        .append(" = ")
                        .append("\"").append(properties.get(key)).append("\"")
                        .append(",");
            }
            propertiesBuilder.replace(propertiesBuilder.length() - 1, propertiesBuilder.length(), "");
            propertiesBuilder.append(")");
            createTableTemplate = createTableTemplate + " " + propertiesBuilder.toString();
        }

        String sql = String.format(createTableTemplate, baseMySqlTableInfo.getTableName(), createSqlBuilder.toString());
        if(log.isDebugEnabled()) {
            log.debug("doris建表sql: " + sql);
        }
        createTable.setSql(sql);
        return createTable;
    }

    /**
     * 解析表配置参数
     *
     * @param tableInfo
     * @return
     */
    private Map<String, Object> parserProperties(DorisTableInfo tableInfo) {
        MapBuilder<String, Object> builder = MapUtil.builder(new HashMap<>());
        Field[] declaredFields = DorisTableInfo.class.getDeclaredFields();
        for (Field declaredField : declaredFields) {
            declaredField.setAccessible(true);
            Object value = null;
            try {
                value = declaredField.get(tableInfo);
            } catch (IllegalAccessException e) {
                throw new BusinessException(e);
            }
            if (value == null || StrUtil.isBlankIfStr(value)) {
                continue;
            }
            String name = declaredField.getName();
            boolean isStatic = Modifier.isStatic(declaredField.getModifiers());
            if (isStatic) {
                continue;
            }
            if (StrUtil.equalsIgnoreCase(name, "storageCoolDownTime")) {
                name = "storage_cooldown_time";
            } else if (StrUtil.equalsAnyIgnoreCase(name, "sequenceCol", "sequenceType")) {
                name = "function_column." + StrUtil.toUnderlineCase(name);
            } else if (StrUtil.startWith(name, "dynamicPartition")) {
                name = StrUtil.subAfter(name, "dynamicPartition", true);
                name = "dynamic_partition." + StrUtil.toUnderlineCase(name);
            } else if (StrUtil.equalsIgnoreCase(name, "groupCommit")) {
                continue;
            }else if (StrUtil.equalsIgnoreCase(name, "bucketsNum")) {
                continue;
            } else {
                name = StrUtil.toUnderlineCase(name);
            }
            builder.put(name, value);
        }
        return builder.build();
    }

    protected void parseColumns(List<CreateTableParamCondition.CreateTableFieldParam> createTableFieldParamList, List<PartitionInfo> partitionInfos, CreateTableParamCondition.TableModelParam tableModelParam, StringBuilder createSqlBuilder) {

        //获取表模型
        CreateTableParamCondition.TableModel tableModel = null;
        Map<String, CreateTableParamCondition.AggTableModelField> aggFieldMap = new HashMap<>();
        if(tableModelParam != null) {
            tableModel = tableModelParam.getTableModel();
            if(tableModel.equals(CreateTableParamCondition.TableModel.AGGREGATE_KEY)) {
                List<CreateTableParamCondition.AggTableModelField> aggFieldList = tableModelParam.getAggFieldList();
                if(CollectionUtils.isEmpty(aggFieldList)) {
                    throw new RuntimeException("doris聚合模型，聚合字段不能为空");
                }
                aggFieldList.forEach(item->{aggFieldMap.put(item.getFieldName(), item);});
            }
            //关键字段必须放在建表的第一个字段
            List<String> keys = new ArrayList<>();
            List<String> fieldList = tableModelParam.getFieldList();
            if(CollectionUtil.isNotEmpty(fieldList)) {
                keys.addAll(fieldList);
            }
            List<CreateTableParamCondition.AggTableModelField> aggFieldList = tableModelParam.getAggFieldList();
            if(CollectionUtil.isNotEmpty(aggFieldList)) {
                aggFieldList.forEach(item->{
                    keys.add(item.getFieldName());
                });
            }
            List<CreateTableParamCondition.CreateTableFieldParam> primaryKeyCreateTableFieldParams = new ArrayList<>();
            List<CreateTableParamCondition.CreateTableFieldParam> createTableFieldParams = new ArrayList<>();
            for (String key : keys) {
                for (CreateTableParamCondition.CreateTableFieldParam createTableFieldParam : createTableFieldParamList) {
                    if(key.equals(createTableFieldParam.getFieldName())) {
                        primaryKeyCreateTableFieldParams.add(createTableFieldParam);
                        break;
                    }
                }
            }
            createTableFieldParamList.forEach(createTableFieldParam -> {
                if(!keys.contains(createTableFieldParam.getFieldName())) {
                    createTableFieldParams.add(createTableFieldParam);
                }
            });
            //重新排列的字段
            List<CreateTableParamCondition.CreateTableFieldParam> orderList = new ArrayList<>();
            orderList.addAll(primaryKeyCreateTableFieldParams);
            orderList.addAll(createTableFieldParams);
            createTableFieldParamList = orderList;
        }

        CreateTableParamCondition.TableModel finalTableModel = tableModel;
        createTableFieldParamList.forEach(createTableFieldParam -> {
            String fieldType = createTableFieldParam.getFieldType();
            String fieldComment = createTableFieldParam.getFieldComment();
            Integer fieldLength = createTableFieldParam.getFieldLength();
            String fieldName = createTableFieldParam.getFieldName();
            Integer fieldDecimalPoint = createTableFieldParam.getFieldDecimalPoint();
            FieldTypeAdapter adapter = getAdapter(fieldType);
            createSqlBuilder.append(DOUBLE_QUOTATION_MARKS)
                    .append(fieldName)
                    .append(DOUBLE_QUOTATION_MARKS + " ")
                    .append(buildSqlDataType(adapter, fieldLength, fieldDecimalPoint));

            if (adapter.isUnsigned()) {
                if (createTableFieldParam.getUnsigned() != null && createTableFieldParam.getUnsigned()) {
                    createSqlBuilder.append(" UNSIGNED");
                }
            }

            if (createTableFieldParam.isNotNull()) {
                createSqlBuilder.append(" NOT NULL");
            }

            if (createTableFieldParam.isAutoIncrement()) {
                createSqlBuilder.append(" AUTO_INCREMENT");
                Long startIncrement = createTableFieldParam.getStartIncrement();
                if(startIncrement > 1) {
                    createSqlBuilder.append("(").append(startIncrement).append(")");
                }
            }

            //如果是聚合模型，添加聚合函数
            if(CreateTableParamCondition.TableModel.AGGREGATE_KEY.equals(finalTableModel)) {
                CreateTableParamCondition.AggTableModelField aggTableModelField = aggFieldMap.get(fieldName);
                if(aggTableModelField != null) {
                    String funcName = aggTableModelField.getFuncName();
                    createSqlBuilder.append(" ").append(funcName);
                }
            }

            String defaultValue = createTableFieldParam.getDefaultValue();
            if (defaultValue != null) {
                createSqlBuilder.append(" ").append(buildDefaultValue(fieldType, defaultValue));
            }

            if (StringUtils.isNotBlank(fieldComment)) {
                createSqlBuilder.append(" COMMENT ")
                        .append("'")
                        .append(fieldComment)
                        .append("'");
            }
            createSqlBuilder.append(",");

            // 分区键
            if(createTableFieldParam.isPartition()) {
                PartitionInfo partitionInfo = createTableFieldParam.getPartitionInfo();
                if(partitionInfo == null) {
                    throw new RuntimeException("分区表信息不能空");
                }
                partitionInfo.setPartitionField(createTableFieldParam.getFieldName());
                partitionInfos.add(partitionInfo);
            }
        });
        createSqlBuilder.deleteCharAt(createSqlBuilder.length() - 1);
    }


    protected List<String> parserPartitionSql(PartitionInfo partitionInfo) {

        List<String> list = new ArrayList<>();
        PartitionType type = partitionInfo.getType();

        if(PartitionType.LIST.equals(type)) {
            List<PartitionInfo.PartitionList> in = partitionInfo.getIn();
            if(CollectionUtil.isEmpty(in)) {
                return list;
            }
            for (PartitionInfo.PartitionList partition : in) {
                String partitionName = partition.getPartitionName();
                if(StringUtils.isBlank(partitionName)) {
                    throw new RuntimeException("分区表名称不能为空");
                }
                List<String> inParams = partition.getList();
                if(CollectionUtil.isEmpty(inParams)) {
                    throw new RuntimeException("LIST类型分区表，参数不能为空");
                }

                StringBuilder sb = new StringBuilder();
                sb.append("\"").append(inParams.get(0)).append("\"");
                for (int i = 1; i < inParams.size(); i++) {
                    sb.append(", \"").append(inParams.get(i)).append("\"");
                }
                String sql = String.format(CREATE_TABLE_PARTITION_LIST, partitionName, sb.toString());
                list.add(sql);

            }
            return list;
        }

        if(PartitionType.RANGE.equals(type)) {
            List<PartitionInfo.PartitionRange> range = partitionInfo.getRange();
            if(CollectionUtil.isEmpty(range)) {
                return list;
            }

            for (PartitionInfo.PartitionRange partition : range) {
                String partitionName = partition.getPartitionName();
                if(StringUtils.isBlank(partitionName)) {
                    throw new RuntimeException("分区表名称不能为空");
                }
                String left = partition.getLeft();
                String right = partition.getRight();
                left = StringUtils.isBlank(left) ? null : "'" + left + "'";
                right = StringUtils.isBlank(right) ? null : "'" + right + "'";
                String sql;
                if (StringUtils.isNotBlank(left) && StringUtils.isNotBlank(right)) {
                    sql = String.format(CREATE_TABLE_PARTITION_RANGE, partitionName, left, right);
                    list.add(sql);
                } else if (StringUtils.isNotBlank(left)) {
                    sql = String.format(CREATE_TABLE_PARTITION_RANGE_LESS, partitionName + "__MIN", left);
                    list.add(sql);
                    sql = String.format(CREATE_TABLE_PARTITION_RANGE_LESS, partitionName + "__MAX", CREATE_TABLE_PARTITION_RANGE_LESS_MAX);
                    list.add(sql);
                } else {
                    sql = String.format(CREATE_TABLE_PARTITION_RANGE_LESS, partitionName, right);
                    list.add(sql);
                }
            }
            return list;
        }

        throw new RuntimeException("未知的分区表类型[" + type + "]");
    }

    @Override
    protected String buildSqlDataType(FieldTypeAdapter adapter, Integer fieldLength, Integer fieldDecimalPoint) {
        StringBuilder columnSb = new StringBuilder(adapter.getDbFieldType());

        // 判断是否需要长度
        if (!ItemFieldTypeEnum.ParamStatus.NO.equals(adapter.getNeedDataLength())) {
            // 如果当前没有传入长度，并且必须需要长度，则取默认值
            if (fieldLength == null && ItemFieldTypeEnum.ParamStatus.MUST.equals(adapter.getNeedDataLength())) {
                fieldLength = adapter.getFiledLength();
            }
            if (fieldLength != null) {

                // 数组类型
                if (StringUtils.startsWithIgnoreCase(adapter.getDbFieldType(), "array<")) {
                    columnSb.insert(columnSb.length() - 1, "(")
                            .insert(columnSb.length() - 1, fieldLength)
                            .insert(columnSb.length() - 1, ")");
                } else {
                    columnSb.append("(")
                            .append(fieldLength)
                            .append(")");
                }

                // 是否需要标度
                if (!ItemFieldTypeEnum.ParamStatus.NO.equals(adapter.getNeedDataDecimalPoint())) {
                    if (fieldDecimalPoint == null && ItemFieldTypeEnum.ParamStatus.MUST.equals(adapter.getNeedDataDecimalPoint())) {
                        fieldDecimalPoint = adapter.getFieldDecimalPoint();
                    }
                    if (fieldDecimalPoint != null) {
                        if (StringUtils.startsWithIgnoreCase(adapter.getDbFieldType(), "array<")) {
                            columnSb.insert(columnSb.length() - 2, ",")
                                    .insert(columnSb.length() - 2, fieldDecimalPoint);
                        } else {
                            columnSb.delete(columnSb.length() - 1, columnSb.length())
                                    .append(",")
                                    .append(fieldDecimalPoint)
                                    .append(")");
                        }
                    }
                }
            }
        }
        return columnSb.toString();
    }

    @Override
    protected String buildDefaultValue(String fieldType, String value) {
        if(value.equalsIgnoreCase("null")) return "DEFAULT NULL";
        return "DEFAULT '" + value + "'";
    }

    @Override
    protected FieldTypeAdapter getAdapter(String fieldType) {
        return DorisFieldTypeEnum.findFieldTypeEnum(fieldType);
    }

    @Override
    public AniHandler.AniAlterTable parserAlterTableSql(AlterTableParamCondition alterTableParamCondition, BaseMySqlDatabaseInfo baseDatabaseInfo, BaseMySqlTableInfo baseTableInfo) {
        AniHandler.AniAlterTable aniAlterTable = super.parserAlterTableSql(alterTableParamCondition, baseDatabaseInfo, baseTableInfo);

        List<String> sqlList = aniAlterTable.getSqlList();

        if (sqlList == null) {
            sqlList = new ArrayList<>();
        }

        PartitionInfo addPartition = alterTableParamCondition.getAddPartition();

        PartitionInfo delPartition = alterTableParamCondition.getDelPartition();

        //添加分区
        if(addPartition != null) {
            List<String> addPartitionSqlList = buildAddPartitionTable(addPartition, baseDatabaseInfo, baseTableInfo);
            if (addPartitionSqlList != null) {
                sqlList.addAll(addPartitionSqlList);
            }
        }

        //删除分区
        if(delPartition != null) {
            sqlList.addAll(buildDelPartitionTable(delPartition, baseDatabaseInfo, baseTableInfo));
        }

        if (CollectionUtil.isNotEmpty(sqlList)) {
            aniAlterTable.setSqlList(sqlList);
        }
        return aniAlterTable;
    }

    private List<String> buildAddPartitionTable(PartitionInfo addPartition, BaseMySqlDatabaseInfo baseDatabaseInfo, BaseMySqlTableInfo baseTableInfo) {
        List<String> sqls = parserPartitionSql(addPartition);
        if (CollectionUtil.isNotEmpty(sqls)) {
            sqls = sqls.stream().map(sql -> String.format(ADD_PARTITION_TEMPLATE, baseTableInfo.getTableName(), sql)).collect(Collectors.toList());
        }
        return sqls;
    }

    private List<String> buildDelPartitionTable(PartitionInfo delPartition, BaseMySqlDatabaseInfo baseDatabaseInfo, BaseMySqlTableInfo baseTableInfo) {
        List<String> sqlList = new ArrayList<>();
        List<PartitionInfo.PartitionRange> range = delPartition.getRange();
        List<PartitionInfo.PartitionList> in = delPartition.getIn();
        if (CollectionUtil.isNotEmpty(range)) {
            for (PartitionInfo.PartitionRange partitionRange : range) {
                String partitionName = partitionRange.getPartitionName();
                sqlList.add(String.format(DROP_PARTITION_TEMPLATE, baseTableInfo.getTableName(), partitionName));
            }
        } else if (CollectionUtil.isNotEmpty(in)) {
            for (PartitionInfo.PartitionList partitionList : in) {
                String partitionName = partitionList.getPartitionName();
                sqlList.add(String.format(DROP_PARTITION_TEMPLATE, baseTableInfo.getTableName(), partitionName));
            }
        }
        return sqlList;
    }

    @Override
    protected String relStrHandler(Where where, List<Object> paramList, List<Map<String, Object>> geoDistanceList, String tableName) {
        Rel rel = where.getType();
        StringBuilder sb = new StringBuilder();
        if (StringUtils.isNotBlank(tableName)) {
            sb.append(tableName);
        }
        String field = where.getField();
        String[] split = field.split("\\.");
        if(split.length > 1) {
            StringBuilder fieldBuilder = new StringBuilder();
            fieldBuilder.append(split[0]);
            for (int i = 1; i < split.length; i++) {
                fieldBuilder.append("['").append(split[i]).append("']");
            }
            sb.append(fieldBuilder.toString());
        }else {
            sb.append(field);
        }

        switch (rel) {
            case EQ:
            case TERM:
                sb.append(" = ");
                Object value = where.getParam();
                if (value instanceof Point) {
                    Point point = (Point) value;
                    sb.append(String.format(BaseMySqlParserUtil.POINT_TEMPLATE, point.getLon(), point.getLat()));
                } else {
                    sb.append("?");
                    paramList.add(where.getParam());
                }
                break;
            case NE:
                sb.append(" != ");
                sb.append("?");
                paramList.add(where.getParam());
                break;
            case GT:
                sb.append(" > ");
                sb.append("?");
                paramList.add(where.getParam());
                break;
            case GTE:
                sb.append(" >= ");
                sb.append("?");
                paramList.add(where.getParam());
                break;
            case LT:
                sb.append(" < ");
                sb.append("?");
                paramList.add(where.getParam());
                break;
            case LTE:
                sb.append(" <= ");
                sb.append("?");
                paramList.add(where.getParam());
                break;
            case IN:
                sb.append(" in ");
                List param = (List) where.getParam();
                sb.append(" ( ");
                for (Object o : param) {
                    sb.append("?");
                    paramList.add(o);
                    sb.append(",");
                }
                sb.deleteCharAt(sb.length() - 1);
                sb.append(" ) ");
                break;
            case NIN:
                sb.append(" not in ");
                List paramNin = (List) where.getParam();
                sb.append(" ( ");
                for (Object o : paramNin) {
                    sb.append("?");
                    paramList.add(o);
                    sb.append(",");
                }
                sb.deleteCharAt(sb.length() - 1);
                sb.append(" ) ");
                break;
            case NULL:
                sb.append(" is null ");
                break;
            case NOT_NULL:
                sb.append(" is not null ");
                break;
            case NOT_MIDDLE_LIKE:
                sb.append(" not like ");
                sb.append("?");
                paramList.add("%" + where.getParam() + "%");
                break;
            case LIKE:
                sb.append(" like ");
                sb.append("?");
                paramList.add("%" + where.getParam() + "%");
                break;
            case ONLY_LIKE:
                sb.append(" like ");
                sb.append("?");
                paramList.add(where.getParam());
                break;
            case FRONT_LIKE:
                sb.append(" like ");
                sb.append(" ? ");
                paramList.add("%" + where.getParam());
                break;
            case TAIL_LIKE:
                sb.append(" like ");
                sb.append(" ? ");
                paramList.add(where.getParam() + "%");
                break;
            case ILIKE:
            case MIDDLE_LIKE:
                sb.append(" like ");
                sb.append(" ? ");
                String sqlParam = (String) where.getParam();
                if (sqlParam.startsWith("*") && sqlParam.endsWith("*")) {
                    sqlParam = "%" + sqlParam.substring(1, sqlParam.length() - 1) + "%";
                }
                if (!sqlParam.startsWith("%") && !sqlParam.endsWith("%")) {
                    sqlParam = "%" + sqlParam + "%";
                }
                paramList.add(sqlParam);
                break;
            case GEO_DISTANCE:
                /** 地理位置查询, 圆形,对应的paramValue是Map对象，至少有两个参数，
                 * distance;//eg:distance:10km 半径
                 * loc;loc:"23.04805756,113.27598616"//圆点的中心值
                 * 如果不带单位，默认为m
                 */
                geoDistanceQuery(where, geoDistanceList, sb);
                break;
            case MATCH:
                sb.append(" match_any ");
                sb.append("?");
                paramList.add(where.getParam());
                break;
            case MATCH_ALL:
                sb.append(" match_all ");
                sb.append("?");
                paramList.add(where.getParam());
                break;
            case MATCH_PHRASE:
                sb.append(" match_phrase ");
                sb.append("?");
                paramList.add(where.getParam());
                break;
            case REGEX:
                sb.append(" match_regexp ");
                sb.append("?");
                paramList.add(where.getParam());
                break;
            case EXISTS:
                sb.append(" is not null");
                break;
            default:
                sb.append(" = ");
                break;
        }
        return sb.toString();
    }

    @Override
    protected String buildWhereSql(List<Where> wheres, Rel rel, List<Object> paramList, List<Map<String, Object>> geoDistanceList, String tableName) {
        StringBuilder sb = new StringBuilder();
        if (wheres != null && !wheres.isEmpty()) {
            for (Where where : wheres) {
                if (Rel.AND.equals(where.getType()) || Rel.OR.equals(where.getType()) || Rel.NOT.equals(where.getType())) {
                    List<Where> params = where.getParams();
                    String sql = buildWhereSql(params, where.getType(), paramList, geoDistanceList, tableName);
                    StringBuilder childSb = new StringBuilder();
                    if (sql.length() > 0) {
                        childSb.append(" (");
                        sql = sql.substring(0, sql.length() - 2 - where.getType().getName().length());
                        childSb.append(sql);
                        childSb.append(") ");
                        childSb.append(rel.getName());
                        childSb.append(" ");
                        if (Rel.NOT.equals(where.getType())) {
                            childSb.insert(0,where.getType() + " ");
                        }
                        sb.append(childSb);
                    }
                } else if(Rel.NESTED.equals(where.getType())) {
                    List<Where> params = where.getParams();
                    for (Where param : params) {
                        param.setField(where.getField() + "['" + param.getField() + "']");
                    }
                    String sql = buildWhereSql(params, Rel.AND, paramList, geoDistanceList, tableName);
                    sb.append(sql);
                } else {
                    String whereSql = buildWhereSql(where, rel, paramList, geoDistanceList, tableName);
                    sb.append(whereSql);
                }
            }
        }
        return sb.toString();
    }
}
