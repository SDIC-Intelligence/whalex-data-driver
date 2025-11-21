package com.meiya.whalex.db.util.param.impl.dwh;

import cn.hutool.core.date.DatePattern;
import cn.hutool.core.util.StrUtil;
import com.meiya.whalex.db.entity.*;
import com.meiya.whalex.db.entity.ani.AniHandler;
import com.meiya.whalex.db.entity.dwh.HiveDatabaseInfo;
import com.meiya.whalex.db.entity.dwh.HiveFieldTypeEnum;
import com.meiya.whalex.db.entity.dwh.HiveTableInfo;
import com.meiya.whalex.db.util.param.AbstractDbModuleParamUtil;
import com.meiya.whalex.exception.BusinessException;
import com.meiya.whalex.exception.ExceptionCode;
import com.meiya.whalex.interior.db.constant.ItemFieldTypeEnum;
import com.meiya.whalex.interior.db.constant.PartitionType;
import com.meiya.whalex.interior.db.operation.in.*;
import com.meiya.whalex.interior.db.search.in.QueryParamCondition;
import com.meiya.whalex.util.JsonUtil;
import com.meiya.whalex.util.date.DateUtil;
import com.meiya.whalex.util.date.JodaTimeUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Hive 参数转换实体
 *
 * @author 黄河森
 * @date 2019/12/26
 * @project whale-cloud-platformX
 */
@Slf4j
public class BaseHiveParamUtil<Q extends AniHandler, D extends HiveDatabaseInfo,
        T extends HiveTableInfo> extends AbstractDbModuleParamUtil<Q, D, T> {

    public static final String ADD_PARTITION = "ALTER TABLE `%s` ADD PARTITION (%s)";

    public static final String ADD_PARTITION_LOCATION = "ALTER TABLE `%s` ADD PARTITION (%s) location '%s'";

    public static final String DROP_PARTITION = "ALTER TABLE `%s` DROP PARTITION (%s)";

    public static final String PARTITION_INFO = "`%s`='%s'";

    public static final String ALTER_TABLE_RENAME = "ALTER TABLE `%s` RENAME TO `%s`";

    /**
     * 插入模板
     */
    public final static String INSERT_DATA_TEMPLATE = "INSERT INTO `%s` ( %s ) VALUES %s";

    @Override
    protected Q transitionListTableParam(QueryTablesCondition queryTablesCondition, D databaseConf) throws Exception {
        AniHandler aniHandler = new AniHandler();
        AniHandler.AniListTable aniListTable = new AniHandler.AniListTable();
        aniListTable.setTableMatch(queryTablesCondition.getTableMatch());
        aniListTable.setDatabaseName(queryTablesCondition.getDatabaseName());
        aniHandler.setListTable(aniListTable);
        return (Q) aniHandler;
    }

    @Override
    protected Q transitionListDatabaseParam(QueryDatabasesCondition queryDatabasesCondition, D databaseConf) throws Exception {
        AniHandler aniHandler = new AniHandler();
        AniHandler.AniListDatabase aniListDatabase = new AniHandler.AniListDatabase();
        aniListDatabase.setDatabaseMatch(queryDatabasesCondition.getDatabaseMatch());
        aniHandler.setListDatabase(aniListDatabase);
        return (Q) aniHandler;
    }

    @Override
    protected Q transitionCreateTableParam(CreateTableParamCondition createTableParamCondition, D databaseConf, T tableConf) throws Exception {
        // 若为表分区修复操作，则无需拼接建表sql
        if (createTableParamCondition.isRepair()) {
            AniHandler aniHandler = new AniHandler();
            AniHandler.AniCreateTable aniCreateTable = new AniHandler.AniCreateTable();
            aniHandler.setAniCreateTable(aniCreateTable);
            aniCreateTable.setRepair(createTableParamCondition.isRepair());
            return (Q) aniHandler;
        }
        AniHandler aniHandler = new AniHandler();
        AniHandler.AniCreateTable aniCreateTable = new AniHandler.AniCreateTable();
        aniHandler.setAniCreateTable(aniCreateTable);
        StringBuilder sql = new StringBuilder();

        String external = "";
        String fileTypeAndPath = "";
        // 每个字段间的分隔
        String fieldsTerminated = "";
        // Array 格式元素与元素间的分隔
        String collectionItemsTerminated = "";
        // K-V 格式分隔
        String mapKeysTerminated = "";
        // 换行分隔
        String linesTerminated = "";
        StringBuilder partitionSqlBuilder = new StringBuilder();

        if (StrUtil.isNotEmpty(tableConf.getFieldsTerminated())) {
            fieldsTerminated = " FIELDS TERMINATED BY '" + tableConf.getFieldsTerminated() + "'";
        }

        if (StrUtil.isNotEmpty(tableConf.getCollectionItemsTerminated())) {
            collectionItemsTerminated = " COLLECTION ITEMS TERMINATED BY '" + tableConf.getCollectionItemsTerminated() + "'";
        }

        if (StrUtil.isNotEmpty(tableConf.getMapKeysTerminated())) {
            mapKeysTerminated = " MAP KEYS TERMINATED BY '" + tableConf.getMapKeysTerminated() + "'";
        }

        if (StrUtil.isNotEmpty(tableConf.getLinesTerminated())) {
            linesTerminated = " LINES TERMINATED BY '" + tableConf.getMapKeysTerminated() + "'";
        }

        // 外部表路径、文件格式拼接
        if (!StrUtil.isAllBlank(tableConf.getFileType(), tableConf.getPath())) {
            if (StrUtil.isBlank(tableConf.getFileType()) || StrUtil.isBlank(tableConf.getPath())) {
                throw new BusinessException("Hive 创建外部表必须同时指定 fileType  和 path");
            }
            external = "EXTERNAL ";
            fileTypeAndPath =  " STORED AS " + tableConf.getFileType() + " LOCATION \"" + tableConf.getPath() + "\"";
        }

        sql.append("CREATE ").append(external).append("TABLE ");


        sql.append(tableConf.getTableName()).append("(");


        // 字段
        List<CreateTableParamCondition.CreateTableFieldParam> createTableFieldParamList = createTableParamCondition.getCreateTableFieldParamList();

        // 获取表级信息是否配置分区键
        String partitionFieldName = null;
        String format = tableConf.getFormat();
        if (StringUtils.isNotBlank(format)) {
            // 解析字段名
            String[] split = StringUtils.split(format, "/");
            for (String partitionFm : split) {
                partitionFieldName = StringUtils.substring(partitionFm, 0, StringUtils.indexOf(partitionFm, "="));
                if(partitionSqlBuilder.length() > 0) {
                    partitionSqlBuilder.append(", ");
                }
                partitionSqlBuilder.append(HiveParserUtil.DOUBLE_QUOTATION_MARKS)
                        .append(partitionFieldName)
                        .append(HiveParserUtil.DOUBLE_QUOTATION_MARKS)
                        .append(" ")
                        .append(HiveFieldTypeEnum.STRING.getDbFieldType());
            }
        }

        // 遍历字段
        for (int i = 0; i < createTableFieldParamList.size(); i++) {
            CreateTableParamCondition.CreateTableFieldParam createTableFieldParam = createTableFieldParamList.get(i);

            // 判断是否标记为分区字段, hive的分区字段不应该在此添加, 直接跳过
            if (createTableFieldParam.isPartition()) {
                if(StringUtils.isNotBlank(partitionFieldName) && partitionFieldName.equals(createTableFieldParam.getFieldName())) {
                    continue;
                }
                String fieldName = createTableFieldParam.getFieldName();
                String fieldType = createTableFieldParam.getFieldType();
                FieldTypeAdapter adapter = getAdapter(fieldType);
                log.info("hive库建表: 分区字段[{}({})]跳过普通字段构建逻辑!",
                        createTableFieldParam.getFieldName(),
                        createTableFieldParam.getFieldComment());

                if(partitionSqlBuilder.length() > 0) {
                    partitionSqlBuilder.append(", ");
                }
                partitionSqlBuilder.append(HiveParserUtil.DOUBLE_QUOTATION_MARKS)
                        .append(fieldName)
                        .append(HiveParserUtil.DOUBLE_QUOTATION_MARKS)
                        .append(" ")
                        .append(adapter.getDbFieldType());
                continue;
            }

            String fieldName = createTableFieldParam.getFieldName().toLowerCase();
            String fieldType = createTableFieldParam.getFieldType();
            Integer fieldLength = createTableFieldParam.getFieldLength();
            Integer fieldDecimalPoint = createTableFieldParam.getFieldDecimalPoint();
            boolean primaryKey = createTableFieldParam.isPrimaryKey();
            FieldTypeAdapter adapter = getAdapter(fieldType);
            sql.append(HiveParserUtil.DOUBLE_QUOTATION_MARKS)
                    .append(fieldName)
                    .append(HiveParserUtil.DOUBLE_QUOTATION_MARKS)
                    .append(" ")
                    .append(buildSqlDataType(adapter, fieldLength, fieldDecimalPoint));

            // 判断是否主键
            if (primaryKey) {
                sql.append(" PRIMARY KEY DISABLE NOVALIDATE RELY");
            } else {
                // 非空
                if (createTableFieldParam.isNotNull()) {
                    sql.append(" NOT NULL");
                }
            }
            // 设置字段备注
            if (StringUtils.isNotBlank(createTableFieldParam.getFieldComment())) {
                sql.append(" COMMENT ").append("\"").append(createTableFieldParam.getFieldComment()).append("\"");
            }

            sql.append(",");
        }

        sql.replace(sql.length() - 1, sql.length(), "");

        sql.append(")");
        // 设置表注释
        if (StringUtils.isNotBlank(createTableParamCondition.getTableComment())) {
            sql.append(" COMMENT ").append("\"").append(createTableParamCondition.getTableComment()).append("\"");
        }
        // 分区键
        if(partitionSqlBuilder.length() > 0) {
            sql.append(" ")
                    .append("PARTITIONED BY (")
                    .append(partitionSqlBuilder.toString())
                    .append(")");
            aniCreateTable.setPartition(true);
        }
        // 行分隔符定义
        if (!StringUtils.isAllBlank(fieldsTerminated, collectionItemsTerminated, mapKeysTerminated, linesTerminated)) {
            sql.append(" ROW FORMAT DELIMITED");
            if (StrUtil.isNotBlank(fieldsTerminated)) {
                sql.append(fieldsTerminated);
            }
            if (StrUtil.isNotBlank(collectionItemsTerminated)) {
                sql.append(collectionItemsTerminated);
            }
            if (StrUtil.isNotBlank(mapKeysTerminated)) {
                sql.append(mapKeysTerminated);
            }
            if (StrUtil.isNotBlank(linesTerminated)) {
                sql.append(linesTerminated);
            }
        }
        // 外部表路径与格式
        sql.append(fileTypeAndPath);
        aniCreateTable.setSql(sql.toString());
        aniCreateTable.setNotExists(createTableParamCondition.isNotExists());
        return (Q) aniHandler;
    }

    protected FieldTypeAdapter getAdapter(String fieldType) {
        return HiveFieldTypeEnum.findFieldTypeEnum(fieldType);
    }

    /**
     * 解析字段类型
     *
     * @param fieldLength
     * @param fieldDecimalPoint
     * @param adapter
     * @return
     */
    protected String buildSqlDataType(FieldTypeAdapter adapter, Integer fieldLength, Integer fieldDecimalPoint) {

        StringBuilder columnSb = new StringBuilder(adapter.getDbFieldType());

        // 判断是否需要长度
        if (!ItemFieldTypeEnum.ParamStatus.NO.equals(adapter.getNeedDataLength())) {
            // 如果当前没有传入长度，并且必须需要长度，则取默认值
            if (fieldLength == null && ItemFieldTypeEnum.ParamStatus.MUST.equals(adapter.getNeedDataLength())) {
                fieldLength = adapter.getFiledLength();
            }
            if (fieldLength != null) {
                columnSb.append("(")
                        .append(fieldLength)
                        .append(")");
                // 是否需要标度
                if (!ItemFieldTypeEnum.ParamStatus.NO.equals(adapter.getNeedDataDecimalPoint())) {
                    if (fieldDecimalPoint == null && ItemFieldTypeEnum.ParamStatus.MUST.equals(adapter.getNeedDataDecimalPoint())) {
                        fieldDecimalPoint = adapter.getFieldDecimalPoint();
                    }
                    if (fieldDecimalPoint != null) {
                        columnSb.delete(columnSb.length() - 1, columnSb.length())
                                .append(",")
                                .append(fieldDecimalPoint)
                                .append(")");
                    }
                }
            }
        }
        return columnSb.toString();
    }

    @Override
    protected Q transitionAlterTableParam(AlterTableParamCondition alterTableParamCondition, D databaseConf, T tableConf) throws Exception {

        List<String> sqlList = new ArrayList<>();

        PartitionInfo addPartition = alterTableParamCondition.getAddPartition();
        PartitionInfo delPartition = alterTableParamCondition.getDelPartition();

        MultiLevelPartitionInfo addMultiLevelPartition = alterTableParamCondition.getAddMultiLevelPartition();
        MultiLevelPartitionInfo delMultiLevelPartition = alterTableParamCondition.getDelMultiLevelPartition();


        String newTableName = alterTableParamCondition.getNewTableName();

        // 添加单分区
        if(addPartition != null) {

            String partitionField = addPartition.getPartitionField();

            if(StringUtils.isBlank(partitionField)) {
                throw new RuntimeException("hive分区字段不能为空");
            }

            PartitionType type = addPartition.getType();

            if(!PartitionType.EQ.equals(type)) {
                throw new RuntimeException("hive分区表只支持等值分区");
            }

            List<PartitionInfo.PartitionEq> eq = addPartition.getEq();
            if(CollectionUtils.isEmpty(eq)) {
                throw new RuntimeException("hive分区表参数列表不能为空");
            }

            for (PartitionInfo.PartitionEq partitionEq : eq) {
                String path = partitionEq.getPath();
                String value = partitionEq.getValue();
                if(StringUtils.isBlank(value)) {
                    throw new RuntimeException("分区字段参数不能为空");
                }
                String partitionInfo = String.format(PARTITION_INFO, partitionField, value);
                if(StringUtils.isBlank(path)) {
                    sqlList.add(String.format(ADD_PARTITION, tableConf.getTableName(),partitionInfo));
                }else {
                    sqlList.add(String.format(ADD_PARTITION_LOCATION, tableConf.getTableName(),partitionInfo, path));
                }
            }

        } else if (addMultiLevelPartition != null) {
            // 添加多级分区
            List<MultiLevelPartitionInfo.PartitionInfo> partitions = addMultiLevelPartition.getPartitions();

            if(CollectionUtils.isEmpty(partitions)) {
                throw new RuntimeException("hive分区字段不能为空");
            }

            PartitionType type = addMultiLevelPartition.getType();

            if(!PartitionType.EQ.equals(type)) {
                throw new RuntimeException("hive分区表只支持等值分区");
            }

            List<MultiLevelPartitionInfo.PartitionEq> eq = addMultiLevelPartition.getEq();
            if(CollectionUtils.isEmpty(eq)) {
                throw new RuntimeException("hive分区表参数列表不能为空");
            }

            List<String> partitionFields = partitions.stream().flatMap(partitionInfo -> Stream.of(partitionInfo.getPartitionField())).collect(Collectors.toList());
            if(CollectionUtils.isEmpty(partitionFields)) {
                throw new RuntimeException("hive分区字段不能为空");
            }
            for (MultiLevelPartitionInfo.PartitionEq partitionEq : eq) {
                String path = partitionEq.getPath();
                List<String> values = partitionEq.getValue();
                if(CollectionUtils.isEmpty(values)) {
                    throw new RuntimeException("分区字段参数不能为空");
                }
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < partitionFields.size(); i++) {
                    String partitionField = partitionFields.get(i);
                    String value = values.get(i);
                    if (sb.length() > 0) {
                        sb.append(",");
                    }
                    sb.append(String.format(PARTITION_INFO, partitionField, value));
                }
                if(StringUtils.isBlank(path)) {
                    sqlList.add(String.format(ADD_PARTITION, tableConf.getTableName(),sb));
                }else {
                    sqlList.add(String.format(ADD_PARTITION_LOCATION, tableConf.getTableName(),sb, path));
                }
            }
        }

        if(delPartition != null) {
            String partitionField = delPartition.getPartitionField();

            if(StringUtils.isBlank(partitionField)) {
                throw new RuntimeException("hive分区字段不能为空");
            }

            PartitionType type = delPartition.getType();

            if(!PartitionType.EQ.equals(type)) {
                throw new RuntimeException("hive分区表只支持等值分区");
            }

            List<PartitionInfo.PartitionEq> eq = delPartition.getEq();
            if(CollectionUtils.isEmpty(eq)) {
                throw new RuntimeException("hive分区表参数列表不能为空");
            }

            for (PartitionInfo.PartitionEq partitionEq : eq) {
                String value = partitionEq.getValue();
                if(StringUtils.isBlank(value)) {
                    throw new RuntimeException("分区字段参数不能为空");
                }
                String partitionInfo = String.format(PARTITION_INFO, partitionField, value);
                sqlList.add(String.format(DROP_PARTITION, tableConf.getTableName(),partitionInfo));
            }
        } else if (delMultiLevelPartition != null) {
            List<MultiLevelPartitionInfo.PartitionInfo> partitions = delMultiLevelPartition.getPartitions();
            if(CollectionUtils.isEmpty(partitions)) {
                throw new RuntimeException("hive分区字段不能为空");
            }
            PartitionType type = delMultiLevelPartition.getType();

            if(!PartitionType.EQ.equals(type)) {
                throw new RuntimeException("hive分区表只支持等值分区");
            }

            List<MultiLevelPartitionInfo.PartitionEq> eq = delMultiLevelPartition.getEq();
            if(CollectionUtils.isEmpty(eq)) {
                throw new RuntimeException("hive分区表参数列表不能为空");
            }

            List<String> partitionFields = partitions.stream().flatMap(partitionInfo -> Stream.of(partitionInfo.getPartitionField())).collect(Collectors.toList());
            if(CollectionUtils.isEmpty(partitionFields)) {
                throw new RuntimeException("hive分区字段不能为空");
            }

            for (MultiLevelPartitionInfo.PartitionEq partitionEq : eq) {
                List<String> values = partitionEq.getValue();
                if(CollectionUtils.isEmpty(values)) {
                    throw new RuntimeException("分区字段参数不能为空");
                }
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < partitionFields.size(); i++) {
                    String partitionField = partitionFields.get(i);
                    String value = values.get(i);
                    if (sb.length() > 0) {
                        sb.append(",");
                    }
                    sb.append(String.format(PARTITION_INFO, partitionField, value));
                }
                sqlList.add(String.format(DROP_PARTITION, tableConf.getTableName(),sb));
            }
        }

        if(StringUtils.isNotBlank(newTableName)) {
            sqlList.add(String.format(ALTER_TABLE_RENAME, tableConf.getTableName(), newTableName));
        }

        AniHandler aniHandler = new AniHandler();
        AniHandler.AniAlterTable aniAlterTable = new AniHandler.AniAlterTable();
        aniHandler.setAniAlterTable(aniAlterTable);
        aniAlterTable.setSqlList(sqlList);
        return (Q) aniHandler;
    }

    @Override
    protected Q transitionCreateIndexParam(IndexParamCondition indexParamCondition, D databaseConf, T tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "HiveParamUtil.transitionCreateIndexParam");
    }

    @Override
    protected Q transitionDropIndexParam(IndexParamCondition indexParamCondition, D databaseConf, T tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "HiveParamUtil.transitionCreateIndexParam");
    }

    @Override
    protected Q transitionQueryParam(QueryParamCondition queryParamCondition, D databaseConf, T tableConf) throws Exception {
        AniHandler aniHandler = new AniHandler();
        AniHandler.AniQuery aniQuery;
        // 如果存在自定义SQL，则不需要转换
        aniQuery = HiveParserUtil.parserQuerySql(queryParamCondition);
        aniQuery.setCount(queryParamCondition.isCountFlag());
        aniHandler.setAniQuery(aniQuery);
        return (Q) aniHandler;
    }

    @Override
    protected Q transitionUpdateParam(UpdateParamCondition updateParamCondition, D databaseConf, T tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "HiveParamUtil.transitionUpdateParam");
    }

    @Override
    protected Q transitionInsertParam(AddParamCondition addParamCondition, D databaseConf, T tableConf) throws Exception {
        AniHandler.AniInsert aniInsert = new AniHandler.AniInsert();

        List<Object> paramList = new ArrayList<>();

        // 入库数据
        List<Map<String, Object>> fieldValueList = addParamCondition.getFieldValueList();

        // 存储入库字段名
        Set<String> fieldSet = new LinkedHashSet<>();

        // 遍历获取字段名
        fieldValueList.forEach(fieldMap -> {
            fieldMap.forEach((key, value) -> {
                fieldSet.add(key);
            });
        });

        StringBuilder fieldBuilder = new StringBuilder();

        for (String key : fieldSet) {
            fieldBuilder.append(HiveParserUtil.DOUBLE_QUOTATION_MARKS)
                    .append(key)
                    .append(HiveParserUtil.DOUBLE_QUOTATION_MARKS)
                    .append(",");
        }
        fieldBuilder.deleteCharAt(fieldBuilder.length() - 1);

        StringBuilder insertSb = new StringBuilder();
        for (int i = 0; i < fieldValueList.size(); i++) {
            Map<String, Object> recordMap = fieldValueList.get(i);
            if (MapUtils.isEmpty(recordMap)) {
                continue;
            }

            StringBuilder valueSb = new StringBuilder();

            for (String field : fieldSet) {
                Object value = recordMap.get(field);
                Object o = translateValue(value);
                if (value instanceof Point) {
                    // 坐标类型特殊处理
                    valueSb.append(o);
                } else {
                    paramList.add(o);
                    valueSb.append("?");
                }
                valueSb.append(",");
            }

            valueSb.deleteCharAt(valueSb.length() - 1);

            insertSb.append("(");
            insertSb.append(valueSb);
            insertSb.append("),");
        }

        insertSb.deleteCharAt(insertSb.length() - 1);

        String insertSql = String.format(INSERT_DATA_TEMPLATE, tableConf.getTableName(), fieldBuilder.toString(), insertSb.toString());

        aniInsert.setSql(insertSql);

        aniInsert.setParamArray(paramList.toArray());

        AniHandler aniHandler = new AniHandler();
        aniHandler.setAniInsert(aniInsert);
        return (Q) aniHandler;
    }

    protected Object translateValue(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Date) {
            return cn.hutool.core.date.DateUtil.format((Date) value, DatePattern.NORM_DATETIME_PATTERN);
        } else if (value instanceof Timestamp) {
            return cn.hutool.core.date.DateUtil.format((Timestamp) value, DatePattern.NORM_DATETIME_PATTERN);
        } else if (value instanceof Point) {
            Point point = (Point) value;
            return point.getLon() + "," + point.getLat();
        } else if (value instanceof Boolean || value instanceof Integer || value instanceof Double || value instanceof Long || value instanceof Float) {
            return value;
        } else if (value instanceof List || (value.getClass().isArray() && !(value instanceof byte[])) || value instanceof Map) {
            return JsonUtil.objectToStr(value);
        }else {
            return value;
        }
    }

    @Override
    protected Q transitionDeleteParam(DelParamCondition delParamCondition, D databaseConf, T tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "HiveParamUtil.transitionDeleteParam");
    }

    @Override
    protected Q transitionDropTableParam(DropTableParamCondition dropTableParamCondition, D databaseConf, T tableConf) throws Exception {
        AniHandler aniHandler = new AniHandler();
        AniHandler.AniDropTable aniDropTable = new AniHandler.AniDropTable();
        aniHandler.setAniDropTable(aniDropTable);
        if (dropTableParamCondition != null) {
            if (StringUtils.isNotBlank(dropTableParamCondition.getStartTime())) {
                aniDropTable.setStartTime(DateUtil.convertToDate(dropTableParamCondition.getStartTime(), JodaTimeUtil.DEFAULT_YMD_FORMAT));
            }
            if (StringUtils.isNotBlank(dropTableParamCondition.getEndTime())) {
                aniDropTable.setStopTime(DateUtil.convertToDate(dropTableParamCondition.getEndTime(), JodaTimeUtil.DEFAULT_YMD_FORMAT));
            }
        }
        return (Q) aniHandler;
    }

    @Override
    public Q transitionUpsertParam(UpsertParamCondition paramCondition, D databaseConf, T tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "HiveParamUtil.transitionUpsertParam");
    }

    @Override
    public Q transitionUpsertParamBatch(UpsertParamBatchCondition paramCondition, D databaseConf, T tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + "transitionUpsertParamBatch");
    }

    @Override
    public Q getCreateDatabaseParam(CreateDatabaseParamCondition paramCondition, D dataConf) {

        String dbName = paramCondition.getDbName();
        if(StringUtils.isBlank(dbName)) {
            throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "数据库名称不能为空");
        }
        AniHandler.AniCreateDatabase aniCreateDatabase = new AniHandler.AniCreateDatabase();
        aniCreateDatabase.setDbName(dbName);
        aniCreateDatabase.setCharacter(paramCondition.getCharacter());
        aniCreateDatabase.setCollate(paramCondition.getCollate());
        // 组装对象
        AniHandler aniHandler = new AniHandler();
        aniHandler.setCreatedatabase(aniCreateDatabase);
        return (Q) aniHandler;
    }

    @Override
    public Q getDropDatabaseParam(DropDatabaseParamCondition paramCondition, D dataConf) {
        String dbName = paramCondition.getDbName();
        if(StringUtils.isBlank(dbName)) {
            throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "数据库名称不能为空");
        }
        AniHandler.AniDropDatabase aniDropDatabase = new AniHandler.AniDropDatabase();
        aniDropDatabase.setDbName(dbName);
        AniHandler aniHandler = new AniHandler();
        aniHandler.setDropdatabase(aniDropDatabase);
        return (Q) aniHandler;
    }
}
