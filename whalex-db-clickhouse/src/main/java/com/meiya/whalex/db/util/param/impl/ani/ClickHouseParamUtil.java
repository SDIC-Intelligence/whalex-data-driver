package com.meiya.whalex.db.util.param.impl.ani;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.DateUtil;
import com.meiya.whalex.annotation.DbParamUtil;
import com.meiya.whalex.db.entity.*;
import com.meiya.whalex.db.entity.ani.*;
import com.meiya.whalex.db.util.common.RelDbUtil;
import com.meiya.whalex.db.util.param.AbstractDbModuleParamUtil;
import com.meiya.whalex.exception.BusinessException;
import com.meiya.whalex.exception.ExceptionCode;
import com.meiya.whalex.interior.db.constant.*;
import com.meiya.whalex.interior.db.operation.in.*;
import com.meiya.whalex.interior.db.search.condition.AggFunctionType;
import com.meiya.whalex.interior.db.search.condition.AggOpType;
import com.meiya.whalex.interior.db.search.condition.Method;
import com.meiya.whalex.interior.db.search.condition.Rel;
import com.meiya.whalex.interior.db.search.in.*;
import com.meiya.whalex.util.AggResultTranslateUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;

import java.sql.Timestamp;
import java.util.*;

/**
 * @author 黄河森
 * @date 2022/7/6
 * @package com.meiya.whalex.db.util.helper.impl.ani
 * @project whalex-data-driver
 */
@DbParamUtil(dbType = DbResourceEnum.clickhouse, version = DbVersionEnum.CLICKHOUSE_22_2_2_1, cloudVendors = CloudVendorsEnum.OPEN)
@Slf4j
public class ClickHouseParamUtil extends AbstractDbModuleParamUtil<AniHandler, ClickHouseDatabaseInfo, ClickHouseTableInfo> {
    protected static final String DISTRIBUTED_KEY = " DISTRIBUTED BY "; // PG 分布表key
    protected static String DOUBLE_QUOTATION_MARKS = "";
    protected final static String SYMBOL_N = "\n";
    protected final static String SYMBOL_R = "\r";
    protected final static String SYMBOL_T = "\t";
    protected final static String SYMBOL_SINGLE_QUOTES = "'";
    protected final static String SYMBOL_SINGLE_QUOTES_COV = "''";
    protected final static String SYMBOL_X_ONE = "\\x";
    protected final static String SYMBOL_X_TOW = "\\\\x";
    protected final static String SYMBOL_SPRIT_ONE = "\\";
    protected final static String SYMBOL_SPRIT_TWO = "\\\\";
    protected final static String SYMBOL_REPLACE = " ";

    protected String getMarks() {
        return DOUBLE_QUOTATION_MARKS;
    }
    protected String getDistributedKey() {
        return DISTRIBUTED_KEY;
    }

    /**
     * 分布式集群建表模板
     */
    public final static String CREATE_TABLE_CLUSTER_DISTRIBUTE_TEMPLATE = "CREATE TABLE ${database}.${tableName} ON CLUSTER ${clusterName} AS ${database}.${shardTableName} ( ${fieldSet}) ENGINE = Distributed(${clusterName}, ${database}, ${shardTableName}, ${distributeField}) COMMENT '${tableComment}'";

    /**
     * 副本建表模板
     */
    public final static String CREATE_TABLE_CLUSTER_REPLICATED_TEMPLATE = "CREATE TABLE ${database}.${tableName} ON CLUSTER ${clusterName} ( ${fieldSet} ) ENGINE = Replicated${engine}('/clickhouse/tables/{layer}-{shard}/${tableName}', '{replica}') COMMENT '${tableComment}'";

    /**
     * 副本建表模板
     */
    public final static String CREATE_TABLE_REPLICATED_TEMPLATE = "CREATE TABLE ${database}.${tableName} ( ${fieldSet} ) ENGINE = Replicated${engine}('/clickhouse/tables/{layer}-{shard}/${tableName}', '{replica}') COMMENT '${tableComment}'";


    /**
     * 集群建表模板
     */
    public final static String CREATE_TABLE_CLUSTER_TEMPLATE = "CREATE TABLE ${database}.${tableName} ON CLUSTER ${clusterName} ( ${fieldSet} ) ENGINE = ${engine} COMMENT '${tableComment}'";

    /**
     * 单机建表模板
     */
    public final static String CREATE_TABLE_STAND_ALONE_TEMPLATE = "CREATE TABLE ${database}.${tableName} ( ${fieldSet} ) ENGINE = ${engine} COMMENT '${tableComment}'";

    /**
     * 修改表模板
     * ALTER TABLE ppz_test DROP COLUMN IF EXISTS name,
     * DROP COLUMN IF EXISTS pass;
     */
    protected final static String ALTER_TABLE_TEMPLATE = "ALTER TABLE ${tableName} %s";

    /**
     * 插入模板
     */
    protected final static String INSERT_DATA_TEMPLATE = "INSERT INTO ${tableName} ( %s ) VALUES %s";

    /**
     * 删除模板
     */
    protected final static String DELETE_DATA_TEMPLATE = "DELETE FROM ${tableName} %s";

    /**
     * 更新模板
     */
    protected final static String UPDATE_DATA_TEMPLATE = "UPDATE ${tableName} SET %s";

    /**
     * 创建索引
     */
    protected final static String CREATE_INDEX_TEMPLATE = "CREATE INDEX ${indexName} ON ${tableName} (${indexField})";

    /**
     * 圆形区域范围查询
     */
    protected final static String GEO_DISTANCE_TEMPLATE = "circle'((%s,%s),%s)' @> %s";

    /**
     * 新增或者更新
     */
    protected final static String UPSERT_DATA_TEMPLATE = "INSERT INTO ${tableName} ( %s ) VALUES (%s) ON conflict(%s) DO UPDATE SET %s";

    /**
     * 数组更新
     */
    protected final static String ARRAY_UPDATE_SET = "CASE $column @> ARRAY['$value'] WHEN TRUE THEN $column ELSE array_append($column, '$value') END";

    /**
     * 数组追加
     */
    protected final static String ARRAY_UPDATE_APPEND = "array_cat($column, $value)";

    @Override
    protected AniHandler transitionListTableParam(QueryTablesCondition queryTablesCondition, ClickHouseDatabaseInfo databaseInfo) throws Exception {
        AniHandler aniHandler = new AniHandler();
        AniHandler.AniListTable aniListTable = new AniHandler.AniListTable();
        aniHandler.setListTable(aniListTable);
        String tableMatch = queryTablesCondition.getTableMatch();
        tableMatch = StringUtils.replaceEach(tableMatch, new String[]{"*", "?"}, new String[]{"%", "_"});
        aniListTable.setTableMatch(tableMatch);
        return aniHandler;
    }

    @Override
    protected AniHandler transitionListDatabaseParam(QueryDatabasesCondition queryDatabasesCondition, ClickHouseDatabaseInfo databaseInfo) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + ".queryListDatabaseMethod");
    }

    @Override
    protected AniHandler transitionCreateTableParam(CreateTableParamCondition createTableParamCondition, ClickHouseDatabaseInfo databaseInfo, ClickHouseTableInfo tableInfo) {
        AniHandler aniHandler = new AniHandler();
        aniHandler.setAniCreateTable(parserCreateTableSql(createTableParamCondition));
        return aniHandler;
    }

    @Override
    protected AniHandler transitionAlterTableParam(AlterTableParamCondition alterTableParamCondition, ClickHouseDatabaseInfo databaseInfo, ClickHouseTableInfo tableInfo) throws Exception {
        AniHandler aniHandler = new AniHandler();
        aniHandler.setAniAlterTable(parserAlterTableSql(alterTableParamCondition));
        return aniHandler;
    }

    @Override
    protected AniHandler transitionCreateIndexParam(IndexParamCondition indexParamCondition, ClickHouseDatabaseInfo databaseInfo, ClickHouseTableInfo tableInfo) {
        AniHandler aniHandler = new AniHandler();
        AniHandler.AniCreateIndex aniCreateIndex = parserCreateIndexSql(indexParamCondition);
        aniHandler.setAniCreateIndex(aniCreateIndex);
        return aniHandler;
    }

    @Override
    protected AniHandler transitionDropIndexParam(IndexParamCondition indexParamCondition, ClickHouseDatabaseInfo databaseInfo, ClickHouseTableInfo tableInfo) {
        return null;
    }

    @Override
    protected AniHandler transitionQueryParam(QueryParamCondition queryParamCondition, ClickHouseDatabaseInfo databaseInfo, ClickHouseTableInfo tableInfo) {
        AniHandler aniHandler = new AniHandler();
        AniHandler.AniQuery aniQuery;
        aniQuery = parserQuerySql(queryParamCondition);
        aniQuery.setCount(queryParamCondition.isCountFlag());
        if(queryParamCondition.getBatchSize() != null){
            aniQuery.setBatchSize(queryParamCondition.getBatchSize());
        }
        aniHandler.setAniQuery(aniQuery);
        return aniHandler;
    }

    @Override
    protected AniHandler transitionUpdateParam(UpdateParamCondition updateParamCondition, ClickHouseDatabaseInfo databaseInfo, ClickHouseTableInfo tableInfo) {
        AniHandler aniHandler = new AniHandler();
        AniHandler.AniUpdate aniUpdate = parserUpdateSql(updateParamCondition);
        aniHandler.setAniUpdate(aniUpdate);
        return aniHandler;
    }

    @Override
    protected AniHandler transitionInsertParam(AddParamCondition addParamCondition, ClickHouseDatabaseInfo databaseInfo, ClickHouseTableInfo tableInfo) {
        AniHandler aniHandler = new AniHandler();
        AniHandler.AniInsert aniInsert = parserInsertSql(addParamCondition);
        aniHandler.setAniInsert(aniInsert);
        return aniHandler;
    }

    @Override
    protected AniHandler transitionDeleteParam(DelParamCondition delParamCondition, ClickHouseDatabaseInfo databaseInfo, ClickHouseTableInfo tableInfo) {
        AniHandler aniHandler = new AniHandler();
        aniHandler.setAniDel(parserDelSql(delParamCondition));
        return aniHandler;
    }

    @Override
    protected AniHandler transitionDropTableParam(DropTableParamCondition dropTableParamCondition, ClickHouseDatabaseInfo databaseInfo, ClickHouseTableInfo tableInfo) throws Exception {
        return new AniHandler();
    }

    @Override
    public AniHandler transitionUpsertParam(UpsertParamCondition paramCondition, ClickHouseDatabaseInfo databaseInfo, ClickHouseTableInfo tableInfo) throws Exception {
        AniHandler aniHandler = new AniHandler();
        aniHandler.setAniUpsert(parserUpsertSql(paramCondition));
        return aniHandler;
    }

    @Override
    public AniHandler transitionUpsertParamBatch(UpsertParamBatchCondition paramCondition, ClickHouseDatabaseInfo databaseInfo, ClickHouseTableInfo tableInfo) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + "transitionUpsertParamBatch");
    }

    /**
     * 构建聚合查询
     * @param paramCondition
     * @return
     */
    public AniHandler.AniQuery buildAggQuerySql(QueryParamCondition paramCondition) {
        AniHandler.AniQuery aniQuery = new AniHandler.AniQuery();
        StringBuilder sb = new StringBuilder();
        List<Aggs> aggList = paramCondition.getAggList();
        //关系型数据库，聚合只有一层
        Aggs aggs = aggList.get(0);
        // 拼接 select
        StringBuilder selectSb = new StringBuilder();
        // 拼接group by
        StringBuilder groupSb = new StringBuilder();
        List<Order> orderList = new ArrayList<>();
        List<AggFunction> aggFunctions = new ArrayList<>();
        // 聚合解析
        aggResolver(aggs, selectSb, groupSb, orderList, aggFunctions);
        sb.append(selectSb);
        sb.append(" from ");
        // 表名
        // 先使用占位符
        sb.append("%s");
        sb.append(" ");

        //where
        if (CollectionUtils.isNotEmpty(paramCondition.getWhere())) {
            List<Object> paramList = new LinkedList<>();
            sb.append(buildWhereSql(paramCondition.getWhere(), paramList));
            aniQuery.setParamArray(paramList.toArray());
        }
        // 分组
        sb.append(" ").append(groupSb);

        //排序， 每一个聚合函数的结果都有对应的别名，顾需要做相应的转换
        if(CollectionUtils.isNotEmpty(orderList)) {
            orderList.stream().forEach(item->{
                String field = item.getField();
                if(CollectionUtils.isNotEmpty(aggFunctions)) {
                    for(AggFunction aggFunction : aggFunctions) {
                        if(aggFunction.getField().equalsIgnoreCase(field)) {
                            item.setField(aggFunction.getFunctionName());
                            break;
                        }
                    }
                }
            });
        }
        sb.append(buildOrderSql(orderList));

        // 分页
        Page page = new Page();
        page.setOffset(aggs.getOffset());
        page.setLimit(aggs.getLimit());
        sb.append(buildLimitSql(page));

        // 详细数据
        aniQuery.setLimit(aggs.getLimit());
        aniQuery.setOffset(aggs.getOffset());
        aniQuery.setAgg(true);
        List<String> aggNames = new ArrayList<>();
        AggResultTranslateUtil.parserRelationDataAggKey(aggs, aggNames);
        aniQuery.setAggNames(aggNames);
        aniQuery.setSql(sb.toString());
        return aniQuery;
    }

    /**
     * 拼接聚合条件select
     *
     * @param aggs
     * @param selectSb
     * @param groupSb
     * @param orderList
     * @param aggFunctionList
     */
    public void aggResolver(Aggs aggs, StringBuilder selectSb, StringBuilder groupSb, List<Order> orderList, List<AggFunction> aggFunctionList) {
        AggOpType type = aggs.getType();
        if(!type.equals(AggOpType.GROUP) && !type.equals(AggOpType.TERMS)) {
            throw new BusinessException("关系型数据库，只支持 group 和 terms聚合操作");
        }
        if (selectSb.length() == 0) {
            selectSb.append("select ")
                    .append(getMarks())
                    .append(KeywordConstant.findKeyword(aggs.getField(), getMarks()))
                    .append(getMarks())
                    .append(" as ")
                    .append(aggs.getAggName());
        } else {
            selectSb.append(",")
                    .append(getMarks())
                    .append(KeywordConstant.findKeyword(aggs.getField(), getMarks()))
                    .append(getMarks())
                    .append(" as ")
                    .append(aggs.getAggName());
        }
        if (groupSb.length() == 0) {
            groupSb.append("group by ").append(getMarks())
                    .append(KeywordConstant.findKeyword(aggs.getField(), getMarks()))
                    .append(getMarks());
        } else {
            groupSb.append(",").append(getMarks())
                    .append(KeywordConstant.findKeyword(aggs.getField(), getMarks()))
                    .append(getMarks());
        }
        if (CollectionUtil.isNotEmpty(aggs.getOrders())) {
            orderList.addAll(aggs.getOrders());
        }
        if (CollectionUtil.isNotEmpty(aggs.getAggList())) {
            aggResolver(aggs.getAggList().get(0), selectSb, groupSb, orderList, aggFunctionList);
        } else {
            selectSb.append(",count(1) as doc_count");
            List<AggFunction> aggFunctions = aggs.getAggFunctions();
            if(CollectionUtils.isNotEmpty(aggFunctions)) {
                aggFunctionList.addAll(aggFunctions);
                aggFunctions.stream().forEach(item->{
                    // 关系型数据库中，distinct不做操作
                    if(item.getAggFunctionType() != AggFunctionType.DISTINCT) {
                        selectSb.append(", " + item.getAggFunctionType().getType() + "(" + getMarks() + KeywordConstant.findKeyword(aggs.getField(), getMarks()) + getMarks() + ") as " + item.getFunctionName());
                    }
                });
            }
        }
    }

    /**
     * 解析 查询 SQL
     *
     * @param paramCondition
     * @return
     */
    public AniHandler.AniQuery parserQuerySql(QueryParamCondition paramCondition) {

        if(CollectionUtils.isNotEmpty(paramCondition.getAggList())) {
            return buildAggQuerySql(paramCondition);
        }

        AniHandler.AniQuery aniQuery = new AniHandler.AniQuery();
        // 查询sql
        StringBuilder sb = new StringBuilder();
        // 总数sql
        StringBuilder sbCount = new StringBuilder();

        // 分区信息
        List<String> partitions = paramCondition.getPartition();

        // 若携带分区信息，则进行分区查询优化
        int unionIndex = 1;
        if(CollectionUtils.isEmpty(partitions)){
            unionIndex = 1;
        }else{
            unionIndex = partitions.size();
        }

        // 查询条件, 生成占位符的sql
        String whereSql = null;
        List<Object> paramListForPartition = null;
        List<Object> paramList = null;
        if (CollectionUtils.isNotEmpty(paramCondition.getWhere())) {
            paramListForPartition = new LinkedList<>();
            paramList = new LinkedList<>();
            whereSql = buildWhereSql(paramCondition.getWhere(), paramList);
        }

        for (int i= 0 ; i < unionIndex ; i ++){
            // 公用sql部分
            StringBuilder sbCom = new StringBuilder();
            if(i != 0){
                sb.append(" union all ");
                sbCount.append(" union all ");
            }
            sb.append("select ");
            sb.append(buildSelectSql(paramCondition.getSelect()));
            sb.append(" from ");
            sbCount.append("select count(1) as count from ");
            // 表名
            // 先使用占位符
            sbCom.append("%s");
            sb.append(" ");

            // 分区
            if(CollectionUtils.isNotEmpty(partitions)){
                sbCom.append(" PARTITION ");
                sbCom.append("(" + partitions.get(i) + ") ");
            }

            // 查询条件
            if (StringUtils.isNotBlank(whereSql)) {
                paramListForPartition.addAll(paramList);
                sbCom.append(whereSql);
            }

            sbCount.append(sbCom);

            sb.append(sbCom);

            //分组
            sb.append(buildGroupSql(paramCondition.getGroup()));
        }

        // 分区查询使用 union 结合分区结果
        if (unionIndex > 1) {
            sb.insert(0, "select * from ( ");
            sb.append(") t ");

            sbCount.insert(0, "select sum(count) as count from ( ");
            sbCount.append(") t ");
        }

        // 排序
        sb.append(buildOrderSql(paramCondition.getOrder()));

        // 分页
        sb.append(buildLimitSql(paramCondition.getPage()));

        // 总数
        aniQuery.setSqlCount(sbCount.toString());
        // 详细数据
        aniQuery.setSql(sb.toString());

        // 设置占位符参数
        if (CollectionUtils.isNotEmpty(paramListForPartition)) {
            aniQuery.setParamArray(paramListForPartition.toArray());
        }

        return aniQuery;
    }

    /**
     * 解析 修改表 SQL
     *
     * @return
     */
    private AniHandler.AniAlterTable parserAlterTableSql(AlterTableParamCondition alterTableParamCondition) {
        AniHandler.AniAlterTable aniAlterTable = new AniHandler.AniAlterTable();
        List<String> delTableFieldParamList = alterTableParamCondition.getDelTableFieldParamList();
        List<AlterTableParamCondition.AddTableFieldParam> addTableFieldParamList = alterTableParamCondition.getAddTableFieldParamList();

        // 构造删除sql
        StringBuilder delSqlBuilder = new StringBuilder();
        if(CollectionUtils.isNotEmpty(delTableFieldParamList)){
            delTableFieldParamList.forEach(element->{
                delSqlBuilder.append("DROP COLUMN IF EXISTS ").append(element).append(",");
            });
        }
        if (delSqlBuilder.length() != 0){
            delSqlBuilder.deleteCharAt(delSqlBuilder.length() - 1).append(";");
            aniAlterTable.setDelSql(String.format(ALTER_TABLE_TEMPLATE, delSqlBuilder.toString()));
        }else {
            aniAlterTable.setDelSql("");
        }

        // 构造字段添加sql
        StringBuilder addSqlBuilder = new StringBuilder();
        List<String> fieldCommentList = new ArrayList<>();
        addTableFieldParamList.forEach(addTableFieldParam -> {
            String fieldType = addTableFieldParam.getFieldType();
            String fieldComment = addTableFieldParam.getFieldComment();
            Integer fieldLength = addTableFieldParam.getFieldLength();
            Integer fieldDecimalPoint = addTableFieldParam.getFieldDecimalPoint();
            String fieldName = addTableFieldParam.getFieldName();
            ClickHouseFieldTypeEnum fieldTypeEnum = ClickHouseFieldTypeEnum.findFieldTypeEnum(fieldType, fieldLength);
            addSqlBuilder.append("ADD COLUMN ");
            addSqlBuilder.append(getMarks())
                    .append(KeywordConstant.findKeyword(fieldName, getMarks()))
                    .append(getMarks())
                    .append(SYMBOL_REPLACE)
                    .append(fieldTypeEnum.getDbFieldType());
            if (fieldTypeEnum.isNeedLength()) {
                if (fieldLength != null) {
                    addSqlBuilder.append("(")
                            .append(fieldLength)
                            .append(")");
                    if(fieldTypeEnum.getDbFieldType().equals(ClickHouseFieldTypeEnum.LONG4.getDbFieldType()) || fieldTypeEnum.getDbFieldType().equals(ClickHouseFieldTypeEnum.LONG8.getDbFieldType())){
                        addSqlBuilder.delete(addSqlBuilder.length()-1,addSqlBuilder.length())
                                .append(",")
                                .append(fieldDecimalPoint)
                                .append(")");
                    }
                } else if (fieldTypeEnum.getFiledLength() != null) {
                    addSqlBuilder.append("(")
                            .append(fieldTypeEnum.getFiledLength())
                            .append(")");
                    if(fieldTypeEnum.getDbFieldType().equals(ClickHouseFieldTypeEnum.LONG4.getDbFieldType()) || fieldTypeEnum.getDbFieldType().equals(ClickHouseFieldTypeEnum.LONG8.getDbFieldType())){
                        addSqlBuilder.delete(addSqlBuilder.length()-1,addSqlBuilder.length())
                                .append(",")
                                .append(fieldDecimalPoint)
                                .append(")");
                    }
                }
            }
            if (addTableFieldParam.getNotNull() != null && addTableFieldParam.getNotNull()) {
                addSqlBuilder.append(" NOT NULL");
            }
            if (addTableFieldParam.isPrimaryKey()) {
                addSqlBuilder.append(" PRIMARY KEY");
            }
            if (StringUtils.isNotBlank(fieldComment)) {
                addSqlBuilder.append(" COMMENT '").append(fieldComment).append("'");
            }
            addSqlBuilder.append(",");
        });
        if (addSqlBuilder.length() != 0){
            addSqlBuilder.deleteCharAt(addSqlBuilder.length() - 1);
            String addTableFieldSql = String.format(ALTER_TABLE_TEMPLATE, addSqlBuilder.toString());
            aniAlterTable.setAddSql(addTableFieldSql);
        }else {
            aniAlterTable.setAddSql("");
        }
        if (CollectionUtils.isNotEmpty(fieldCommentList)) {
            aniAlterTable.setFieldCommentList(fieldCommentList);
        }
        return aniAlterTable;
    }

    /**
     * 解析 建表 SQL
     *
     * @return
     */
    public AniHandler.AniCreateTable parserCreateTableSql(CreateTableParamCondition createTableParamCondition) {
        AniHandler.AniCreateTable createTable = new AniHandler.AniCreateTable();
        List<CreateTableParamCondition.CreateTableFieldParam> createTableFieldParamList = createTableParamCondition.getCreateTableFieldParamList();
        StringBuilder createSqlBuilder = new StringBuilder();
        // 存放分布键
        Set<String> distributedList = new LinkedHashSet<>();
        List<String> fieldCommentList = new ArrayList<>();
        createTableFieldParamList.forEach(createTableFieldParam -> {
            String fieldType = createTableFieldParam.getFieldType();
            String fieldComment = createTableFieldParam.getFieldComment();
            Integer fieldLength = createTableFieldParam.getFieldLength();
            Integer fieldDecimalPoint = createTableFieldParam.getFieldDecimalPoint();
            String fieldName = createTableFieldParam.getFieldName();
            ClickHouseFieldTypeEnum fieldTypeEnum = ClickHouseFieldTypeEnum.findFieldTypeEnum(fieldType, fieldLength);
            createSqlBuilder.append(getMarks())
                    .append(KeywordConstant.findKeyword(fieldName, getMarks()))
                    .append(getMarks())
                    .append(SYMBOL_REPLACE)
                    .append(fieldTypeEnum.getDbFieldType());
            if (fieldTypeEnum.isNeedLength()) {
                if (fieldLength != null) {
                    createSqlBuilder.append("(")
                            .append(fieldLength)
                            .append(")");
                    if(fieldTypeEnum.getDbFieldType().equals(ClickHouseFieldTypeEnum.LONG4.getDbFieldType()) || fieldTypeEnum.getDbFieldType().equals(ClickHouseFieldTypeEnum.LONG8.getDbFieldType())){
                        createSqlBuilder.delete(createSqlBuilder.length()-1,createSqlBuilder.length())
                                .append(",")
                                .append(fieldDecimalPoint)
                                .append(")");
                    }
                } else if (fieldTypeEnum.getFiledLength() != null) {
                    createSqlBuilder.append("(")
                            .append(fieldTypeEnum.getFiledLength())
                            .append(")");
                    if(fieldTypeEnum.getDbFieldType().equals(ClickHouseFieldTypeEnum.LONG4.getDbFieldType()) || fieldTypeEnum.getDbFieldType().equals(ClickHouseFieldTypeEnum.LONG8.getDbFieldType())){
                        createSqlBuilder.delete(createSqlBuilder.length()-1,createSqlBuilder.length())
                                .append(",")
                                .append(fieldDecimalPoint)
                                .append(")");
                    }
                }
            }
            if (createTableFieldParam.isNotNull()) {
                createSqlBuilder.append(" NOT NULL");
            }
            if (createTableFieldParam.isPrimaryKey()) {
                createSqlBuilder.append(" PRIMARY KEY");
            }
            if (StringUtils.isNotBlank(fieldComment)) {
                createSqlBuilder.append(" COMMENT '").append(fieldComment).append("'");
            }
            createSqlBuilder.append(",");
            if (createTableFieldParam.isDistributed()) {
                distributedList.add(KeywordConstant.findKeyword(fieldName, getMarks()));
            }
        });
        createSqlBuilder.deleteCharAt(createSqlBuilder.length() - 1);

        String createTableFieldSql = createSqlBuilder.toString();


        createTable.setSql(createTableFieldSql);
        // 判断是否存在分布键
        if (CollectionUtils.isNotEmpty(distributedList)) {
            createTable.setDistributedField(new AniHandler.DistributedField(distributedList.iterator().next()));
        }

        if (CollectionUtils.isNotEmpty(fieldCommentList)) {
            createTable.setFieldCommentList(fieldCommentList);
        }
        if (StringUtils.isNotBlank(createTableParamCondition.getTableComment())) {
            createTable.setTableComment(createTableParamCondition.getTableComment());
        }
        createTable.setNotExists(createTableParamCondition.isNotExists());
        return createTable;
    }

    /**
     * 解析 数据插入语句
     *
     * @param addParamCondition
     * @return
     */
    public AniHandler.AniInsert parserInsertSql(AddParamCondition addParamCondition) {
        AniHandler.AniInsert aniInsert = new AniHandler.AniInsert();
        // 入库数据
        List<Map<String, Object>> fieldValueList = addParamCondition.getFieldValueList();

        // 存储入库字段名
        Set<String> fieldSet = new LinkedHashSet<>();

        // 遍历获取字段名
        fieldValueList.forEach(fieldMap -> {
            fieldMap.forEach((key, value) -> {
                fieldSet.add(KeywordConstant.findKeyword(key, getMarks()));
            });
        });

        StringBuilder fieldBuilder = new StringBuilder();

        for (String key : fieldSet) {
            fieldBuilder.append(getMarks())
                    .append(key)
                    .append(getMarks())
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
                setValue(valueSb, value);
                valueSb.append(",");
            }

            valueSb.deleteCharAt(valueSb.length() - 1);

            insertSb.append("(");
            insertSb.append(valueSb);
            insertSb.append("),");
        }

        insertSb.deleteCharAt(insertSb.length() - 1);

        String insertSql = String.format(INSERT_DATA_TEMPLATE, fieldBuilder.toString(), insertSb.toString());

        aniInsert.setSql(insertSql);

        return aniInsert;
    }

    /**
     * 设置字段值
     *
     * @param valueSb
     * @param value
     */
    private void setValue(StringBuilder valueSb, Object value) {
        if (value == null) {
            valueSb.append("null");
            return;
        }
        if (value instanceof Date) {
            valueSb.append(SYMBOL_SINGLE_QUOTES);
            valueSb.append(DateUtil.format((Date) value, DatePattern.NORM_DATETIME_PATTERN));
            valueSb.append(SYMBOL_SINGLE_QUOTES);
        } else if (value instanceof Timestamp) {
            valueSb.append(SYMBOL_SINGLE_QUOTES);
            valueSb.append(DateUtil.format((Timestamp) value, DatePattern.NORM_DATETIME_PATTERN));
            valueSb.append(SYMBOL_SINGLE_QUOTES);
        } else if (value instanceof Point) {
            Point point = (Point) value;
            valueSb.append("point").append(SYMBOL_SINGLE_QUOTES).append("(").append(point.getLon()).append(",").append(point.getLat()).append(")").append(SYMBOL_SINGLE_QUOTES);
        } else if (value instanceof Integer || value instanceof Double || value instanceof Long || value instanceof Float) {
            valueSb.append(value);
        } else if (value instanceof List || (value.getClass().isArray() && !(value instanceof byte[]))) {
            valueSb.append("ARRAY[");
            List valueList;
            if (value.getClass().isArray()) {
                valueList = Arrays.asList(((Object[]) value));
            } else {
                valueList = (List) value;
            }
            for (int i = 0; i < valueList.size(); i++) {
                Object o = valueList.get(i);
                if (!(value instanceof Integer)
                        && !(value instanceof Double)
                        && !(value instanceof Long)
                        && !(value instanceof Float)) {
                    valueSb.append(SYMBOL_SINGLE_QUOTES);
                    valueSb.append(o);
                    valueSb.append(SYMBOL_SINGLE_QUOTES).append(",");
                } else {
                    valueSb.append(o);
                }
            }
            if (valueSb.toString().endsWith(",")) {
                valueSb.deleteCharAt(valueSb.length() - 1);
            }
            valueSb.append("]");
        } else {
            valueSb.append(SYMBOL_SINGLE_QUOTES);
            valueSb.append(formatter(String.valueOf(value)));
            valueSb.append(SYMBOL_SINGLE_QUOTES);
        }
    }

    /**
     * 解析 数据删除语句
     *
     * @param delParamCondition
     * @return
     */
    public AniHandler.AniDel parserDelSql(DelParamCondition delParamCondition) {
        AniHandler.AniDel aniDel = new AniHandler.AniDel();
        List<Where> where = delParamCondition.getWhere();
        if (CollectionUtils.isNotEmpty(where)) {
            List<Object> paramList = new LinkedList<>();
            String whereSql = buildWhereSql(where, paramList);
            String delSql = String.format(DELETE_DATA_TEMPLATE, whereSql);
            aniDel.setSql(delSql);
            aniDel.setParamArray(paramList.toArray());
        }
        return aniDel;
    }

    /**
     * 解析 创建索引语句
     *
     * @param indexParamCondition
     * @return
     */
    public AniHandler.AniCreateIndex parserCreateIndexSql(IndexParamCondition indexParamCondition) {
        AniHandler.AniCreateIndex aniCreateIndex = new AniHandler.AniCreateIndex();
        String column = indexParamCondition.getColumn();
        aniCreateIndex.setSql(StringUtils.replaceEach(CREATE_INDEX_TEMPLATE, new String[]{"${indexName}", "${indexField}"}, new String[]{"index_" + column, column}));
        return aniCreateIndex;
    }

    /**
     * 解析 数据更新语句
     *
     * @param updateParamCondition
     * @return
     */
    public AniHandler.AniUpdate parserUpdateSql(UpdateParamCondition updateParamCondition) {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + ".parserUpdateSql");
    }

    /**
     * 对插入数据字段值进行格式化
     *
     * @param value
     * @return
     */
    private String formatter(String value) {
        if (value.contains(SYMBOL_N) || value.contains(SYMBOL_R) || value.contains(SYMBOL_T)) {
            value = value.replaceAll(SYMBOL_N, SYMBOL_REPLACE).replaceAll(SYMBOL_R, SYMBOL_REPLACE)
                    .replaceAll(SYMBOL_T, SYMBOL_REPLACE);
            value = value.trim();
        }
        if (value.contains(SYMBOL_SINGLE_QUOTES)) {
            value = value.replace(SYMBOL_SINGLE_QUOTES, SYMBOL_SINGLE_QUOTES_COV);
            value = value.trim();
        }
        if (value.endsWith(SYMBOL_SPRIT_TWO) || value.endsWith(SYMBOL_SPRIT_ONE)) {
            value = value.replace(SYMBOL_SPRIT_TWO, SYMBOL_REPLACE).replace(SYMBOL_SPRIT_ONE, SYMBOL_REPLACE);
            value = value.trim();
        }
        if (value.contains(SYMBOL_X_ONE) || value.contains(SYMBOL_X_TOW)) {
            value = value.replace(SYMBOL_SPRIT_ONE, SYMBOL_REPLACE).replace(SYMBOL_X_TOW, SYMBOL_REPLACE);
            value = value.trim();
        }
        return value;
    }

    /**
     * 构造 SELECT
     *
     * @param selects
     * @return
     */
    private String buildSelectSql(List<String> selects) {
        StringBuilder sb = new StringBuilder();
        if (selects != null && !selects.isEmpty()) {
            for (String select : selects) {
                if (StringUtils.isBlank(select)) {
                    continue;
                }
                String checkMethod = checkMethod(select);
                if (StringUtils.isBlank(checkMethod)) {

                    String checkAlias = checkAlias(select, getMarks());
                    if (StringUtils.isBlank(checkAlias)) {
                        sb.append(getMarks());
                        sb.append(KeywordConstant.findKeyword(select, getMarks()));
                        sb.append(getMarks());
                    } else {
                        sb.append(checkAlias);
                    }
                    sb.append(",");

                } else {
                    sb.append(checkMethod);
                    sb.append(",");
                }
            }
        }

        if (sb.length() > 0) {
            sb.deleteCharAt(sb.length() - 1);
        } else {
            sb.append(" * ");
        }
        return sb.toString();
    }

    /**
     * 验证查询字段是否带有函数
     * f(max(fieldName))
     *
     * MAX(fieldName) AS "max(fieldName)"
     *
     * @param select
     * @return
     */
    private String checkMethod(String select) {
        boolean flag = StringUtils.startsWithIgnoreCase(select, "f(");
        if (flag) {
            select = select.substring(2, select.lastIndexOf(")"));
            String methodName = select.substring(0, select.indexOf("("));
            String parse = Method.parse(methodName.toLowerCase()).getName();
            String field = select.substring(select.indexOf("(") + 1, select.lastIndexOf(")"));
            StringBuffer stringBuffer = new StringBuffer();
            StringBuffer queryField = stringBuffer.append(parse).append("(").append(getMarks()).append(KeywordConstant.findKeyword(field, getMarks())).append(getMarks()).append(") AS ").append("\"")
                    .append(parse)
                    .append("(")
                    .append(field)
                    .append(")")
                    .append("\"");
            return queryField.toString();
        } else {
            return null;
        }
    }

    /**
     * 构造 WHERE
     *
     * @param wheres
     * @return
     */
    private String buildWhereSql(List<Where> wheres, List<Object> paramList) {
        String sql = buildWhereSql(wheres, Rel.AND, paramList);
        if (sql.length() > 0) {
            sql = sql.substring(0, sql.length() - 4);
            return " where " + sql;
        }
        return "";
    }

    /**
     * 构造 WHERE 递归
     *
     * @param wheres
     * @return
     */
    private String buildWhereSql(List<Where> wheres, Rel rel, List<Object> paramList) {
        StringBuilder sb = new StringBuilder();
        if (wheres != null && !wheres.isEmpty()) {
            for (Where where : wheres) {
                if (Rel.AND.equals(where.getType()) || Rel.OR.equals(where.getType()) || Rel.NOT.equals(where.getType())) {
                    List<Where> params = where.getParams();
                    String sql = buildWhereSql(params, where.getType(), paramList);
                    if (sql.length() > 0) {
                        sb.append(" (");
                        sql = sql.substring(0, sql.length() - 2 - where.getType().getName().length());
                        sb.append(sql);
                        sb.append(") ");
                        sb.append(rel.getName());
                        sb.append(" ");
                        if (Rel.NOT.equals(where.getType())) {
                            sb.insert(0,where.getType() + " ");
                        }
                    }
                } else {
                    sb.append(buildWhereSql(where, rel, paramList));
                }
            }
        }
        return sb.toString();
    }

    /**
     * 构造 WHERE 递归
     *
     * @param where
     * @param rel
     * @return
     */
    private String buildWhereSql(Where where, Rel rel, List<Object> paramList) {
        StringBuilder sb = new StringBuilder();
        if (whereHandler(where)) {
            sb.append(relStrHandler(where, paramList));
            sb.append(" ");
            sb.append(rel.getName());
            sb.append(" ");
        }
        return sb.toString();
    }

    /**
     * 字段判断，不为空并且不为分表时间字段
     *
     * @param where
     * @return
     */
    private boolean whereHandler(Where where) {
        return where != null && !StringUtils.isBlank(where.getField()) && !CommonConstant.BURST_ZONE.equalsIgnoreCase(where.getField());
    }

    /**
     * 解析操作符
     *
     * @param where
     * @return
     */
    private String relStrHandler(Where where, List<Object> paramList) {
        // 解决pg 日期查询问题
        String placeholder = "?";
        if(ClickHouseFieldTypeEnum.DATE.getFieldType().equalsIgnoreCase(where.getParamType())
                || ClickHouseFieldTypeEnum.TIME.getFieldType().equalsIgnoreCase(where.getParamType())) {
            placeholder = "?::timestamp";
        }
        Rel rel = where.getType();
        StringBuilder sb = new StringBuilder();
        sb.append(getMarks());
        sb.append(KeywordConstant.findKeyword(where.getField(), getMarks()));
        sb.append(getMarks());
        switch (rel) {
            case EQ:
            case TERM:
                sb.append(" = ");
                sb.append(placeholder);
                paramList.add(where.getParam());
                break;
            case NE:
                sb.append(" != ");
                sb.append(placeholder);
                paramList.add(where.getParam());
                break;
            case GT:
                sb.append(" > ");
                sb.append(placeholder);
                paramList.add(where.getParam());
                break;
            case GTE:
                sb.append(" >= ");
                sb.append(placeholder);
                paramList.add(where.getParam());
                break;
            case LT:
                sb.append(" < ");
                sb.append(placeholder);
                paramList.add(where.getParam());
                break;
            case LTE:
                sb.append(" <= ");
                sb.append(placeholder);
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
            case EXISTS:
            case NOT_NULL:
                sb.append(" is not null ");
                break;
            case MIDDLE_LIKE:
                sb.append(" like ");
                sb.append(" ? ");
                String sqlParam = (String)where.getParam();
                if(sqlParam.startsWith("*") && sqlParam.endsWith("*")){
                    sqlParam ="%" + sqlParam.substring(1,sqlParam.length()-1) + "%";
                }
                if(!sqlParam.startsWith("%") && !sqlParam.endsWith("%")){
                    sqlParam ="%" + sqlParam + "%";
                }
                paramList.add(sqlParam);
                break;
            case LIKE:
                sb.append(" like ");
                sb.append("?");
                paramList.add("%" + where.getParam() + "%");
                break;
            case FRONT_LIKE:
                sb.append(" like ");
                sb.append("?");
                paramList.add("%" + where.getParam());
                break;
            case TAIL_LIKE:
                sb.append(" like ");
                sb.append("?");
                paramList.add(where.getParam() + "%");
                break;
            case NOT_FRONT_LIKE:
                sb.append(" not like ");
                sb.append("?");
                paramList.add("%" + where.getParam());
                break;
            case NOT_MIDDLE_LIKE:
                sb.append(" not like ");
                sb.append("?");
                paramList.add("%" + where.getParam() +"%");
                break;
            case NOT_TAIL_LIKE:
                sb.append(" not like ");
                sb.append("?");
                paramList.add(where.getParam() +"%");
                break;
            case REGEX:
                sb.append(" ~ ");
                sb.append("?");
                paramList.add(where.getParam());
            case GEO_DISTANCE:
                // 坐标查询不放在 where 中，去掉前面 append 添加的字段名
                //如果不带单位，默契认为m
                if (where.getParam() instanceof Map) {
                    // 坐标查询不放在 where 中，去掉前面 append 添加的字段名
                    sb.delete(sb.lastIndexOf(getMarks() + KeywordConstant.findKeyword(where.getField(), getMarks()) + getMarks()), sb.length());
                    Map<String, Object> paramMap = (Map) where.getParam();
                    String loc = ObjectUtils.toString(paramMap.get(CommonConstant.LOC));
                    String[] split = loc.split(CommonConstant.COMMA);
                    double x = Double.valueOf(split[0]);
                    double y = Double.valueOf(split[1]);
                    String s = DistanceUnitEnum.kmToM(paramMap.get(CommonConstant.DISTANCE));
                    // pg中 1m = 0.00001
                    double distance = Double.parseDouble(s) * 0.00001;
                    sb.append(String.format(GEO_DISTANCE_TEMPLATE, x, y, distance , getMarks() + KeywordConstant.findKeyword(where.getField(), getMarks()) + getMarks()));
                } else {
                    log.error("GEO_DISTANCE的参数无法解析:" + where.getParam());
                }
                break;
            default:
                sb.append(" = ");
                sb.append(placeholder);
                paramList.add(where.getParam());
                break;
        }
        return sb.toString();
    }

    /**
     * 解析分页
     *
     * @param page
     * @return
     */
    private static String buildLimitSql(Page page) {
        page = RelDbUtil.pageHandler(page);
        StringBuilder sb = new StringBuilder();
        if (!page.getLimit().equals(Page.LIMIT_ALL_DATA)) {
            sb.append(" limit ").append(page.getLimit());
        }
        if (page.getOffset() > 0) {
            sb.append(" offset ").append(page.getOffset());
        }
        return sb.toString();
    }

    /**
     * 解析排序
     * @param orders
     * @return
     */
    public String buildOrderSql(List<Order> orders) {
        StringBuilder sb = new StringBuilder();
        if (orders != null && !orders.isEmpty()) {
            StringBuilder sb1 = new StringBuilder();
            for (Order order : orders) {
                if (!RelDbUtil.orderHandler(order)) {
                    continue;
                }
                sb1.append(getMarks());
                sb1.append(KeywordConstant.findKeyword(order.getField(), getMarks()));
                sb1.append(getMarks());
                sb1.append(" ");
                sb1.append(order.getSort().getName());
                sb1.append(",");
            }
            if (sb1.length() > 0) {
                sb1.deleteCharAt(sb1.length() - 1);
                sb.append(" order by ");
                sb.append(sb1);
            }
        }
        return sb.toString();
    }

    /**
     * 解析分组
     * @param groupList
     * @return
     */
    public String buildGroupSql(List<String> groupList) {
        StringBuilder sb = new StringBuilder();
        if (groupList != null && !groupList.isEmpty()) {
            sb.append(" GROUP BY ");
            for (int i = 0; i < groupList.size(); i++) {
                String group = groupList.get(i);
                sb.append(getMarks())
                        .append(group)
                        .append(getMarks());
                if (i < groupList.size() - 1) {
                    sb.append(",");
                }
            }
        }
        return sb.toString();
    }

    public AniHandler.AniUpsert parserUpsertSql(UpsertParamCondition paramCondition) {

        // 入库数据
        Map<String, Object> fieldValueMap = paramCondition.getUpsertParamMap();
        // 存储入库字段名
        Set<String> fieldSet = fieldValueMap.keySet();
        // 存放字段列表Str
        StringBuilder fieldBuilder = new StringBuilder();
        for (String key : fieldSet) {
            fieldBuilder.append(getMarks())
                    .append(KeywordConstant.findKeyword(key, getMarks()))
                    .append(getMarks())
                    .append(",");
        }
        fieldBuilder.deleteCharAt(fieldBuilder.length() - 1);
        // 存放入库值Str
        StringBuilder insertSb = new StringBuilder();
        StringBuilder valueSb = new StringBuilder();
        for (String field : fieldSet) {
            Object value = fieldValueMap.get(field);
            setValue(valueSb, value);
            valueSb.append(",");
        }
        valueSb.deleteCharAt(valueSb.length() - 1);
        insertSb.append(valueSb);

        // 拼接更新部分SQL
        StringBuilder updateSb = new StringBuilder();
        // 比对字段
        List<String> conflictFieldList = paramCondition.getConflictFieldList();
        fieldValueMap.forEach((key, value) -> {
            if (conflictFieldList.contains(key)) {
                return;
            }
            updateSb.append(getMarks())
                    .append(KeywordConstant.findKeyword(key, getMarks()))
                    .append(getMarks())
                    .append(" = ");
            setValue(updateSb, value);
            updateSb.append(",");
        });
        updateSb.deleteCharAt(updateSb.length() - 1);

        // 拼接 conflict
        StringBuilder cfSb = new StringBuilder();
        conflictFieldList.forEach((conflictField) -> cfSb.append(conflictField).append(","));
        cfSb.deleteCharAt(cfSb.length() - 1);
        AniHandler.AniUpsert aniUpsert = new AniHandler.AniUpsert();
        aniUpsert.setSql(String.format(UPSERT_DATA_TEMPLATE, fieldBuilder.toString(), insertSb.toString(), cfSb.toString(), updateSb.toString()));
        return aniUpsert;

    }
}
