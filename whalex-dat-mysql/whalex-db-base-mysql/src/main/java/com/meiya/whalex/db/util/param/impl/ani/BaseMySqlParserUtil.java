package com.meiya.whalex.db.util.param.impl.ani;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.map.MapUtil;
import cn.hutool.core.util.ArrayUtil;
import com.meiya.whalex.db.constant.IndexType;
import com.meiya.whalex.db.entity.*;
import com.meiya.whalex.db.entity.ani.*;
import com.meiya.whalex.db.util.common.RelDbUtil;
import com.meiya.whalex.exception.BusinessException;
import com.meiya.whalex.exception.ExceptionCode;
import com.meiya.whalex.interior.db.constant.CommonConstant;
import com.meiya.whalex.interior.db.constant.DistanceUnitEnum;
import com.meiya.whalex.interior.db.constant.ForeignKeyActionEnum;
import com.meiya.whalex.interior.db.constant.ItemFieldTypeEnum;
import com.meiya.whalex.interior.db.operation.in.*;
import com.meiya.whalex.interior.db.search.condition.*;
import com.meiya.whalex.interior.db.search.in.*;
import com.meiya.whalex.keyword.KeyWordHandler;
import com.meiya.whalex.util.AggResultTranslateUtil;
import com.meiya.whalex.util.JsonUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;

import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Stream;

/**
 * 关系型数据库通用，解析查询实体转换为SQL
 *
 * @author 黄河森
 * @date 2019/12/23
 * @project whale-cloud-platformX
 */
@Slf4j
public class BaseMySqlParserUtil {

    public final static KeyWordHandler keyWordHandler = new BaseMysqlKeywordHandler();

    protected final static String DOUBLE_QUOTATION_MARKS = "`";
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

    /**
     * 建表模板
     */
    protected final static String CREATE_TABLE_TEMPLATE = "CREATE TABLE `%s` ( %s ) COMMENT='%s' ENGINE=%s DEFAULT CHARSET=utf8";

    /**
     * 建表模板
     */
    protected final static String CREATE_TABLE_LIKE_TEMPLATE = "CREATE TABLE `%s` LIKE `%s`";

    /**
     * 插入模板
     */
    protected final static String INSERT_DATA_TEMPLATE = "INSERT INTO `%s` ( %s ) VALUES %s";

    /**
     * 删除模板
     */
    protected final static String DELETE_DATA_TEMPLATE = "DELETE FROM `%s` %s";

    /**
     * 更新模板
     */
    protected final static String UPDATE_DATA_TEMPLATE = "UPDATE `%s` SET %s";

    /**
     * 创建索引
     */
    protected final static String CREATE_INDEX_TEMPLATE = "CREATE %s INDEX %s ON `%s`(%s)";

    /**
     * 删除索引
     */
    private final static String DROP_INDEX_TEMPLATE = "DROP INDEX %s ON `%s`";

    /**
     * 坐标
     */
    protected final static String POINT_TEMPLATE = "GeomFromText('POINT(%s %s)')";

    /**
     * MYSQL 距离函数，计算结果为 度，需要乘以 111195 转为米
     */
    protected final static String ST_DISTANCE = "(ST_DISTANCE(%s, POINT(%s, %s)) * 111195) AS %s";

    /**
     * 插入模板
     */
    protected final static String UPSERT_DATA_TEMPLATE = "INSERT INTO `%s` (%s) VALUES %s ON DUPLICATE KEY UPDATE %s";

    /**
     * 更新数组操作：排重追加
     */
    protected final static String UPDATE_ARRAY_SET_TEMPLATE = "CASE JSON_CONTAINS($column,'$value') WHEN 1 THEN $column ELSE json_merge_preserve($column,'$value') END";

    /**
     * 更新数组操作：排重追加
     */
    protected final static String UPDATE_ARRAY_LIST_TEMPLATE = "json_merge_preserve($column,'$value')";


    /**
     * 删除表字段
     */
    protected final static String DROP_TABLE_FIELD = "ALTER TABLE `%s` DROP `%s`";

    /**
     *新增表主键
     */
    protected final static String ADD_PRIMARY_KEY = "ALTER TABLE `%s` ADD PRIMARY KEY(`%s`)";

    /**
     *删除主键
     */
    protected final static String DROP_PRIMARY_KEY = "ALTER TABLE `%s` DROP PRIMARY KEY";

    /**
     *新增表外键
     */
    protected final static String ADD_FOREIGN_KEY = "ALTER TABLE `%s` ADD CONSTRAINT `%s` FOREIGN KEY(`%s`) REFERENCES `%s`(`%s`) ON DELETE %s ON UPDATE %s";

    /**
     *新增表外键
     */
    protected final static String DROP_FOREIGN_KEY = "ALTER TABLE `%s` DROP FOREIGN KEY `%s`";

    /**
     * 修改字段名称
     */
    protected final static String UPDATE_TABLE_FIELD_NAME = "ALTER TABLE `%s` CHANGE  `%s`  `%s` ${dataType_%s} ${notNull_%s} ${DEFAULT_%s} ${COMMENT_%s}";


    /**
     * 修改表名
     */
    protected final static String UPDATE_TABLE_NAME = "ALTER TABLE `%s` RENAME TO `%s`";

    /**
     * 修改表描述
     */
    protected final static String ALTER_TABLE_COMMENT = "ALTER TABLE `%s` COMMENT '%s'";

    /**
     * 删除字段约束
     */
    protected final static String DROP_COLUMN_CONSTRAINT = "ALTER TABLE `%s` ALTER COLUMN `%s` DROP %s";

    protected String getDropTableFieldSQL() {
        return DROP_TABLE_FIELD;
    }

    protected String getUpdateTableNameSQL() {
        return UPDATE_TABLE_NAME;
    }

    protected String getUpdateTableFieldNameSQL() {
        return UPDATE_TABLE_FIELD_NAME;
    }

    /**
     * 构建聚合查询
     * @param paramCondition
     * @return
     */
    public AniHandler.AniQuery buildAggQuerySql(QueryParamCondition paramCondition, BaseMySqlDatabaseInfo baseMySqlDatabaseInfo, BaseMySqlTableInfo baseMySqlTableInfo) {
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
        sb.append(DOUBLE_QUOTATION_MARKS).append(baseMySqlTableInfo.getTableName()).append(DOUBLE_QUOTATION_MARKS);
        sb.append(" ");

        //where
        List<Map<String, Object>> geoDistanceList = new ArrayList<>(1);
        if (CollectionUtils.isNotEmpty(paramCondition.getWhere())) {
            List<Object> paramList = new LinkedList<>();
            sb.append(buildWhereSql(paramCondition.getWhere(), paramList, geoDistanceList, null));
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
        sb.append(buildOrderSql(orderList, null));

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
            selectSb.append("select ").append(aggs.getField()).append(" as ").append(aggs.getAggName());
        } else {
            selectSb.append(",").append(aggs.getField()).append(" as ").append(aggs.getAggName());
        }
        if (groupSb.length() == 0) {
            groupSb.append("group by ").append(aggs.getField());
        } else {
            groupSb.append(",").append(aggs.getField());
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
                        selectSb.append(", " + item.getAggFunctionType().getType() + "(" + item.getField() + ") as " + item.getFunctionName());
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
    public AniHandler.AniQuery parserQuerySql(QueryParamCondition paramCondition, BaseMySqlDatabaseInfo baseMySqlDatabaseInfo, BaseMySqlTableInfo baseMySqlTableInfo) {
        if(CollectionUtils.isNotEmpty(paramCondition.getAggList())) {
            return buildAggQuerySql(paramCondition, baseMySqlDatabaseInfo, baseMySqlTableInfo);
        }
        // 子表关联
        AssociatedQuery associatedQuery = paramCondition.getAssociatedQuery();

        AniHandler.AniQuery aniQuery = new AniHandler.AniQuery();
        // 查询sql
        StringBuilder sb = new StringBuilder();
        // 总数sql
        StringBuilder sbCount = new StringBuilder();
        // 公用sql部分
        StringBuilder sbCom = new StringBuilder();

        sb.append("select ");
        if (associatedQuery != null) {
            sb.append(buildSelectSql(paramCondition.getSelect(), baseMySqlTableInfo.getTableName()));
            sb.append(",");
            sb.append(buildSelectSql(associatedQuery.getSelect(), associatedQuery.getTableName()));
        } else {
            sb.append(buildSelectSql(paramCondition.getSelect(), null));
        }

        sb.append(" from ");

        sbCount.append("select count(1) as count from ");

        // 表名
        // 先使用占位符
        sbCom.append(DOUBLE_QUOTATION_MARKS).append(baseMySqlTableInfo.getTableName()).append(DOUBLE_QUOTATION_MARKS);
        sb.append(" ");

        // 子表查询判断
        if (associatedQuery != null) {
            String tableName = associatedQuery.getTableName();
            AssociatedType type = associatedQuery.getType();
            sbCom.append(" ");
            switch (type) {
                case LEFT_JOIN:
                    sbCom.append("LEFT JOIN ");
                    break;
                case INNER_JOIN:
                    sbCom.append("INNER JOIN ");
                    break;
                case FULL_JOIN:
                    sbCom.append("FULL OUTER JOIN ");
                default:
                    sbCom.append("LEFT JOIN ");
                    break;
            }
            sbCom.append(tableName).append(" ON ").append(baseMySqlTableInfo.getTableName()).append(".").append(associatedQuery.getLocalField())
                    .append(" = ").append(tableName).append(".").append(associatedQuery.getForeignField());
        }

        // 查询条件, 生成占位符的sql
        if (CollectionUtils.isNotEmpty(paramCondition.getWhere())) {
            //  是否包含范围查询
            List<Map<String, Object>> geoDistanceList = new ArrayList<>(1);
            List<Object> paramList = new LinkedList<>();
            if (associatedQuery != null) {
                sbCom.append(buildWhereSql(paramCondition.getWhere(), paramList, geoDistanceList, baseMySqlTableInfo.getTableName() + "."));
            } else {
                sbCom.append(buildWhereSql(paramCondition.getWhere(), paramList, geoDistanceList, null));
            }
            aniQuery.setParamArray(paramList.toArray());

            // 若包含范围查询需要增加 SELECT 和 HIVING 子句
            if (CollectionUtils.isNotEmpty(geoDistanceList)) {
                Map<String, Object> map = geoDistanceList.get(0);
                sb.insert(sb.indexOf("from"), "," + String.format(ST_DISTANCE, map.get("fieldName"), map.get(CommonConstant.LOC), map.get(CommonConstant.LAT), map.get("asName")) + SYMBOL_REPLACE);
                sbCom.append(" HAVING ").append(map.get("asName")).append(" < ").append(map.get(CommonConstant.DISTANCE));
            }
        }

        // 处理关联查询子表条件
        if (associatedQuery != null && associatedQuery.getWhere() != null) {
            //  是否包含范围查询
            List<Map<String, Object>> geoDistanceList = new ArrayList<>(1);
            List<Object> paramList = new LinkedList<>();
            String subSQL = buildWhereSql(CollectionUtil.newArrayList(associatedQuery.getWhere()), paramList, geoDistanceList, associatedQuery.getTableName() + ".");
            if (StringUtils.contains(sbCom.toString(), "where")) {
                sbCom.append(StringUtils.replace(subSQL, "where", "and"));
                aniQuery.setParamArray(ArrayUtil.append(aniQuery.getParamArray(), paramList.toArray()));
            } else {
                sbCom.append(subSQL);
                aniQuery.setParamArray(paramList.toArray());
            }
            // 若包含范围查询需要增加 SELECT 和 HIVING 子句
            if (CollectionUtils.isNotEmpty(geoDistanceList)) {
                Map<String, Object> map = geoDistanceList.get(0);
                sb.insert(sb.indexOf("from")
                        , "," + String.format(ST_DISTANCE
                                , associatedQuery.getTableName() + "." + map.get("fieldName")
                                , map.get(CommonConstant.LOC)
                                , map.get(CommonConstant.LAT)
                                , map.get("asName")) + SYMBOL_REPLACE);
                sbCom.append(" HAVING ").append(map.get("asName")).append(" < ").append(map.get(CommonConstant.DISTANCE));
            }
        }

        sbCount.append(sbCom);

        // 总数
        aniQuery.setSqlCount(sbCount.toString());

        sb.append(sbCom);

        //分组
        sb.append(buildGroupSql(paramCondition.getGroup(), associatedQuery == null ? null : baseMySqlTableInfo.getTableName()));

        // 排序
        sb.append(buildOrderSql(paramCondition.getOrder(), associatedQuery == null ? null : baseMySqlTableInfo.getTableName()));

        // 分页
        sb.append(buildLimitSql(paramCondition.getPage()));

        // 详细数据
        aniQuery.setSql(sb.toString());

        // 设置分页信息
        aniQuery.setLimit(paramCondition.getPage().getLimit());
        aniQuery.setOffset(paramCondition.getPage().getOffset());

        return aniQuery;
    }

    /**
     * 解析 建表 SQL
     *
     * @return
     */
    public AniHandler.AniCreateTable parserCreateTableSql(CreateTableParamCondition createTableParamCondition, BaseMySqlDatabaseInfo baseMySqlDatabaseInfo, BaseMySqlTableInfo baseMySqlTableInfo) {
        AniHandler.AniCreateTable createTable = new AniHandler.AniCreateTable();

        // 解析 LIKE 语法
        CreateTableLikeParam createTableLike = createTableParamCondition.getCreateTableLike();
        if (createTableLike != null) {
            String copyTableName = createTableLike.getCopyTableName();
            if (StringUtils.isNotBlank(copyTableName)) {
                createTable.setSql(String.format(CREATE_TABLE_LIKE_TEMPLATE, baseMySqlTableInfo.getTableName(), copyTableName));
                createTable.setNotExists(createTableParamCondition.isNotExists());
                return createTable;
            }
        }

        List<CreateTableParamCondition.CreateTableFieldParam> createTableFieldParamList = createTableParamCondition.getCreateTableFieldParamList();
        StringBuilder createSqlBuilder = new StringBuilder();
        List<String> distributed = new ArrayList<>();
        // 字段解析
        parseColumns(createTableFieldParamList, createSqlBuilder, distributed);
        String tableComment = createTableParamCondition.getTableComment();

        // 建表SQL
        String sql = String.format(CREATE_TABLE_TEMPLATE, baseMySqlTableInfo.getTableName(), createSqlBuilder.toString(), StringUtils.isNotBlank(tableComment) ? tableComment : "", baseMySqlTableInfo.getEngine());

        // 自增配置
        for (CreateTableParamCondition.CreateTableFieldParam createTableFieldParam : createTableFieldParamList) {
            if(createTableFieldParam.isAutoIncrement()) {
                Long startIncrement = createTableFieldParam.getStartIncrement();
                if(startIncrement > 1) {
                    sql = sql + " AUTO_INCREMENT = " + startIncrement;
                    break;
                }
            }
        }
        createTable.setSql(sql);
        createTable.setNotExists(createTableParamCondition.isNotExists());
        return createTable;
    }

    /**
     * 解析字段
     *
     * @param createTableFieldParamList
     * @param createSqlBuilder
     * @param distributes
     */
    protected void parseColumns(List<CreateTableParamCondition.CreateTableFieldParam> createTableFieldParamList, StringBuilder createSqlBuilder, List<String> distributes) {
        // 存放主键
        List<String> primaryKeyList = new LinkedList<>();
        createTableFieldParamList.forEach(createTableFieldParam -> {
            String fieldType = createTableFieldParam.getFieldType();
            String fieldComment = createTableFieldParam.getFieldComment();
            Integer fieldLength = createTableFieldParam.getFieldLength();
            String fieldName = createTableFieldParam.getFieldName();
            Integer fieldDecimalPoint = createTableFieldParam.getFieldDecimalPoint();
            String onUpdate = createTableFieldParam.getOnUpdate();
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

            String defaultValue = createTableFieldParam.getDefaultValue();
            if (createTableFieldParam.isNotNull()) {
                createSqlBuilder.append(" NOT NULL");
            } else if(defaultValue == null && !createTableFieldParam.isAutoIncrement() && !createTableFieldParam.isPrimaryKey()){
                createSqlBuilder.append(" NULL");
            }

            if (defaultValue != null) {
                createSqlBuilder.append(" ").append(buildDefaultValue(fieldType, defaultValue));
            }

            if(StringUtils.isNotBlank(onUpdate)) {
                createSqlBuilder.append(" ON UPDATE ").append(onUpdate);
            }

            if (createTableFieldParam.isAutoIncrement()) {
                createSqlBuilder.append(" AUTO_INCREMENT");
            }

            if (StringUtils.isNotBlank(fieldComment)) {
                createSqlBuilder.append(" COMMENT ")
                        .append("'")
                        .append(fieldComment)
                        .append("'");
            }
            createSqlBuilder.append(",");
            if (createTableFieldParam.isPrimaryKey()) {
                primaryKeyList.add(fieldName);
            }
            if (createTableFieldParam.isDistributed()) {
                distributes.add(fieldName);
            }
        });
        createSqlBuilder.deleteCharAt(createSqlBuilder.length() - 1);
        if (CollectionUtils.isNotEmpty(primaryKeyList)) {
            createSqlBuilder.append(",PRIMARY KEY (");
            primaryKeyList.forEach(primaryKey -> {
                createSqlBuilder.append(DOUBLE_QUOTATION_MARKS)
                        .append(primaryKey)
                        .append(DOUBLE_QUOTATION_MARKS)
                        .append(",");
            });
            createSqlBuilder.deleteCharAt(createSqlBuilder.length() - 1);
            createSqlBuilder.append(")");
        }
    }

    /**
     * 解析 数据插入语句
     *
     * @param addParamCondition
     * @return
     */
    public AniHandler.AniInsert parserInsertSql(AddParamCondition addParamCondition, BaseMySqlDatabaseInfo baseDmDatabaseInfo, BaseMySqlTableInfo baseDmTableInfo) {
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
            fieldBuilder.append(DOUBLE_QUOTATION_MARKS)
                    .append(key)
                    .append(DOUBLE_QUOTATION_MARKS)
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

        String insertSql = String.format(INSERT_DATA_TEMPLATE, baseDmTableInfo.getTableName(), fieldBuilder.toString(), insertSb.toString());

        aniInsert.setSql(insertSql);

        aniInsert.setParamArray(paramList.toArray());

        aniInsert.setReturnGeneratedKey(addParamCondition.isReturnGeneratedKey());

        aniInsert.setReturnFields(addParamCondition.getReturnFields());

        return aniInsert;
    }

    /**
     * 解析 数据删除语句
     *
     * @param delParamCondition
     * @return
     */
    public AniHandler.AniDel parserDelSql(DelParamCondition delParamCondition, BaseMySqlDatabaseInfo baseMySqlDatabaseInfo, BaseMySqlTableInfo baseMySqlTableInfo) {
        AniHandler.AniDel aniDel = new AniHandler.AniDel();
        List<Where> where = delParamCondition.getWhere();
        if (CollectionUtils.isNotEmpty(where)) {
            List<Object> paramList = new LinkedList<>();
            String whereSql = buildWhereSql(where, paramList, null, null);
            String delSql = String.format(DELETE_DATA_TEMPLATE, baseMySqlTableInfo.getTableName(), whereSql);
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
    public AniHandler.AniCreateIndex parserCreateIndexSql(IndexParamCondition indexParamCondition, BaseMySqlDatabaseInfo databaseInfo, BaseMySqlTableInfo tableInfo) {
        AniHandler.AniCreateIndex aniCreateIndex = new AniHandler.AniCreateIndex();

        List<IndexParamCondition.IndexColumn> columns = indexParamCondition.getColumns();

        List<String> indexFields = new ArrayList<>();

        List<String> indexNameByFields = new ArrayList<>();

        if (CollectionUtil.isNotEmpty(columns)) {
            for (IndexParamCondition.IndexColumn column : columns) {
                String indexColumn = DOUBLE_QUOTATION_MARKS + column.getColumn() + DOUBLE_QUOTATION_MARKS;
                Sort sort = column.getSort();
                if (sort != null) {
                    indexColumn = indexColumn + " " + sort.name();
                }
                indexFields.add(indexColumn);
                indexNameByFields.add(column.getColumn());
            }
        } else {
            String column = indexParamCondition.getColumn();
            String indexColumn = DOUBLE_QUOTATION_MARKS + column + DOUBLE_QUOTATION_MARKS;
            String sort = indexParamCondition.getSort();
            if (StringUtils.isNotBlank(sort)) {
                if (StringUtils.equalsIgnoreCase(sort, "-1")) {
                    indexColumn = indexColumn + " " + "DESC";
                } else {
                    indexColumn = indexColumn + " " + "ASC";
                }
            }
            indexFields.add(indexColumn);
            indexNameByFields.add(column);
        }

        String indexName = indexParamCondition.getIndexName();
        if (StringUtils.isBlank(indexName)) {
            indexName = "index_" + tableInfo.getTableName() + "_" + CollectionUtil.join(indexNameByFields, "_");
        }

        String indexType = "";
        IndexType type = indexParamCondition.getIndexType();
        if (type != null) {
            switch (type) {
                case UNIQUE:
                    indexType = type.name();
                    break;
            }
        }

        Boolean isNotExists = indexParamCondition.getIsNotExists();

        aniCreateIndex.setSql(String.format(CREATE_INDEX_TEMPLATE, indexType, indexName, tableInfo.getTableName(), CollectionUtil.join(indexFields, ",")));

        if (isNotExists != null && isNotExists) {
            aniCreateIndex.setNotExists(true);
        } else {
            aniCreateIndex.setNotExists(false);
        }

        aniCreateIndex.setIndexName(indexName);

        return aniCreateIndex;

    }

    /**
     * 解析 创建索引语句
     *
     * @param indexParamCondition
     * @return
     */
    public AniHandler.AniDropIndex parserDropIndexSql(IndexParamCondition indexParamCondition, BaseMySqlDatabaseInfo databaseInfo, BaseMySqlTableInfo tableInfo) {
        String indexName = indexParamCondition.getIndexName();

        if (StringUtils.isBlank(indexName)) {
            throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "删除索引必须指定 indexName 索引名称!");
        }

        AniHandler.AniDropIndex aniDropIndex = new AniHandler.AniDropIndex();
        aniDropIndex.setSql(String.format(DROP_INDEX_TEMPLATE, indexName, tableInfo.getTableName()));
        return aniDropIndex;
    }

    /**
     * 解析 数据更新语句
     *
     * @param updateParamCondition
     * @return
     */
    public AniHandler.AniUpdate parserUpdateSql(UpdateParamCondition updateParamCondition, BaseMySqlDatabaseInfo baseMySqlDatabaseInfo, BaseMySqlTableInfo baseMySqlTableInfo) {
        AniHandler.AniUpdate aniUpdate = new AniHandler.AniUpdate();
        Map<String, Object> updateParamMap = updateParamCondition.getUpdateParamMap();
        UpdateParamCondition.ArrayProcessMode arrayProcessMode = updateParamCondition.getArrayProcessMode();
        StringBuilder updateSb = new StringBuilder();
        List<StringBuilder> updateForArraySbList = new ArrayList<>();
        // 占位符参数
        List<Object> paramList = new ArrayList<>();
        List<List<Object>> subParamList = new ArrayList<>();
        updateParamMap.forEach((key, value) -> {
            if (value != null
                    && (value instanceof List || (value.getClass().isArray() && !(value instanceof byte[])))
                    && !UpdateParamCondition.ArrayProcessMode.COVER.equals(arrayProcessMode)) {
                if (value.getClass().isArray()) {
                    value = CollectionUtil.newArrayList(ArrayUtil.wrap(value));
                }
                List v = (List) value;
                if (UpdateParamCondition.ArrayProcessMode.ADD_TO_SET.equals(arrayProcessMode)) {
                    if (v.size() == 1) {
                        arrayInUpdate(updateSb, key, value, UPDATE_ARRAY_SET_TEMPLATE);
                        updateSb.append(",");
                    } else {
                        for (int i = 0; i < v.size(); i++) {
                            Object o = v.get(i);
                            StringBuilder currentSb;
                            if (i == 0) {
                                currentSb = updateSb;
                            } else {
                                currentSb = new StringBuilder();
                                updateForArraySbList.add(currentSb);
                                List<Object> subParam = new ArrayList<>();
                                subParamList.add(subParam);
                            }
                            arrayInUpdate(currentSb, key, CollectionUtil.newArrayList(o), UPDATE_ARRAY_SET_TEMPLATE);
                            if (i == 0) {
                                currentSb.append(",");
                            }
                        }
                    }
                } else {
                    arrayInUpdate(updateSb, key, value, UPDATE_ARRAY_LIST_TEMPLATE);
                    updateSb.append(",");
                }
            } else {
                updateSb.append(DOUBLE_QUOTATION_MARKS)
                        .append(key)
                        .append(DOUBLE_QUOTATION_MARKS)
                        .append(" = ");
                Object o = translateValue(value);
                if (value instanceof Point) {
                    updateSb.append(o);
                } else {
                    paramList.add(o);
                    updateSb.append("?");
                }
                updateSb.append(",");
            }
        });
        updateSb.deleteCharAt(updateSb.length() - 1);
        List<Where> where = updateParamCondition.getWhere();
        List<Object> whereParams = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(where)) {
            String whereSql = buildWhereSql(where, whereParams, null, null);
            updateSb.append(" ").append(whereSql);
            if (CollectionUtil.isNotEmpty(updateForArraySbList)) {
                for (StringBuilder builder : updateForArraySbList) {
                    builder.append(" ").append(whereSql);
                }
            }
        }
        List<Object[]> subParamArray = new ArrayList<>(subParamList.size());

        if (CollectionUtil.isNotEmpty(whereParams)) {
            paramList.addAll(whereParams);
            if (CollectionUtil.isNotEmpty(updateForArraySbList)) {
                for (List<Object> subParam : subParamList) {
                    subParam.addAll(whereParams);
                    subParamArray.add(subParam.toArray());
                }
            }
        }
        aniUpdate.setParamArray(paramList.toArray());
        aniUpdate.setSubParamArray(subParamArray);
        aniUpdate.setSql(String.format(UPDATE_DATA_TEMPLATE, baseMySqlTableInfo.getTableName(), updateSb.toString()));
        if (CollectionUtil.isNotEmpty(updateForArraySbList)) {
            List<String> subSQL = new ArrayList<>();
            for (int i = 0; i < updateForArraySbList.size(); i++) {
                StringBuilder builder = updateForArraySbList.get(i);
                subSQL.add(String.format(UPDATE_DATA_TEMPLATE, baseMySqlTableInfo.getTableName(), builder.toString()));
            }
            aniUpdate.setSubSQL(subSQL);
        }
        return aniUpdate;
    }

    /**
     * 数组更新拼接
     * @param updateSb
     * @param field
     * @param value
     * @param template
     */
    protected void arrayInUpdate(StringBuilder updateSb, String field, Object value, String template) {
        updateSb.append(DOUBLE_QUOTATION_MARKS)
                .append(field)
                .append(DOUBLE_QUOTATION_MARKS)
                .append(" = ")
                .append(StringUtils.replaceEach(template, new String[]{"$column", "$value"}, new String[]{DOUBLE_QUOTATION_MARKS + field + DOUBLE_QUOTATION_MARKS, JsonUtil.objectToStr(value)}));
    }

    protected Object translateValue(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Date) {
            return DateUtil.format((Date) value, DatePattern.NORM_DATETIME_PATTERN);
        } else if (value instanceof Timestamp) {
            return DateUtil.format((Timestamp) value, DatePattern.NORM_DATETIME_PATTERN);
        } else if (value instanceof Point) {
            Point point = (Point) value;
            return String.format(getPointTemplate(), point.getLon(), point.getLat());
        } else if (value instanceof Boolean || value instanceof Integer || value instanceof Double || value instanceof Long || value instanceof Float) {
            return value;
        } else if (value instanceof List || (value.getClass().isArray() && !(value instanceof byte[])) || value instanceof Map) {
            return JsonUtil.objectToStr(value);
        }else {
            return value;
        }
    }

    protected String getPointTemplate() {
        return POINT_TEMPLATE;
    }

    /**
     * 构造 SELECT
     *
     * @param selects
     * @return
     */
    protected String buildSelectSql(List<String> selects, String tableName) {
        StringBuilder sb = new StringBuilder();
        if (selects != null && !selects.isEmpty()) {
            for (String select : selects) {
                if (StringUtils.isBlank(select)) {
                    continue;
                }
                String checkMethod = checkMethod(select, tableName);
                if (StringUtils.isBlank(checkMethod)) {
                    String checkAlias = checkAlias(select, DOUBLE_QUOTATION_MARKS, tableName);
                    if (StringUtils.isBlank(checkAlias)) {
                        if ("*".equalsIgnoreCase(select)) {
                            if (StringUtils.isNotBlank(tableName)) {
                                sb.append(tableName).append(".");
                            }
                            sb.append(select);
                        } else {
                            if (StringUtils.isNotBlank(tableName)) {
                                sb.append(tableName).append(".");
                            }
                            sb.append(DOUBLE_QUOTATION_MARKS);
                            sb.append(select);
                            sb.append(DOUBLE_QUOTATION_MARKS);
                        }
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
            sb.append(" ");
            if (StringUtils.isNotBlank(tableName)) {
                sb.append(tableName).append(".");
            }
            sb.append("*");
            sb.append(" ");
        }
        return sb.toString();
    }

    /**
     * 验证查询字段是否带有函数
     *
     * @param select
     * @return
     */
    private String checkMethod(String select, String tableName) {
        boolean flag = StringUtils.startsWithIgnoreCase(select, "f(");
        if (flag) {
            select = select.substring(2, select.lastIndexOf(")"));
            String methodName = select.substring(0, select.indexOf("("));
            String parse = Method.parse(methodName.toLowerCase()).getName();
            String field = select.substring(select.indexOf("(") + 1, select.lastIndexOf(")"));
            StringBuffer stringBuffer = new StringBuffer();
            stringBuffer.append(parse).append("(");
            if (StringUtils.isNotBlank(tableName)) {
                stringBuffer.append(tableName).append(".");
            }
            stringBuffer.append(DOUBLE_QUOTATION_MARKS).append(field).append(DOUBLE_QUOTATION_MARKS).append(") AS ").append(DOUBLE_QUOTATION_MARKS)
                    .append(parse)
                    .append("(")
                    .append(field)
                    .append(")")
                    .append(DOUBLE_QUOTATION_MARKS);
            return stringBuffer.toString();
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
    protected String buildWhereSql(List<Where> wheres, List<Object> paramList, List<Map<String, Object>> geoDistanceList, String tableName) {
        String sql = buildWhereSql(wheres, Rel.AND, paramList, geoDistanceList, tableName);
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
                } else {
                    String whereSql = buildWhereSql(where, rel, paramList, geoDistanceList, tableName);
                    sb.append(whereSql);
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
    protected String buildWhereSql(Where where, Rel rel, List<Object> paramList, List<Map<String, Object>> geoDistanceList, String tableName) {
        StringBuilder sb = new StringBuilder();
        if (whereHandler(where)) {
            String relStrHandler = relStrHandler(where, paramList, geoDistanceList, tableName);
            if (StringUtils.isNotBlank(StringUtils.trimToEmpty(relStrHandler))) {
                sb.append(relStrHandler);
                sb.append(" ");
                sb.append(rel.getName());
                sb.append(" ");
            }
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
     * @param paramList
     * @return
     */
    protected String relStrHandler(Where where, List<Object> paramList, List<Map<String, Object>> geoDistanceList, String tableName) {
        Rel rel = where.getType();
        StringBuilder sb = new StringBuilder();
        if (StringUtils.isNotBlank(tableName)) {
            sb.append(tableName);
        }
        sb.append(where.getField());
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
            case MATCH:
                sb.append(" like ");
                sb.append("?");
                paramList.add("%" + where.getParam() + "%");
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
            case ONLY_LIKE:
                sb.append(" like ");
                sb.append(" ? ");
                paramList.add(where.getParam());
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
            default:
                sb.append(" = ");
                break;
        }
        return sb.toString();
    }

    protected void geoDistanceQuery(Where where, List<Map<String, Object>> geoDistanceList, StringBuilder sb) {
        if (geoDistanceList == null) {
            throw new BusinessException("当前Mysql 操作方法不允许使用范围查询语法");
        }
        if (where.getParam() instanceof Map) {
            // 坐标查询不放在 where 中，去掉前面 append 添加的字段名
            sb.delete(sb.lastIndexOf(where.getField()), sb.length());
            Map<String, Object> paramMap = (Map) where.getParam();
            String loc = ObjectUtils.toString(paramMap.get(CommonConstant.LOC));
            String[] split = loc.split(CommonConstant.COMMA);
            double x = Double.valueOf(split[0]);
            double y = Double.valueOf(split[1]);
            Map<String, Object> geoInfo = new HashMap<>(4);
            geoDistanceList.add(geoInfo);
            geoInfo.put("fieldName", where.getField());
            geoInfo.put(CommonConstant.LOC, x);
            geoInfo.put(CommonConstant.LAT, y);
            geoInfo.put(CommonConstant.DISTANCE, DistanceUnitEnum.kmToM(paramMap.get(CommonConstant.DISTANCE)));
            String asName = CommonConstant.DISTANCE;
            if (geoDistanceList.size() > 1) {
                asName = CommonConstant.DISTANCE + geoDistanceList.size();
            }
            geoInfo.put("asName", asName);
        } else {
            log.error("GEO_DISTANCE的参数无法解析:" + where.getParam());
        }
    }

    /**
     * 解析分页
     *
     * @param page
     * @return
     */
    protected String buildLimitSql(Page page) {
        if (page != null && page.getLimit().equals(Page.LIMIT_ALL_DATA)) {
            return "";
        }
        page = RelDbUtil.pageHandler(page);
        StringBuilder sb = new StringBuilder();
        sb.append(" limit ");
        if (page.getOffset() > 0) {
            sb.append(page.getOffset())
                    .append(",");
        }
        sb.append(page.getLimit());
        return sb.toString();
    }


    /**
     * 分组SQL
     *
     * @param groupList
     * @return
     */
    protected String buildGroupSql(List<String> groupList, String tableName) {
        StringBuilder sb = new StringBuilder();
        if (groupList != null && !groupList.isEmpty()) {
            sb.append(" GROUP BY ");
            for (int i = 0; i < groupList.size(); i++) {
                String group = groupList.get(i);
                if (StringUtils.isNotBlank(tableName)) {
                    sb.append(tableName).append(".");
                }
                sb.append(group);
                if (i < groupList.size() - 1) {
                    sb.append(",");
                }
            }
        }
        return sb.toString();
    }

    /**
     * 排序SQL
     *
     * @param orders
     * @return
     */
    protected String buildOrderSql(List<Order> orders, String tableName) {
        StringBuilder sb = new StringBuilder();
        if (orders != null && !orders.isEmpty()) {
            StringBuilder sb1 = new StringBuilder();
            for (Order order : orders) {
                if (!RelDbUtil.orderHandler(order)) {
                    continue;
                }

                String field = "";

                if (StringUtils.isNotBlank(tableName)) {
                    field = tableName + ".";
                }
                field = field + order.getField();

                if(order.getNullSort() != null) {
                    if(order.getNullSort() == Sort.ASC) {
                        sb1.append("IF(ISNULL(" + field + "), 0, 1), ");
                    }else {
                        sb1.append("IF(ISNULL(" + field + "), 0, 1) DESC, ");
                    }
                }

                sb1.append(field);
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

    private String checkAlias(String select, String marks, String tableName) {
        if (StringUtils.isBlank(select)) {
            return null;
        }
        boolean flag = StringUtils.startsWithIgnoreCase(select, "as(");
        if (flag) {
            select = select.substring(3, select.lastIndexOf(")"));
            String[] split = select.split(",");
            String field;
            String alias;
            if (split.length == 1) {
                field = alias = StringUtils.trim(split[0]);
            } else {
                field = StringUtils.trim(split[0]);
                alias = StringUtils.trim(split[1]);
            }
            StringBuffer stringBuffer = new StringBuffer();
            if (StringUtils.isNotBlank(tableName)) {
                stringBuffer.append(tableName).append(".");
            }
            stringBuffer.append(marks).append(field)
                    .append(marks)
                    .append(" as ")
                    .append(marks)
                    .append(alias)
                    .append(marks);
            return stringBuffer.toString();
        } else {
            return null;
        }
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

    protected List<AniHandler.AlterTableSqlInfo> buildAlterTableAddColumn(AlterTableParamCondition.AddTableFieldParam addTableFieldParam, BaseMySqlDatabaseInfo baseMySqlDatabaseInfo, BaseMySqlTableInfo baseMySqlTableInfo){

        //例子： alter table `person` add column `desc` VARCHAR(255) not null default '' comment '描述'
        String fieldType = addTableFieldParam.getFieldType();
        String fieldComment = addTableFieldParam.getFieldComment();
        Integer fieldLength = addTableFieldParam.getFieldLength();
        Integer fieldDecimalPoint = addTableFieldParam.getFieldDecimalPoint();
        String fieldName = addTableFieldParam.getFieldName();
        String defaultValue = addTableFieldParam.getDefaultValue();
        String onUpdate = addTableFieldParam.getOnUpdate();
        boolean isNotExists = addTableFieldParam.isNotExists();
        if(StringUtils.isBlank(fieldType)) {
            throw new RuntimeException("字段类型不能为空");
        }

        StringBuilder sb = new StringBuilder();

        FieldTypeAdapter adapter = getAdapter(fieldType);
        //基本语句
        sb.append("ALTER TABLE `").append(baseMySqlTableInfo.getTableName()).append("` ADD COLUMN ")
                .append("`").append(fieldName).append("` ")
                .append(buildSqlDataType(adapter,  fieldLength, fieldDecimalPoint));

        if (adapter.isUnsigned()) {
            if (addTableFieldParam.getUnsigned() != null && addTableFieldParam.getUnsigned()) {
                sb.append(" UNSIGNED");
            }
        }

        //约束条件[不为空]
        Boolean notNull = addTableFieldParam.getNotNull();
        if (notNull != null) {
            if(notNull) {
                sb.append(" NOT NULL");
            }else {
                sb.append(" NULL");
            }
        }

        //约束条件[默认值]
        if(defaultValue != null) {
            sb.append(" ").append(buildDefaultValue(fieldType, defaultValue));
        }

        if(StringUtils.isNotBlank(onUpdate)) {
            sb.append(" ON UPDATE ").append(onUpdate);
        }

        //注释
        if(fieldComment != null) {
            sb.append(" COMMENT").append("'").append(fieldComment).append("'");
        }

        List<AniHandler.AlterTableSqlInfo> sqlList = new ArrayList<>(2);
        sqlList.add(alterTableSqlInfoBuild(sb.toString(), fieldName, isNotExists));



        if(addTableFieldParam.isPrimaryKey()) {
            sqlList.add(alterTableSqlInfoBuild(String.format(ADD_PRIMARY_KEY, baseMySqlTableInfo.getTableName(), fieldName), fieldName, false));
        }

        return sqlList;
    }

    private AniHandler.AlterTableSqlInfo alterTableSqlInfoBuild(String sql, String columnName, boolean isNotExists) {
        AniHandler.AlterTableSqlInfo alterTableSqlInfo = new AniHandler.AlterTableSqlInfo();
        alterTableSqlInfo.setSql(sql);
        alterTableSqlInfo.setColumnName(columnName);
        alterTableSqlInfo.setNotExists(isNotExists);
        return alterTableSqlInfo;
    }


    protected List<AniHandler.AlterTableSqlInfo> buildAlterTableModifyColumn(AlterTableParamCondition.UpdateTableFieldParam updateTableFieldParam, BaseMySqlDatabaseInfo baseMySqlDatabaseInfo, BaseMySqlTableInfo baseMySqlTableInfo) {

        List<AniHandler.AlterTableSqlInfo> sqlList = new ArrayList<>(2);

        String fieldName = updateTableFieldParam.getFieldName();
        String newFieldName = updateTableFieldParam.getNewFieldName();

        //修改字段名
        if(StringUtils.isNotBlank(newFieldName)) {
            String sql = String.format(getUpdateTableFieldNameSQL(), baseMySqlTableInfo.getTableName(), fieldName, newFieldName, fieldName, fieldName, fieldName, fieldName);
            AniHandler.AlterTableSqlInfo alterTableSqlInfo = alterTableSqlInfoBuild(sql, fieldName, false);
            sqlList.add(alterTableSqlInfo);
            return sqlList;
        }

        //例子： alter table `person` modify column `desc` VARCHAR(255) not null default '' comment '描述'
        String fieldType = updateTableFieldParam.getFieldType();
        String fieldComment = updateTableFieldParam.getFieldComment();
        Integer fieldLength = updateTableFieldParam.getFieldLength();
        Integer fieldDecimalPoint = updateTableFieldParam.getFieldDecimalPoint();
        String defaultValue = updateTableFieldParam.getDefaultValue();
        String onUpdate = updateTableFieldParam.getOnUpdate();
        boolean dropFlag = updateTableFieldParam.isDropFlag();
        if(dropFlag) {
            boolean dropDefaultValue = updateTableFieldParam.isDropDefaultValue();
            boolean dropNotNull = updateTableFieldParam.isDropNotNull();
            if(dropDefaultValue) {
                String sql = String.format(DROP_COLUMN_CONSTRAINT, baseMySqlTableInfo.getTableName(), fieldName, "DEFAULT");
                sqlList.add(alterTableSqlInfoBuild(sql, fieldName, false));
            }
            if(dropNotNull) {
                String sql = String.format(DROP_COLUMN_CONSTRAINT, baseMySqlTableInfo.getTableName(), fieldName, "NOT NULL");
                sqlList.add(alterTableSqlInfoBuild(sql, fieldName, false));
            }
            return sqlList;
        }

        StringBuilder sb = new StringBuilder();

        //基本语句
        sb.append("ALTER TABLE `").append(baseMySqlTableInfo.getTableName()).append("` MODIFY COLUMN ")
                .append("`").append(fieldName).append("` ");

        if(StringUtils.isBlank(fieldType)) {
            sb.append("${dataType_"+fieldName+"}");
        }else{
            sb .append(buildSqlDataType(getAdapter(fieldType),  fieldLength, fieldDecimalPoint));
        }

        if (getAdapter(fieldType).isUnsigned()) {
            if (updateTableFieldParam.getUnsigned() != null && updateTableFieldParam.getUnsigned()) {
                sb.append(" UNSIGNED");
            }
        }

        //约束条件[不为空]
        Boolean notNull = updateTableFieldParam.getNotNull();
        if (notNull == null) {
            sb.append(" ${notNull_"+fieldName+"}");
        }else {
            if(notNull) {
                sb.append(" NOT NULL");
            }else {
                sb.append(" NULL");
            }
        }

        //约束条件[默认值]
        if(defaultValue != null) {
            sb.append(" ").append(buildDefaultValue(fieldType, defaultValue));
        }else {
            sb.append(" ${DEFAULT_"+fieldName+"}");
        }

        if(StringUtils.isNotBlank(onUpdate)) {
            sb.append(" ON UPDATE ").append(onUpdate);
        }

        //注释
        if(fieldComment != null) {
            sb.append(" COMMENT").append("'").append(fieldComment).append("'");
        }else {
            sb.append(" ${COMMENT_"+fieldName+"}");
        }


        sqlList.add(alterTableSqlInfoBuild(sb.toString(), fieldName, false));


        if(updateTableFieldParam.isPrimaryKey()) {
            String sql = String.format(ADD_PRIMARY_KEY, baseMySqlTableInfo.getTableName(), fieldName);
            sqlList.add(alterTableSqlInfoBuild(sql, fieldName, false));
        }

        return sqlList;
    }

    protected String buildDefaultValue(String fieldType, String value) {

        if(value.equalsIgnoreCase("null")) return "DEFAULT NULL";

        if(StringUtils.isBlank(fieldType)) {
            return "DEFAULT '" + value + "'";
        }else {

            if((fieldType.equalsIgnoreCase("String")
                    || fieldType.equalsIgnoreCase("Char"))) {
                return "DEFAULT '" + value + "'";
            }else {
                return "DEFAULT " + value;
            }

        }
    }

    /**
     * 解析 修改表 SQL
     *
     * @return
     */
    public AniHandler.AniAlterTable parserAlterTableSql(AlterTableParamCondition alterTableParamCondition, BaseMySqlDatabaseInfo baseDatabaseInfo, BaseMySqlTableInfo baseTableInfo) {
        List<AniHandler.AlterTableSqlInfo> sqlList = new ArrayList<>();
        AniHandler.AniAlterTable aniAlterTable = new AniHandler.AniAlterTable();
        List<String> delTableFieldParamList = alterTableParamCondition.getDelTableFieldParamList();
        List<AlterTableParamCondition.AddTableFieldParam> addTableFieldParamList = alterTableParamCondition.getAddTableFieldParamList();
        List<AlterTableParamCondition.UpdateTableFieldParam> updateTableFieldParamList = alterTableParamCondition.getUpdateTableFieldParamList();
        List<AlterTableParamCondition.ForeignParam> addForeignParamList = alterTableParamCondition.getAddForeignParamList();
        List<AlterTableParamCondition.ForeignParam> delForeignParamList = alterTableParamCondition.getDelForeignParamList();
        String newTableName = alterTableParamCondition.getNewTableName();
        String tableComment = alterTableParamCondition.getTableComment();

        // 构造删除字段sql
        if(CollectionUtils.isNotEmpty(delTableFieldParamList)){
            delTableFieldParamList.forEach(element->{
                String sql = String.format(getDropTableFieldSQL(), baseTableInfo.getTableName(), element);
                AniHandler.AlterTableSqlInfo alterTableSqlInfo = alterTableSqlInfoBuild(sql, element, false);
                sqlList.add(alterTableSqlInfo);
            });
        }

        // 构造新增字段sql
        addTableFieldParamList.forEach(addTableFieldParam -> {
            sqlList.addAll(buildAlterTableAddColumn(addTableFieldParam, baseDatabaseInfo, baseTableInfo));
        });

        // 构造修改字段sql
        updateTableFieldParamList.forEach(updateTableFieldParam -> {
            sqlList.addAll(buildAlterTableModifyColumn(updateTableFieldParam, baseDatabaseInfo, baseTableInfo));
        });

        //新增外键
        if(CollectionUtils.isNotEmpty(addForeignParamList)){
            addForeignParamList.forEach(element->{
                String foreignName = element.getForeignName();
                String foreignKey = element.getForeignKey();
                String referencesTableName = element.getReferencesTableName();
                String referencesField = element.getReferencesField();
                ForeignKeyActionEnum onDeletion = element.getOnDeletion();
                ForeignKeyActionEnum onUpdate = element.getOnUpdate();
                if(onDeletion == null) {
                    onDeletion = ForeignKeyActionEnum.RESTRICT;
                }
                if(onUpdate == null) {
                    onUpdate = ForeignKeyActionEnum.RESTRICT;
                }
                if(StringUtils.isAnyBlank(foreignKey, referencesTableName, referencesField)) {
                    throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION,
                            "参数foreignKey, referencesTableName, referencesField不能为空");
                }
                if(StringUtils.isBlank(foreignName)) {
                    foreignName = foreignKey + "_to_" + referencesTableName + "_" + referencesField;
                }

                String sql = String.format(
                        getAddForeignKeySql(),
                        baseTableInfo.getTableName(),
                        foreignName, foreignKey, referencesTableName, referencesField, onDeletion.val, onUpdate.val);

                sqlList.add(alterTableSqlInfoBuild(sql, foreignKey, false));
            });
        }
        //删除外键
        if(CollectionUtils.isNotEmpty(delForeignParamList)){
            delForeignParamList.forEach(element->{

                String foreignName = element.getForeignName();
                String foreignKey = element.getForeignKey();
                String referencesTableName = element.getReferencesTableName();
                String referencesField = element.getReferencesField();

                if(StringUtils.isBlank(foreignName)) {
                    if(StringUtils.isAnyBlank(foreignKey, referencesTableName, referencesField)) {
                        throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION,
                                "参数foreignKey, referencesTableName, referencesField不能为空或foreignName不能为空");
                    }
                    foreignName = foreignKey + "_to_" + referencesTableName + "_" + referencesField;
                }
                String sql = String.format(DROP_FOREIGN_KEY, baseTableInfo.getTableName(), foreignName);
                sqlList.add(alterTableSqlInfoBuild(sql, foreignName, false));
            });
        }

        if(alterTableParamCondition.isDelPrimaryKey()) {
            String sql = String.format(DROP_PRIMARY_KEY, baseTableInfo.getTableName());
            sqlList.add(alterTableSqlInfoBuild(sql, null, false));
        }

        if(StringUtils.isNotBlank(newTableName)) {
            String sql = String.format(getUpdateTableNameSQL(), baseTableInfo.getTableName(), newTableName);
            sqlList.add(alterTableSqlInfoBuild(sql, null, false));
        }

        if(StringUtils.isNotBlank(tableComment)) {
            String sql = String.format(ALTER_TABLE_COMMENT, baseTableInfo.getTableName(), tableComment);
            sqlList.add(alterTableSqlInfoBuild(sql, null, false));
        }

        aniAlterTable.setAlterTableSqlInfos(sqlList);

        return aniAlterTable;
    }

    protected String getAddForeignKeySql() {
        return ADD_FOREIGN_KEY;
    }

    /**
     * upsert SQL
     *
     * @param upsertParamCondition
     * @return
     */
    public AniHandler.AniUpsert parserUpsertSql(UpsertParamCondition upsertParamCondition, BaseMySqlDatabaseInfo baseMySqlDatabaseInfo, BaseMySqlTableInfo baseMySqlTableInfo) {
        AniHandler.AniUpsert aniUpsert = new AniHandler.AniUpsert();
        Map<String, Object> upsertParamMap = upsertParamCondition.getUpsertParamMap();
        Map<String, Object> updateParamMap = upsertParamCondition.getUpdateParamMap();
        StringBuilder insertFieldSb = new StringBuilder();
        StringBuilder insertValueSb = new StringBuilder();
        StringBuilder updateSb = new StringBuilder();
        Object[] paramArray = new Object[upsertParamMap.size()];
        int index = 0;
        int updateIndex = 0;
        boolean updateFlag = MapUtil.isEmpty(updateParamMap);
        Object[] updateParamArray;
        if (updateFlag) {
            updateParamArray = new Object[upsertParamMap.size()];
        } else {
            updateParamArray = new Object[updateParamMap.size()];
        }
        insertValueSb.append("(");
        for (Map.Entry<String, Object> entry : upsertParamMap.entrySet()) {
            insertFieldSb.append(entry.getKey()).append(",");
            Object o = translateValue(entry.getValue());
            if (entry.getValue() instanceof Point) {
                // 地理位置坐标
                insertValueSb.append(o);
            } else {
                paramArray[index++] = o;
                insertValueSb.append("?");
            }
            insertValueSb.append(",");
            if (updateFlag) {
                updateSb.append(entry.getKey()).append(" = ");
                if (entry.getValue() instanceof Point) {
                    // 地理位置坐标
                    updateSb.append(o);
                } else {
                    updateParamArray[updateIndex++] = o;
                    updateSb.append("?");
                }
                updateSb.append(",");
            }
        }
        insertValueSb = insertValueSb.deleteCharAt(insertValueSb.length() - 1);
        insertValueSb.append(")");
        if (!updateFlag) {
            for (Map.Entry<String, Object> entry : updateParamMap.entrySet()) {
                updateSb.append(entry.getKey()).append(" = ");
                Object o = translateValue(entry.getValue());
                if (entry.getValue() instanceof Point) {
                    updateSb.append(o);
                } else {
                    updateSb.append("?");
                    updateParamArray[updateIndex++] = o;
                }
                updateSb.append(",");
            }
        }

        insertFieldSb = insertFieldSb.deleteCharAt(insertFieldSb.length() - 1);
        updateSb = updateSb.deleteCharAt(updateSb.length() - 1);
        String sql = String.format(UPSERT_DATA_TEMPLATE, baseMySqlTableInfo.getTableName(), insertFieldSb.toString(), insertValueSb.toString(), updateSb.toString());
        aniUpsert.setSql(sql);
        aniUpsert.setParamArray(ArrayUtil.addAll(paramArray, updateParamArray));
        return aniUpsert;
    }

    /**
     * upsert SQL
     *
     * @param upsertParamBatchCondition
     * @return
     */
    public AniHandler.AniUpsertBatch parserUpsertBatchSql(UpsertParamBatchCondition upsertParamBatchCondition, BaseMySqlDatabaseInfo baseMySqlDatabaseInfo, BaseMySqlTableInfo baseMySqlTableInfo) {
        AniHandler.AniUpsertBatch aniUpsertBatch = new AniHandler.AniUpsertBatch();
        List<Map<String, Object>> upsertParamList = upsertParamBatchCondition.getUpsertParamList();
        List<String> updateKeys = upsertParamBatchCondition.getUpdateKeys();
        Set<String> keys = upsertParamList.parallelStream().flatMap(map -> Stream.of(new HashSet(map.keySet()))).reduce((a, b) -> {a.addAll(b); return a;}).orElse(new HashSet());
        StringBuilder insertFieldSb = new StringBuilder();
        StringBuilder insertValueSb = new StringBuilder();
        StringBuilder updateSb = new StringBuilder();
        Object[] paramArray = new Object[upsertParamList.size() * keys.size()];
        int index = 0;
        boolean updateFlag = CollectionUtil.isEmpty(updateKeys);
        for (String key : keys) {
            insertFieldSb.append(key).append(",");
            if (updateFlag) {
                updateSb.append(key).append(" = values(").append(key).append("),");
            }
        }
        if (!updateFlag) {
            for (String updateKey : updateKeys) {
                updateSb.append(updateKey).append(" = values(").append(updateKey).append("),");
            }
        }
        for (Map<String, Object> upsertParamMap : upsertParamList) {
            insertValueSb.append("(");
            for (String key : keys) {
                Object value = upsertParamMap.get(key);
                Object o = translateValue(value);
                if (value instanceof Point) {
                    insertValueSb.append(o);
                } else {
                    insertValueSb.append("?");
                    paramArray[index++] = o;
                }
                insertValueSb.append(",");
            }
            insertValueSb = insertValueSb.deleteCharAt(insertValueSb.length() - 1);
            insertValueSb.append("),");
        }
        insertFieldSb = insertFieldSb.deleteCharAt(insertFieldSb.length() - 1);
        insertValueSb = insertValueSb.deleteCharAt(insertValueSb.length() - 1);
        updateSb = updateSb.deleteCharAt(updateSb.length() - 1);
        String sql = String.format(UPSERT_DATA_TEMPLATE, baseMySqlTableInfo.getTableName(), insertFieldSb.toString(), insertValueSb.toString(), updateSb.toString());
        aniUpsertBatch.setSql(CollectionUtil.newArrayList(sql));
        ArrayList<Object[]> paramList = new ArrayList<>();
        paramList.add(paramArray);
        aniUpsertBatch.setParamArray(paramList);
        return aniUpsertBatch;
    }

    /**
     * 根据标准数据类型获取类型适配
     *
     * @return
     */
    protected FieldTypeAdapter getAdapter(String fieldType) {
        return BaseMySqlFieldTypeEnum.findFieldTypeEnum(fieldType);
    }

    public AniHandler.AniCreateDatabase parserCreateDatabase(CreateDatabaseParamCondition paramCondition, BaseMySqlDatabaseInfo dataConf) {

        String dbName = paramCondition.getDbName();
        if(StringUtils.isBlank(dbName)) {
            throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "数据库名称不能为空");
        }
        AniHandler.AniCreateDatabase aniCreateDatabase = new AniHandler.AniCreateDatabase();
        aniCreateDatabase.setDbName(dbName);
        aniCreateDatabase.setCharacter(paramCondition.getCharacter());
        aniCreateDatabase.setCollate(paramCondition.getCollate());
        return aniCreateDatabase;
    }

    public AniHandler.AniUpdateDatabase parserUpdateDatabase(UpdateDatabaseParamCondition paramCondition, BaseMySqlDatabaseInfo dataConf) {

        String dbName = paramCondition.getDbName();
        String newDbName = paramCondition.getNewDbName();
        if(StringUtils.isBlank(dbName)) {
            throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "数据库名称不能为空");
        }
        if(StringUtils.isBlank(newDbName)) {
            throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "新数据库名称不能为空");
        }
        AniHandler.AniUpdateDatabase aniUpdateDatabase = new AniHandler.AniUpdateDatabase();
        aniUpdateDatabase.setDbName(dbName);
        aniUpdateDatabase.setNewDbName(newDbName);
        return aniUpdateDatabase;
    }

    public AniHandler.AniDropDatabase parserDropDatabase(DropDatabaseParamCondition paramCondition, BaseMySqlDatabaseInfo dataConf) {

        String dbName = paramCondition.getDbName();
        if(StringUtils.isBlank(dbName)) {
            throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "数据库名称不能为空");
        }
        AniHandler.AniDropDatabase aniDropDatabase = new AniHandler.AniDropDatabase();
        aniDropDatabase.setDbName(dbName);
        return aniDropDatabase;
    }

}
