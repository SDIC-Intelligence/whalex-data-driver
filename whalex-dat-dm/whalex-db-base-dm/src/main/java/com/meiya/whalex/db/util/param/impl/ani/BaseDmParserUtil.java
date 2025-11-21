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
import com.meiya.whalex.interior.db.constant.ForeignKeyActionEnum;
import com.meiya.whalex.interior.db.constant.ItemFieldTypeEnum;
import com.meiya.whalex.interior.db.operation.in.AlterTableParamCondition;
import com.meiya.whalex.interior.db.operation.in.CreateTableParamCondition;
import com.meiya.whalex.interior.db.operation.in.Point;
import com.meiya.whalex.interior.db.search.condition.*;
import com.meiya.whalex.interior.db.search.in.*;
import com.meiya.whalex.keyword.KeyWordHandler;
import com.meiya.whalex.util.AggResultTranslateUtil;
import com.meiya.whalex.util.JsonUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 关系型数据库通用，解析查询实体转换为SQL
 *
 * @author 蔡荣桂
 * @date 2021/4/14
 * @project whale-cloud-platformX
 */
@Slf4j
public class BaseDmParserUtil {

    public final static String DOUBLE_QUOTATION_MARKS = "\"";
    public final static String SYMBOL_N = "\n";
    public final static String SYMBOL_R = "\r";
    public final static String SYMBOL_T = "\t";
    public final static String SYMBOL_SINGLE_QUOTES = "'";
    public final static String SYMBOL_SINGLE_QUOTES_COV = "''";
    public final static String SYMBOL_X_ONE = "\\x";
    public final static String SYMBOL_X_TOW = "\\\\x";
    public final static String SYMBOL_SPRIT_ONE = "\\";
    public final static String SYMBOL_SPRIT_TWO = "\\\\";
    public final static String SYMBOL_REPLACE = " ";
    public final static KeyWordHandler keyWordHandler = new BaseDmKeywordHandler();

    /**
     * 建表模板
     */
    private final static String CREATE_TABLE_TEMPLATE = "CREATE TABLE %s ( %s )\n";

    /**
     * 创建序列模板
     */
    private final static String CREATE_SEQUENCE_TEMPLATE = "create sequence %s start with %s increment by %s nominvalue nomaxvalue nocycle nocache\n";

    /**
     * 表注释模板
     */
    private final static String TABLE_COMMENT_TEMPLATE = "COMMENT ON TABLE %s IS '%s'\n";

    /**
     * 字段注释模板
     */
    private final static String COLUMN_COMMENT_TEMPLATE = "COMMENT ON COLUMN %s.%s IS '%s'\n";

    /**
     * 插入模板
     */
    private final static String INSERT_DATA_TEMPLATE = "INSERT INTO %s ( %s ) VALUES %s";

    /**
     * 删除模板
     */
    private final static String DELETE_DATA_TEMPLATE = "DELETE FROM %s %s";

    /**
     * 更新模板
     */
    private final static String UPDATE_DATA_TEMPLATE = "UPDATE %s SET %s";

    /**
     * 创建索引
     */
    private final static String CREATE_INDEX_TEMPLATE = "CREATE %s INDEX \"%s\" ON %s(%s) GLOBAL";

    /**
     * 删除索引
     */
    private final static String DROP_INDEX_TEMPLATE = "DROP INDEX %s.\"%s\"";

    /**
     * 插入模板
     */
    private final static String UPSERT_DATA_TEMPLATE = "MERGE INTO %s AS t1 USING (%s) AS t2 ON (%s) WHEN MATCHED THEN UPDATE SET %s WHEN NOT MATCHED THEN INSERT (%s) VALUES(%s)";

    /**
     * 时间函数模板
     */
    private final static String DATE_FUNC_TEMPLATE = "TO_DATE(%s, 'SYYYY-MM-DD HH24:MI:SS')";

    /**
     * 坐标
     */
    protected final static String POINT_TEMPLATE = "dmgeo.ST_PointFromText('POINT(%s %s)',0)";

    /**
     * 修改表名
     */
    protected final static String UPDATE_TABLE_NAME = "ALTER TABLE %s RENAME TO %s";

    /**
     * 设置字段备注模板
     */
    protected final static String SET_FIELD_COMMENT_TEMPLATE = "COMMENT ON COLUMN %s.%s IS '%s'";

    /**
     * 修改字段名称
     */
    protected final static String UPDATE_TABLE_FIELD_NAME = "ALTER TABLE %s RENAME COLUMN %s TO %s";

    /**
     * 新增表主键
     */
    protected final static String ADD_PRIMARY_KEY = "ALTER TABLE %s ADD PRIMARY KEY(%s)";

    /**
     * 新增表外键
     */
    protected final static String ADD_FOREIGN_KEY = "ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY(%s) REFERENCES %s(%s) ON DELETE %s ON UPDATE %s";

    /**
     * 新增表外键
     */
    protected final static String DROP_FOREIGN_KEY = "ALTER TABLE %s DROP CONSTRAINT %s";

    /**
     * 修改表模板
     * ALTER TABLE ppz_test DROP COLUMN IF EXISTS name,
     * DROP COLUMN IF EXISTS pass;
     */
    protected final static String ALTER_TABLE_TEMPLATE = "ALTER TABLE %s %s";

    /**
     * 构建聚合查询
     *
     * @param paramCondition
     * @return
     */
    public AniHandler.AniQuery buildAggQuerySql(QueryParamCondition paramCondition, BaseDmDatabaseInfo baseDmDatabaseInfo, BaseDmTableInfo baseDmTableInfo) {
        //是否忽略大小写
        boolean ignoreCase = baseDmDatabaseInfo.isIgnoreCase() && baseDmTableInfo.isIgnoreCase();
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
        aggResolver(aggs, selectSb, groupSb, orderList, aggFunctions, ignoreCase);
        sb.append(selectSb);
        sb.append(" from ");

        // 表名
        String tableName = getTableName(baseDmDatabaseInfo.getSchema(), baseDmTableInfo.getTableName(), ignoreCase);
        sb.append(tableName);
        sb.append(" ");

        //where
        if (CollectionUtils.isNotEmpty(paramCondition.getWhere())) {
            List<Object> paramList = new LinkedList<>();
            sb.append(buildWhereSql(paramCondition.getWhere(), paramList, ignoreCase));
            aniQuery.setParamArray(paramList.toArray());
        }
        // 分组
        sb.append(" ").append(groupSb);

        //排序， 每一个聚合函数的结果都有对应的别名，顾需要做相应的转换
        if (CollectionUtils.isNotEmpty(orderList)) {
            orderList.stream().forEach(item -> {
                String field = item.getField();
                if (CollectionUtils.isNotEmpty(aggFunctions)) {
                    for (AggFunction aggFunction : aggFunctions) {
                        if (aggFunction.getField().equalsIgnoreCase(field)) {
                            item.setField(aggFunction.getFunctionName());
                            break;
                        }
                    }
                }
            });
        }
        sb.append(buildOrderSql(orderList, ignoreCase));

        // 分页
        Page page = new Page();
        page.setOffset(aggs.getOffset());
        page.setLimit(aggs.getLimit());
        String pageSql = buildLimitSql(page);
        if (pageSql.equals("")) {
            // 不分页
            aniQuery.setSql(sb.toString());
        } else {
            //分页
            aniQuery.setSql(sb.toString() + pageSql);
        }

        // 详细数据
        aniQuery.setLimit(aggs.getLimit());
        aniQuery.setOffset(aggs.getOffset());
        aniQuery.setAgg(true);
        List<String> aggNames = new ArrayList<>();
        AggResultTranslateUtil.parserRelationDataAggKey(aggs, aggNames);
        aniQuery.setAggNames(aggNames);
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
    public void aggResolver(Aggs aggs, StringBuilder selectSb, StringBuilder groupSb, List<Order> orderList, List<AggFunction> aggFunctionList, boolean ignoreCase) {


        AggOpType type = aggs.getType();
        if (!type.equals(AggOpType.GROUP) && !type.equals(AggOpType.TERMS)) {
            throw new BusinessException("关系型数据库，只支持 group 和 terms聚合操作");
        }

        String filed = fieldHandler(aggs.getField(), ignoreCase);

        if (selectSb.length() == 0) {
            selectSb.append("select ").append(filed).append(" as \"").append(aggs.getAggName()).append("\"");
        } else {
            selectSb.append(",").append(filed).append(" as \"").append(aggs.getAggName()).append("\"");
        }
        if (groupSb.length() == 0) {
            groupSb.append("group by ").append(filed);
        } else {
            groupSb.append(",").append(filed);
        }
        if (CollectionUtil.isNotEmpty(aggs.getOrders())) {
            orderList.addAll(aggs.getOrders());
        }
        if (CollectionUtil.isNotEmpty(aggs.getAggList())) {
            aggResolver(aggs.getAggList().get(0), selectSb, groupSb, orderList, aggFunctionList, ignoreCase);
        } else {
            selectSb.append(",count(1) as \"doc_count\"");
            List<AggFunction> aggFunctions = aggs.getAggFunctions();
            if (CollectionUtils.isNotEmpty(aggFunctions)) {
                aggFunctionList.addAll(aggFunctions);
                aggFunctions.stream().forEach(item -> {
                    // 关系型数据库中，distinct不做操作
                    if (item.getAggFunctionType() != AggFunctionType.DISTINCT) {
                        selectSb.append(", ")
                                .append(item.getAggFunctionType().getType())
                                .append("(")
                                .append(fieldHandler(item.getField(), ignoreCase))
                                .append(") as \"")
                                .append(item.getFunctionName())
                                .append("\"");
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
    public AniHandler.AniQuery parserQuerySql(QueryParamCondition paramCondition, BaseDmDatabaseInfo baseDmDatabaseInfo, BaseDmTableInfo baseDmTableInfo) {

        //是否忽略大小写
        boolean ignoreCase = baseDmDatabaseInfo.isIgnoreCase() && baseDmTableInfo.isIgnoreCase();

        if (CollectionUtils.isNotEmpty(paramCondition.getAggList())) {
            return buildAggQuerySql(paramCondition, baseDmDatabaseInfo, baseDmTableInfo);
        }

        AniHandler.AniQuery aniQuery = new AniHandler.AniQuery();
        // 查询sql
        StringBuilder sb = new StringBuilder();
        // 总数sql
        StringBuilder sbCount = new StringBuilder();
        // 公用sql部分
        StringBuilder sbCom = new StringBuilder();

        sb.append("select ");
        sb.append(buildSelectSql(paramCondition.getSelect(), ignoreCase));
        sb.append(" from ");

        sbCount.append("select count(1) as count from ");

        // 表名
        String tableName = getTableName(baseDmDatabaseInfo.getSchema(), baseDmTableInfo.getTableName(), ignoreCase);
        sbCom.append(tableName);
        sb.append(" ");

        // 查询条件, 生成占位符的sql
        if (CollectionUtils.isNotEmpty(paramCondition.getWhere())) {
            //  是否包含范围查询
            List<Map<String, Object>> geoDistanceList = new ArrayList<>(1);
            List<Object> paramList = new LinkedList<>();
            sbCom.append(buildWhereSql(paramCondition.getWhere(), paramList, ignoreCase));
            aniQuery.setParamArray(paramList.toArray());
        }

        //分组
        sbCom.append(buildGroupSql(paramCondition.getGroup(), ignoreCase));

        sbCount.append(sbCom);

        // 总数
        aniQuery.setSqlCount(sbCount.toString());


        sb.append(sbCom);

        // 排序
        sb.append(buildOrderSql(paramCondition.getOrder(), ignoreCase));

        // 分页
        sb.append(buildLimitSql(paramCondition.getPage()));

        // 详细数据
        aniQuery.setSql(sb.toString());

        // 设置分页信息
        aniQuery.setLimit(paramCondition.getPage().getLimit());
        aniQuery.setOffset(paramCondition.getPage().getOffset());

        return aniQuery;
    }

    public String fieldHandler(String field, boolean ignoreCase) {

        if (ignoreCase) {
            if (keyWordHandler.isKeyWord(field)) {
                return keyWordHandler.handler(field);
            }

            //字段包含- 或 以数字开头
            if (StringUtils.containsIgnoreCase(field, "-") || StringUtils.isNumeric(StringUtils.substring(field, 0, 1))) {
                return DOUBLE_QUOTATION_MARKS + field + DOUBLE_QUOTATION_MARKS;
            }

            return field;
        }
        return DOUBLE_QUOTATION_MARKS + field + DOUBLE_QUOTATION_MARKS;
    }

    public String getTableName(String schema, String tableName, boolean ignoreCase) {
        return fieldHandler(schema, ignoreCase) + "." + fieldHandler(tableName, ignoreCase);
    }

    /**
     * 解析 建表 SQL
     * 建表语句中不能同时给字段写注释，因此需要分成多条sql执行
     *
     * @return
     */
    public AniHandler.AniCreateTable parserCreateTableSql(CreateTableParamCondition createTableParamCondition, BaseDmDatabaseInfo baseDmDatabaseInfo, BaseDmTableInfo baseDmTableInfo) {
        AniHandler.AniCreateTable createTable = new AniHandler.AniCreateTable();
        List<CreateTableParamCondition.CreateTableFieldParam> createTableFieldParamList = createTableParamCondition.getCreateTableFieldParamList();

        //是否忽略大小写
        boolean ignoreCase = baseDmDatabaseInfo.isIgnoreCase() && baseDmTableInfo.isIgnoreCase();
        //表名
        String tableName = getTableName(baseDmDatabaseInfo.getSchema(), baseDmTableInfo.getTableName(), ignoreCase);
        // 表注释
        String tableComment = createTableParamCondition.getTableComment();
        //存放建表sql
        StringBuilder createSqlBuilder = new StringBuilder();
        // 存放注释sql
        StringBuilder commentSqlBuilder = new StringBuilder();
        // 存放主键
        List<String> primaryKeyList = new LinkedList<>();
        //存放序列sql
//        StringBuilder sequenceSqlBuilder = new StringBuilder();

        //表注释sql
        if (StringUtils.isNotBlank(tableComment)) {
            commentSqlBuilder.append(String.format(TABLE_COMMENT_TEMPLATE, tableName, tableComment));
        }

        //建表sql
        for (CreateTableParamCondition.CreateTableFieldParam createTableFieldParam : createTableFieldParamList) {

            String fieldComment = createTableFieldParam.getFieldComment();

            //字段
            String fieldName = fieldHandler(createTableFieldParam.getFieldName(), ignoreCase);
            createSqlBuilder.append(fieldName);

            //字段类型
            String fieldType = createTableFieldParam.getFieldType();
            Integer fieldLength = createTableFieldParam.getFieldLength();
            Integer fieldDecimalPoint = createTableFieldParam.getFieldDecimalPoint();
            createSqlBuilder.append(" ").append(buildSqlDataType(fieldName, getAdapter(fieldType), fieldLength, fieldDecimalPoint));

            //主键自增 使用序列+默认值实现
            if (createTableFieldParam.isAutoIncrement()) {
                Long startIncrement = createTableFieldParam.getStartIncrement() == null ? 1L : createTableFieldParam.getStartIncrement();
                Long incrementBy = createTableFieldParam.getIncrementBy() == null ? 1L : createTableFieldParam.getIncrementBy();
                //生成序列名
                /*String sequenceName = baseDmTableInfo.getTableName() + "_" + createTableFieldParam.getFieldName();
                sequenceName = getTableName(baseDmDatabaseInfo.getSchema(), sequenceName, ignoreCase);
                sequenceSqlBuilder.append((String.format(CREATE_SEQUENCE_TEMPLATE, sequenceName, startIncrement, incrementBy)));
                createSqlBuilder.append(" DEFAULT ").append(sequenceName + ".NEXTVAL");*/
                createSqlBuilder.append(" IDENTITY(").append(startIncrement).append(",").append(incrementBy).append(")");
            }

            //约束条件
            if (createTableFieldParam.isNotNull()) {
                createSqlBuilder.append(" NOT NULL");
            }

            //默认值
            String defaultValue = createTableFieldParam.getDefaultValue();
            if (defaultValue != null) {
                createSqlBuilder.append(" DEFAULT ").append(defaultValue);
            }

            //字段注释
            if (StringUtils.isNotBlank(fieldComment)) {
                commentSqlBuilder.append(String.format(COLUMN_COMMENT_TEMPLATE, tableName, fieldName, fieldComment));
            }

            createSqlBuilder.append(",");
            if (createTableFieldParam.isPrimaryKey()) {
                primaryKeyList.add(fieldName);
            }
        }

        //主键
        createSqlBuilder.deleteCharAt(createSqlBuilder.length() - 1);
        if (CollectionUtils.isNotEmpty(primaryKeyList)) {
            createSqlBuilder.append(",PRIMARY KEY (");
            primaryKeyList.forEach(primaryKey -> {
                createSqlBuilder.append(primaryKey).append(",");
            });
            createSqlBuilder.deleteCharAt(createSqlBuilder.length() - 1);
            createSqlBuilder.append(")");
        }

        String createTableSql = String.format(CREATE_TABLE_TEMPLATE, tableName, createSqlBuilder.toString());
        String commentSql = commentSqlBuilder.toString();
        //sql的顺序 序列 建表 字段注释
        createTable.setSql(createTableSql + commentSql);
        createTable.setNotExists(createTableParamCondition.isNotExists());
        return createTable;
    }


    /**
     * 解析 数据插入语句
     *
     * @param addParamCondition
     * @return
     */
    public AniHandler.AniInsert parserInsertSql(AddParamCondition addParamCondition, BaseDmDatabaseInfo baseDmDatabaseInfo, BaseDmTableInfo baseDmTableInfo) {
        AniHandler.AniInsert aniInsert = new AniHandler.AniInsert();

        List<Object> paramsList = new ArrayList<>();

        // 入库数据
        List<Map<String, Object>> fieldValueList = addParamCondition.getFieldValueList();

        aniInsert.setAddTotal(fieldValueList.size());

        // 存储入库字段名
        Set<String> fieldSet = new LinkedHashSet<>();

        // 遍历获取字段名
        fieldValueList.forEach(fieldMap -> {
            fieldMap.forEach((key, value) -> {
                fieldSet.add(key);
            });
        });

        StringBuilder fieldBuilder = new StringBuilder();

        //是否忽略大小写
        boolean ignoreCase = baseDmDatabaseInfo.isIgnoreCase() && baseDmTableInfo.isIgnoreCase();

        //表名
        String tableName = getTableName(baseDmDatabaseInfo.getSchema(), baseDmTableInfo.getTableName(), ignoreCase);

        for (String field : fieldSet) {
            fieldBuilder.append(fieldHandler(field, ignoreCase)).append(",");
        }
        fieldBuilder.deleteCharAt(fieldBuilder.length() - 1);

        StringBuilder insertSql = new StringBuilder();
        insertSql.append("BEGIN ");
//        insertSql.append("SET IDENTITY_INSERT ").append(tableName).append(" ON; ");
        // 是否返回自增主键
        boolean returnGeneratedKey = addParamCondition.isReturnGeneratedKey();
        for (int i = 0; i < fieldValueList.size(); i++) {
            StringBuilder insertSb = new StringBuilder();
            Map<String, Object> recordMap = fieldValueList.get(i);
            if (MapUtils.isEmpty(recordMap)) {
                continue;
            }

            StringBuilder valueSb = new StringBuilder();

            for (String field : fieldSet) {
                Object value = recordMap.get(field);
                Object o = translateValue(value);
                if (value instanceof Point) {
                    // 特殊处理经纬度类型
                    valueSb.append(o);
                } else {
                    paramsList.add(o);
                    valueSb.append("?");
                }
                valueSb.append(",");
            }

            valueSb.deleteCharAt(valueSb.length() - 1);

            insertSb.append("(");
            insertSb.append(valueSb);
            insertSb.append("),");
            insertSb.deleteCharAt(insertSb.length() - 1);
            insertSb.append(";");

            // 返回自增主键值
            if (returnGeneratedKey) {
                insertSb.append(" SELECT @@IDENTITY AS GENERATED_KEY;");
            }

            String format = String.format(INSERT_DATA_TEMPLATE, tableName, fieldBuilder.toString(), insertSb.toString());
            insertSql.append(format);
        }
        // 返回值
        List<String> returnFields = addParamCondition.getReturnFields();
        // 定义变量作为返回值
        if (CollectionUtil.isNotEmpty(returnFields)) {
            StringBuilder returnFieldSb = new StringBuilder("DECLARE ");
            StringBuilder selectReturnFieldSb = new StringBuilder("SELECT ");
            List<String> insertReturns = new ArrayList<>(returnFields.size());
            List<String> insertIntoList = new ArrayList<>(returnFields.size());
            for (String returnField : returnFields) {
                returnFieldSb.append(fieldHandler(returnField, false))
                        .append(" ").append(tableName).append(".")
                        .append(fieldHandler(returnField, ignoreCase))
                        .append("%type; ");
                selectReturnFieldSb.append(fieldHandler(returnField, false)).append(",");
                insertReturns.add(fieldHandler(returnField, ignoreCase));
                insertIntoList.add(fieldHandler(returnField, false));
            }

            insertSql.insert(insertSql.lastIndexOf(";"), " RETURNING " + CollectionUtil.join(insertReturns, ",") + " INTO " + CollectionUtil.join(insertIntoList, ","));

            selectReturnFieldSb.delete(selectReturnFieldSb.length() - 1, selectReturnFieldSb.length()).append(";");

            insertSql.insert(0, returnFieldSb);
            insertSql.append(" ").append(selectReturnFieldSb);
        }

        insertSql.append(" END;");

        aniInsert.setSql(insertSql.toString());
        aniInsert.setParamArray(paramsList.toArray());
        aniInsert.setReturnGeneratedKey(addParamCondition.isReturnGeneratedKey());
        aniInsert.setReturnFields(returnFields);
        return aniInsert;
    }

    /**
     * 解析 数据删除语句
     *
     * @param delParamCondition
     * @return
     */
    public AniHandler.AniDel parserDelSql(DelParamCondition delParamCondition, BaseDmDatabaseInfo baseDmDatabaseInfo, BaseDmTableInfo baseDmTableInfo) {
        //是否忽略大小写
        boolean ignoreCase = baseDmDatabaseInfo.isIgnoreCase() && baseDmTableInfo.isIgnoreCase();
        String tableName = getTableName(baseDmDatabaseInfo.getSchema(), baseDmTableInfo.getTableName(), ignoreCase);
        AniHandler.AniDel aniDel = new AniHandler.AniDel();
        List<Where> where = delParamCondition.getWhere();
        if (CollectionUtils.isNotEmpty(where)) {
            List<Object> paramList = new LinkedList<>();
            String whereSql = buildWhereSql(where, paramList, ignoreCase);
            String delSql = String.format(DELETE_DATA_TEMPLATE, tableName, whereSql);
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
    public AniHandler.AniCreateIndex parserCreateIndexSql(IndexParamCondition indexParamCondition, BaseDmDatabaseInfo baseDmDatabaseInfo, BaseDmTableInfo baseDmTableInfo) {
        //是否忽略大小写
        boolean ignoreCase = baseDmDatabaseInfo.isIgnoreCase() && baseDmTableInfo.isIgnoreCase();
        // 表名
        String tableName = getTableName(baseDmDatabaseInfo.getSchema(), baseDmTableInfo.getTableName(), ignoreCase);
        AniHandler.AniCreateIndex aniCreateIndex = new AniHandler.AniCreateIndex();

        List<IndexParamCondition.IndexColumn> columns = indexParamCondition.getColumns();

        List<String> indexFields = new ArrayList<>();

        List<String> indexNameByFields = new ArrayList<>();

        if (CollectionUtil.isNotEmpty(columns)) {
            for (IndexParamCondition.IndexColumn column : columns) {
                String indexColumn = fieldHandler(column.getColumn(), ignoreCase);
                Sort sort = column.getSort();
                if (sort != null) {
                    indexColumn = indexColumn + " " + sort.name();
                }
                indexFields.add(indexColumn);
                indexNameByFields.add(column.getColumn());
            }
        } else {
            String column = indexParamCondition.getColumn();
            String indexColumn = fieldHandler(column, ignoreCase);
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
            indexName = "index_" + baseDmTableInfo.getTableName() + "_" + CollectionUtil.join(indexNameByFields, "_");
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

        aniCreateIndex.setSql(String.format(CREATE_INDEX_TEMPLATE, indexType, indexName, tableName, CollectionUtil.join(indexFields, ",")));
        return aniCreateIndex;
    }

    /**
     * 解析 创建索引语句
     *
     * @param indexParamCondition
     * @return
     */
    public AniHandler.AniDropIndex parserDropIndexSql(IndexParamCondition indexParamCondition, BaseDmDatabaseInfo baseDmDatabaseInfo, BaseDmTableInfo baseDmTableInfo) {
        String indexName = indexParamCondition.getIndexName();

        if (StringUtils.isBlank(indexName)) {
            throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "删除索引必须指定 indexName 索引名称!");
        }

        //是否忽略大小写
        boolean ignoreCase = baseDmDatabaseInfo.isIgnoreCase() && baseDmTableInfo.isIgnoreCase();
        AniHandler.AniDropIndex aniDropIndex = new AniHandler.AniDropIndex();
        aniDropIndex.setSql(String.format(DROP_INDEX_TEMPLATE, fieldHandler(baseDmDatabaseInfo.getSchema(), ignoreCase), indexName));
        return aniDropIndex;
    }

    /**
     * 解析 数据更新语句
     *
     * @param updateParamCondition
     * @return
     */
    public AniHandler.AniUpdate parserUpdateSql(UpdateParamCondition updateParamCondition, BaseDmDatabaseInfo baseDmDatabaseInfo, BaseDmTableInfo baseDmTableInfo) {
        //是否忽略大小写
        boolean ignoreCase = baseDmDatabaseInfo.isIgnoreCase() && baseDmTableInfo.isIgnoreCase();
        String tableName = getTableName(baseDmDatabaseInfo.getSchema(), baseDmTableInfo.getTableName(), ignoreCase);
        List<Object> paramList = new ArrayList<>();
        AniHandler.AniUpdate aniUpdate = new AniHandler.AniUpdate();
        Map<String, Object> updateParamMap = updateParamCondition.getUpdateParamMap();
        StringBuilder updateSb = new StringBuilder();
        updateParamMap.forEach((field, value) -> {
            updateSb.append(fieldHandler(field, ignoreCase)).append(" =");
            Object o = translateValue(value);
            if (value instanceof Point) {
                // 特殊处理经纬度类型
                updateSb.append(o);
            } else {
                paramList.add(o);
                updateSb.append("?");
            }
            updateSb.append(",");
        });
        updateSb.deleteCharAt(updateSb.length() - 1);
        List<Where> where = updateParamCondition.getWhere();
        if (CollectionUtils.isNotEmpty(where)) {
            String whereSql = buildWhereSql(where, paramList, ignoreCase);
            updateSb.append(" ").append(whereSql);
        }
        aniUpdate.setParamArray(paramList.toArray());
        aniUpdate.setSql(String.format(UPDATE_DATA_TEMPLATE, tableName, updateSb.toString()));
        return aniUpdate;
    }

    private Object translateValue(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Date) {
            return DateUtil.format((Date) value, DatePattern.NORM_DATETIME_PATTERN);
        } else if (value instanceof Timestamp) {
            return DateUtil.format((Timestamp) value, DatePattern.NORM_DATETIME_PATTERN);
        } else if (value instanceof Point) {
            Point point = (Point) value;
            return String.format(POINT_TEMPLATE, point.getLon(), point.getLat());
        } else if (value instanceof Boolean || value instanceof Integer || value instanceof Double || value instanceof Long || value instanceof Float) {
            return value;
        } else if (value instanceof List || (value.getClass().isArray() && !(value instanceof byte[])) || value instanceof Map) {
            return JsonUtil.objectToStr(value);
        } else {
            return value;
        }
    }

    /**
     * 对插入数据字段值进行格式化
     *
     * @param value
     * @return
     */
    protected String formatter(String value) {
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
    private String buildSelectSql(List<String> selects, boolean ignoreCase) {
        StringBuilder sb = new StringBuilder();
        if (CollectionUtil.isNotEmpty(selects)) {
            for (String select : selects) {

                if (StringUtils.isBlank(select)) {
                    continue;
                }

                String checkMethod = checkMethod(select, ignoreCase);
                if (StringUtils.isBlank(checkMethod)) {
                    sb.append(fieldHandler(select, ignoreCase));
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
     *
     * @param select
     * @return
     */
    private String checkMethod(String select, boolean ignoreCase) {
        // 样例 f(count(field))
        boolean flag = StringUtils.startsWithIgnoreCase(select, "f(");
        if (flag) {
            select = select.substring(2, select.lastIndexOf(")"));
            String methodName = select.substring(0, select.indexOf("("));
            String parse = Method.parse(methodName.toLowerCase()).getName();
            String field = select.substring(select.indexOf("(") + 1, select.lastIndexOf(")"));
            field = fieldHandler(field, ignoreCase);
            StringBuffer stringBuffer = new StringBuffer();
            stringBuffer
                    .append(parse).append("(").append(field).append(") AS ")
                    .append(DOUBLE_QUOTATION_MARKS).append(select).append(DOUBLE_QUOTATION_MARKS);
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
    private String buildWhereSql(List<Where> wheres, List<Object> paramList, boolean ignoreCase) {
        String sql = buildWhereSql(wheres, Rel.AND, paramList, ignoreCase);
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
    private String buildWhereSql(List<Where> wheres, Rel rel, List<Object> paramList, boolean ignoreCase) {
        StringBuilder sb = new StringBuilder();
        if (wheres != null && !wheres.isEmpty()) {
            for (Where where : wheres) {
                if (Rel.AND.equals(where.getType()) || Rel.OR.equals(where.getType()) || Rel.NOT.equals(where.getType())) {
                    List<Where> params = where.getParams();
                    String sql = buildWhereSql(params, where.getType(), paramList, ignoreCase);
                    if (sql.length() > 0) {
                        sb.append(" (");
                        sql = sql.substring(0, sql.length() - 2 - where.getType().getName().length());
                        sb.append(sql);
                        sb.append(") ");
                        sb.append(rel.getName());
                        sb.append(" ");
                        if (Rel.NOT.equals(where.getType())) {
                            sb.insert(0, where.getType() + " ");
                        }
                    }
                } else {
                    String whereSql = buildWhereSql(where, rel, paramList, ignoreCase);
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
    private String buildWhereSql(Where where, Rel rel, List<Object> paramList, boolean ignoreCase) {
        StringBuilder sb = new StringBuilder();
        if (whereHandler(where)) {
            String relStrHandler = relStrHandler(where, paramList, ignoreCase);
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
    private String relStrHandler(Where where, List<Object> paramList, boolean ignoreCase) {
        // oracle数据库date类型不能用string进行比较，需要用函数转换
        String prepareFlag = "?";
        if (where.getParam() instanceof Date) {
            prepareFlag = String.format(DATE_FUNC_TEMPLATE, "?");
            String format = DateUtil.format((Date) where.getParam(), DatePattern.NORM_DATETIME_PATTERN);
            where.setParam(format);
        } else if (where.getParam() instanceof String
                && StringUtils.equalsIgnoreCase(where.getParamType(), ItemFieldTypeEnum.DATE.getVal())) {
            prepareFlag = String.format(DATE_FUNC_TEMPLATE, "?");
        }

        Rel rel = where.getType();
        StringBuilder sb = new StringBuilder();

        String field = where.getField();
        sb.append(fieldHandler(field, ignoreCase));

        switch (rel) {
            case EQ:
            case TERM:
                sb.append(" = ");
                sb.append(prepareFlag);
                paramList.add(where.getParam());
                break;
            case NE:
                sb.append(" != ");
                sb.append(prepareFlag);
                paramList.add(where.getParam());
                break;
            case GT:
                sb.append(" > ");
                sb.append(prepareFlag);
                paramList.add(where.getParam());
                break;
            case GTE:
                sb.append(" >= ");
                sb.append(prepareFlag);
                paramList.add(where.getParam());
                break;
            case LT:
                sb.append(" < ");
                sb.append(prepareFlag);
                paramList.add(where.getParam());
                break;
            case LTE:
                sb.append(" <= ");
                sb.append(prepareFlag);
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
            case ILIKE:
            case LIKE:
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
            default:
                sb.append(" = ");
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
    private String buildLimitSql(Page page) {
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
    private String buildGroupSql(List<String> groupList, boolean ignoreCase) {
        StringBuilder sb = new StringBuilder();
        if (groupList != null && !groupList.isEmpty()) {
            sb.append(" GROUP BY ");
            for (int i = 0; i < groupList.size(); i++) {
                String group = groupList.get(i);
                sb.append(fieldHandler(group, ignoreCase));
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
    private String buildOrderSql(List<Order> orders, boolean ignoreCase) {
        StringBuilder sb = new StringBuilder();
        if (orders != null && !orders.isEmpty()) {
            StringBuilder sb1 = new StringBuilder();
            for (Order order : orders) {
                if (!RelDbUtil.orderHandler(order)) {
                    continue;
                }
                sb1.append(fieldHandler(order.getField(), ignoreCase));
                sb1.append(" ");
                sb1.append(order.getSort().getName());

                if(order.getNullSort() != null) {
                    if(order.getNullSort() == Sort.ASC) {
                        sb1.append(" NULLS FIRST");
                    }else {
                        sb1.append(" NULLS LAST");
                    }
                }

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
     * 解析 sql 字段
     *
     * @param fieldName
     * @param adapter
     * @param fieldLength
     * @param fieldDecimalPoint
     * @return
     */
    protected String buildSqlDataType(String fieldName, FieldTypeAdapter adapter, Integer fieldLength, Integer fieldDecimalPoint) {
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

        // 判断是否为 json 类型
        if (adapter.equals(BaseDmFieldTypeEnum.JSON)) {
            columnSb.append(" CHECK (").append(fieldName).append(" IS JSON(LAX))");
        }

        return columnSb.toString();
    }

    /**
     * 根据标准数据类型获取类型适配
     *
     * @return
     */
    protected FieldTypeAdapter getAdapter(String fieldType) {
        return BaseDmFieldTypeEnum.findFieldTypeEnum(fieldType);
    }


    private List<String> buildAlterTableAddColumn(String tableName, AlterTableParamCondition.AddTableFieldParam addTableFieldParam,
                                                  boolean ignoreCase) {

        String fieldType = addTableFieldParam.getFieldType();
        String fieldComment = addTableFieldParam.getFieldComment();
        Integer fieldLength = addTableFieldParam.getFieldLength();
        Integer fieldDecimalPoint = addTableFieldParam.getFieldDecimalPoint();
        String fieldName = addTableFieldParam.getFieldName();

        fieldName = fieldHandler(fieldName, ignoreCase);

        StringBuilder sb = new StringBuilder();
        //基本语法
        sb.append("ALTER TABLE " + tableName + " ADD COLUMN ")
                .append(fieldName)
                .append(SYMBOL_REPLACE)
                .append(buildSqlDataType(fieldName, getAdapter(fieldType), fieldLength, fieldDecimalPoint));


        if (addTableFieldParam.getNotNull() != null && addTableFieldParam.getNotNull()) {
            sb.append(" NOT NULL");
        }

        if (addTableFieldParam.getDefaultValue() != null) {
            sb.append(" DEFAULT ").append("'").append(addTableFieldParam.getDefaultValue()).append("'");
        }

        if (addTableFieldParam.isPrimaryKey()) {
            sb.append(" PRIMARY KEY");
        }

        List<String> sqlList = new ArrayList<>(2);
        sqlList.add(sb.toString());

        if (StringUtils.isNotBlank(fieldComment)) {
            String setComment = String.format(SET_FIELD_COMMENT_TEMPLATE, tableName, fieldName, fieldComment);
            sqlList.add(setComment);
        }
        return sqlList;
    }

    private List<String> buildAlterTableModifyColumn(String tableName, AlterTableParamCondition.UpdateTableFieldParam updateTableFieldParam,
                                                     boolean ignoreCase) {

        List<String> sqlList = new ArrayList<>(5);

        String fieldName = updateTableFieldParam.getFieldName();
        String newFieldName = updateTableFieldParam.getNewFieldName();


        fieldName = fieldHandler(fieldName, ignoreCase);


        //修改字段名
        if (StringUtils.isNotBlank(newFieldName)) {
            newFieldName = fieldHandler(newFieldName, ignoreCase);
            sqlList.add(String.format(UPDATE_TABLE_FIELD_NAME, tableName, fieldName, newFieldName));
            return sqlList;
        }

        //例子： alter TABLE dat_test alter COLUMN name1 set default '2222', alter COLUMN name1 set not null
        String fieldType = updateTableFieldParam.getFieldType();
        String fieldComment = updateTableFieldParam.getFieldComment();
        Integer fieldLength = updateTableFieldParam.getFieldLength();
        Integer fieldDecimalPoint = updateTableFieldParam.getFieldDecimalPoint();
        String defaultValue = updateTableFieldParam.getDefaultValue();


        if (fieldType != null) {
            String modifyFiledTypeSql = "MODIFY COLUMN " + fieldName + " " + buildSqlDataType(fieldName, getAdapter(fieldType), fieldLength, fieldDecimalPoint);
            sqlList.add(String.format(ALTER_TABLE_TEMPLATE, tableName, modifyFiledTypeSql));
        }

        //约束条件[不为空]
        Boolean notNull = updateTableFieldParam.getNotNull();
        if (notNull != null) {
            if (notNull) {
                String alterSql = "ALTER COLUMN " + fieldName + " SET NOT NULL";
                sqlList.add(String.format(ALTER_TABLE_TEMPLATE, tableName, alterSql));
            } else {
                String alterSql = "ALTER COLUMN " + fieldName + " SET NULL";
                sqlList.add(String.format(ALTER_TABLE_TEMPLATE, tableName, alterSql));
            }
        }

        //约束条件[默认值]
        if (defaultValue != null) {

            String alterSql = "ALTER COLUMN " + fieldName + " SET DEFAULT '" + defaultValue + "'";
            sqlList.add(String.format(ALTER_TABLE_TEMPLATE, tableName, alterSql));
        }

        //注释
        if (StringUtils.isNotBlank(fieldComment)) {
            String setComment = String.format(SET_FIELD_COMMENT_TEMPLATE, tableName, fieldName, fieldComment);
            sqlList.add(setComment);
        }

        if (updateTableFieldParam.isPrimaryKey()) {
            sqlList.add(String.format(ADD_PRIMARY_KEY, tableName, fieldName));
        }

        return sqlList;
    }

    /**
     * 解析 修改表 SQL
     *
     * @return
     */
    public AniHandler.AniAlterTable parserAlterTableSql(AlterTableParamCondition alterTableParamCondition,
                                                        BaseDmDatabaseInfo baseDmDatabaseInfo,
                                                        BaseDmTableInfo baseDmTableInfo) {
        AniHandler.AniAlterTable aniAlterTable = new AniHandler.AniAlterTable();
        List<String> delTableFieldParamList = alterTableParamCondition.getDelTableFieldParamList();
        List<AlterTableParamCondition.AddTableFieldParam> addTableFieldParamList = alterTableParamCondition.getAddTableFieldParamList();
        List<AlterTableParamCondition.UpdateTableFieldParam> updateTableFieldParamList = alterTableParamCondition.getUpdateTableFieldParamList();
        List<AlterTableParamCondition.ForeignParam> addForeignParamList = alterTableParamCondition.getAddForeignParamList();
        List<AlterTableParamCondition.ForeignParam> delForeignParamList = alterTableParamCondition.getDelForeignParamList();
        String newTableName = alterTableParamCondition.getNewTableName();
        String tableComment = alterTableParamCondition.getTableComment();

        //是否忽略大小写
        boolean ignoreCase = baseDmDatabaseInfo.isIgnoreCase() && baseDmTableInfo.isIgnoreCase();
        String tableName = getTableName(baseDmDatabaseInfo.getSchema(), baseDmTableInfo.getTableName(), ignoreCase);
        List<String> sqlList = new ArrayList<>();

        // 构造修改表名sql
        if (StringUtils.isNotBlank(newTableName)) {
            newTableName = fieldHandler(newTableName, ignoreCase);
            sqlList.add(String.format(UPDATE_TABLE_NAME, tableName, newTableName));
        }

        // 修改表描述
        if(StringUtils.isNotBlank(tableComment)) {
            sqlList.add(String.format(TABLE_COMMENT_TEMPLATE, tableName, tableComment));
        }

        // 构造删除sql
        if (CollectionUtils.isNotEmpty(delTableFieldParamList)) {
            delTableFieldParamList.forEach(element -> {
                element = fieldHandler(element, ignoreCase);
                String dropColumnSql = "DROP COLUMN " + element;
                sqlList.add(String.format(ALTER_TABLE_TEMPLATE, tableName, dropColumnSql));
            });
        }


        // 构造字段添加sql
        addTableFieldParamList.forEach(addTableFieldParam -> {
            sqlList.addAll(buildAlterTableAddColumn(tableName, addTableFieldParam, ignoreCase));

        });

        // 构造字段修改sql
        updateTableFieldParamList.forEach(updateTableFieldParam -> {
            sqlList.addAll(buildAlterTableModifyColumn(tableName, updateTableFieldParam, ignoreCase));
        });

        //新增外键
        if (CollectionUtils.isNotEmpty(addForeignParamList)) {
            addForeignParamList.forEach(element -> {
                String foreignName = element.getForeignName();
                String foreignKey = element.getForeignKey();
                String referencesTableName = element.getReferencesTableName();
                String referencesField = element.getReferencesField();
                ForeignKeyActionEnum onDeletion = element.getOnDeletion();
                ForeignKeyActionEnum onUpdate = element.getOnUpdate();
                if (onDeletion == null) {
                    onDeletion = ForeignKeyActionEnum.NO_ACTION;
                }
                if (onUpdate == null) {
                    onUpdate = ForeignKeyActionEnum.NO_ACTION;
                }
                if (StringUtils.isAnyBlank(foreignKey, referencesTableName, referencesField)) {
                    throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION,
                            "参数foreignKey, referencesTableName, referencesField不能为空");
                }
                if (StringUtils.isBlank(foreignName)) {
                    foreignName = foreignKey + "_to_" + referencesTableName + "_" + referencesField;
                }

                referencesTableName = getTableName(baseDmDatabaseInfo.getSchema(), referencesTableName, ignoreCase);

                sqlList.add(String.format(
                        ADD_FOREIGN_KEY,
                        tableName,
                        fieldHandler(foreignName, ignoreCase),
                        fieldHandler(foreignKey, ignoreCase),
                        referencesTableName,
                        fieldHandler(referencesField, ignoreCase),
                        onDeletion.val,
                        onUpdate.val));
            });
        }
        //删除外键
        if (CollectionUtils.isNotEmpty(delForeignParamList)) {
            delForeignParamList.forEach(element -> {

                String foreignName = element.getForeignName();
                String foreignKey = element.getForeignKey();
                String referencesTableName = element.getReferencesTableName();
                String referencesField = element.getReferencesField();

                if (StringUtils.isBlank(foreignName)) {
                    if (StringUtils.isAnyBlank(foreignKey, referencesTableName, referencesField)) {
                        throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION,
                                "参数foreignKey, referencesTableName, referencesField不能为空或foreignName不能为空");
                    }
                    foreignName = foreignKey + "_to_" + referencesTableName + "_" + referencesField;
                }
                sqlList.add(String.format(DROP_FOREIGN_KEY, tableName, fieldHandler(foreignName, ignoreCase)));
            });
        }

        if (alterTableParamCondition.isDelPrimaryKey()) {
            throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "达梦不支持删除主键");
        }

        aniAlterTable.setSqlList(sqlList);

        return aniAlterTable;
    }

    /**
     * upsert SQL
     *
     * @param upsertParamCondition
     * @return
     */
    public AniHandler.AniUpsert parserUpsertSql(UpsertParamCondition upsertParamCondition, BaseDmDatabaseInfo baseDmDatabaseInfo, BaseDmTableInfo baseDmTableInfo) {

        //是否忽略大小写
        boolean ignoreCase = baseDmDatabaseInfo.isIgnoreCase() && baseDmTableInfo.isIgnoreCase();

        List<String> conflictFieldList = upsertParamCondition.getConflictFieldList();
        Map<String, Object> upsertParamMap = upsertParamCondition.getUpsertParamMap();
        Map<String, Object> updateParamMap = upsertParamCondition.getUpdateParamMap();
        Set<Map.Entry<String, Object>> entries = upsertParamMap.entrySet();
        // 临时表
        StringBuilder temporaryTableSb = new StringBuilder("SELECT ");
        // ON 连接条件
        StringBuilder onSb = new StringBuilder();
        // 更新
        StringBuilder updateSb = new StringBuilder();
        // 新增字段
        StringBuilder insertSb = new StringBuilder();
        StringBuilder insertValueSb = new StringBuilder();
        int index = 0;
        boolean updateFlag = MapUtil.isEmpty(updateParamMap);
        Object[] paramArray = new Object[upsertParamMap.size()];
        for (Map.Entry<String, Object> entry : entries) {
            Object o = translateValue(entry.getValue());
            if (entry.getValue() instanceof Point) {
                temporaryTableSb.append(o);
            } else {
                paramArray[index++] = o;
                temporaryTableSb.append("?");
            }
            temporaryTableSb.append(" AS ").append(fieldHandler(entry.getKey(), ignoreCase)).append(",");
            if (updateFlag && !conflictFieldList.contains(entry.getKey())) {
                updateSb.append("t1.").append(fieldHandler(entry.getKey(), ignoreCase))
                        .append(" = ")
                        .append("t2.").append(fieldHandler(entry.getKey(), ignoreCase)).append(",");
            }
            insertSb.append(fieldHandler(entry.getKey(), ignoreCase)).append(",");
            insertValueSb.append("t2.").append(fieldHandler(entry.getKey(), ignoreCase)).append(",");
        }
        for (String filed : conflictFieldList) {
            onSb.append("t1.").append(fieldHandler(filed, ignoreCase)).append(" = ")
                    .append("t2.").append(fieldHandler(filed, ignoreCase)).append(" AND ");
        }
        if (!updateFlag) {
            Set<Map.Entry<String, Object>> updateEntries = updateParamMap.entrySet();
            for (Map.Entry<String, Object> entry : updateEntries) {
                if (!conflictFieldList.contains(entry.getKey())) {
                    updateSb.append("t1.").append(fieldHandler(entry.getKey(), ignoreCase)).append(" = ");
                    Object o = translateValue(entry.getValue());
                    if (entry.getValue() instanceof Point) {
                        updateSb.append(o);
                    } else {
                        paramArray = ArrayUtil.append(paramArray, o);
                        updateSb.append("?");
                    }
                    updateSb.append(",");
                }
            }
        }
        if (updateSb.length() == 0) {
            throw new BusinessException(ExceptionCode.UPSERT_PARAM_EXCEPTION, "排重字段不能与更新字段完全一致");
        }
        temporaryTableSb = temporaryTableSb.deleteCharAt(temporaryTableSb.length() - 1);
        temporaryTableSb.append(" FROM DUAL");
        onSb = onSb.replace(onSb.lastIndexOf(" AND "), onSb.length() - 1, "");
        updateSb = updateSb.deleteCharAt(updateSb.length() - 1);
        insertSb = insertSb.deleteCharAt(insertSb.length() - 1);
        insertValueSb = insertValueSb.deleteCharAt(insertValueSb.length() - 1);
        String tableName = getTableName(baseDmDatabaseInfo.getSchema(), baseDmTableInfo.getTableName(), ignoreCase);
        String sql = String.format(UPSERT_DATA_TEMPLATE, tableName, temporaryTableSb.toString(), onSb.toString(), updateSb.toString(), insertSb.toString(), insertValueSb.toString());
        AniHandler.AniUpsert aniUpsert = new AniHandler.AniUpsert();
        aniUpsert.setSql(sql);
        aniUpsert.setParamArray(paramArray);
        return aniUpsert;
    }

    /**
     * 批量upsert SQL
     *
     * @param upsertParamBatchCondition
     * @param baseDmDatabaseInfo
     * @param baseDmTableInfo
     * @return
     */
    public AniHandler.AniUpsertBatch parserUpsertBatchSql(UpsertParamBatchCondition upsertParamBatchCondition, BaseDmDatabaseInfo baseDmDatabaseInfo, BaseDmTableInfo baseDmTableInfo) {
        List<String> conflictFieldList = upsertParamBatchCondition.getConflictFieldList();
        List<Map<String, Object>> upsertParamList = upsertParamBatchCondition.getUpsertParamList();
        List<String> updateKeys = upsertParamBatchCondition.getUpdateKeys();
        Optional<HashSet> optional = upsertParamList.parallelStream().flatMap(map -> Stream.of(new HashSet(map.keySet()))).reduce((a, b) -> {
            a.addAll(b);
            return a;
        });
        Set<String> keys = null;
        if (optional.isPresent()) {
            keys = optional.get();
        } else {
            throw new BusinessException(ExceptionCode.UPSERT_PARAM_EXCEPTION, "未获取到更新或新增的字段");
        }
        AniHandler.AniUpsertBatch aniUpsertBatch = new AniHandler.AniUpsertBatch();
        aniUpsertBatch.setSql(new ArrayList<>());
        aniUpsertBatch.setParamArray(new ArrayList<>());
        // 一次执行 99 行记录
        int batchSize = 99;
        for (int i = 0; i < upsertParamList.size(); i = i + batchSize) {
            List<Map<String, Object>> subParam;
            if (i + batchSize < upsertParamList.size()) {
                subParam = upsertParamList.subList(i, i + batchSize);
            } else {
                subParam = upsertParamList.subList(i, upsertParamList.size());
            }
            AniHandler.AniUpsert aniUpsert = upsertBatchSql(conflictFieldList, subParam, updateKeys, keys, baseDmDatabaseInfo, baseDmTableInfo);
            aniUpsertBatch.getSql().add(aniUpsert.getSql());
            aniUpsertBatch.getParamArray().add(aniUpsert.getParamArray());
        }
        return aniUpsertBatch;
    }

    /**
     * 组件批量 upsert 语句
     *
     * @param conflictFieldList
     * @param upsertParamList
     * @param updateKeys
     * @param keys
     * @param baseDmDatabaseInfo
     * @param baseDmTableInfo
     * @return
     */
    public AniHandler.AniUpsert upsertBatchSql(List<String> conflictFieldList, List<Map<String, Object>> upsertParamList, List<String> updateKeys, Set<String> keys, BaseDmDatabaseInfo baseDmDatabaseInfo, BaseDmTableInfo baseDmTableInfo) {
        //是否忽略大小写
        boolean ignoreCase = baseDmDatabaseInfo.isIgnoreCase() && baseDmTableInfo.isIgnoreCase();
        // 临时表
        StringBuilder temporaryTableSb = new StringBuilder();
        // ON 连接条件
        StringBuilder onSb = new StringBuilder();
        // 更新
        StringBuilder updateSb = new StringBuilder();
        // 新增字段
        StringBuilder insertSb = new StringBuilder();
        StringBuilder insertValueSb = new StringBuilder();
        int index = 0;
        boolean updateFlag = CollectionUtil.isEmpty(updateKeys);
        Object[] paramArray = new Object[upsertParamList.size() * keys.size()];
        // 拼接中间表
        for (Map<String, Object> map : upsertParamList) {
            temporaryTableSb.append("SELECT ");
            for (String key : keys) {
                Object paramO = map.get(key);
                Object o = translateValue(paramO);
                if (paramO instanceof Point) {
                    temporaryTableSb.append(o);
                } else {
                    paramArray[index++] = o;
                    temporaryTableSb.append("?");
                }
                temporaryTableSb.append(" AS ");
                temporaryTableSb.append(fieldHandler(key, ignoreCase));
                temporaryTableSb.append(",");
            }
            temporaryTableSb = temporaryTableSb.deleteCharAt(temporaryTableSb.length() - 1);
            temporaryTableSb.append(" FROM DUAL UNION ALL ");
        }

        for (String key : keys) {
            if (updateFlag && !conflictFieldList.contains(key)) {
                updateSb.append("t1.");
                updateSb.append(fieldHandler(key, ignoreCase));
                updateSb.append(" = ").append("t2.");
                updateSb.append(fieldHandler(key, ignoreCase));
                updateSb.append(",");
            }
            insertSb.append(fieldHandler(key, ignoreCase));
            insertSb.append(",");
            insertValueSb.append("t2.");
            insertValueSb.append(fieldHandler(key, ignoreCase));
            insertValueSb.append(",");
        }

        for (String filed : conflictFieldList) {
            onSb.append("t1.");
            onSb.append(fieldHandler(filed, ignoreCase));
            onSb.append(" = ").append("t2.");
            onSb.append(fieldHandler(filed, ignoreCase));
            onSb.append(" AND ");
        }
        if (!updateFlag) {
            for (String updateKey : updateKeys) {
                if (!conflictFieldList.contains(updateKey)) {
                    updateSb.append("t1.");
                    updateSb.append(fieldHandler(updateKey, ignoreCase));
                    updateSb.append(" = ").append("t2.");
                    updateSb.append(fieldHandler(updateKey, ignoreCase));
                    updateSb.append(",");
                }
            }
        }
        if (updateSb.length() == 0) {
            throw new BusinessException(ExceptionCode.UPSERT_PARAM_EXCEPTION, "排重字段不能与更新字段完全一致");
        }
        temporaryTableSb = temporaryTableSb.replace(temporaryTableSb.lastIndexOf(" UNION ALL "), temporaryTableSb.length() - 1, "");
        onSb = onSb.replace(onSb.lastIndexOf(" AND "), onSb.length() - 1, "");
        updateSb = updateSb.deleteCharAt(updateSb.length() - 1);
        insertSb = insertSb.deleteCharAt(insertSb.length() - 1);
        insertValueSb = insertValueSb.deleteCharAt(insertValueSb.length() - 1);
        String tableName = getTableName(baseDmDatabaseInfo.getSchema(), baseDmTableInfo.getTableName(), ignoreCase);
        String sql = String.format(UPSERT_DATA_TEMPLATE, tableName, temporaryTableSb.toString(), onSb.toString(), updateSb.toString(), insertSb.toString(), insertValueSb.toString());
        AniHandler.AniUpsert aniUpsert = new AniHandler.AniUpsert();
        aniUpsert.setSql(sql);
        aniUpsert.setParamArray(paramArray);
        return aniUpsert;
    }

}
