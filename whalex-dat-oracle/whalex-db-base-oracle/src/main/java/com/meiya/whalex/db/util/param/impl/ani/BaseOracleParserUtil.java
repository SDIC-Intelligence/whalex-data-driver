package com.meiya.whalex.db.util.param.impl.ani;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.DateUtil;
import com.meiya.whalex.db.constant.IndexType;
import com.meiya.whalex.db.entity.*;
import com.meiya.whalex.db.entity.ani.AniHandler;
import com.meiya.whalex.db.entity.ani.BaseOracleDatabaseInfo;
import com.meiya.whalex.db.entity.ani.BaseOracleFieldTypeEnum;
import com.meiya.whalex.db.entity.ani.BaseOracleTableInfo;
import com.meiya.whalex.db.entity.ani.KeywordConstant;
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
import com.meiya.whalex.interior.db.search.in.AggFunction;
import com.meiya.whalex.interior.db.search.in.Aggs;
import com.meiya.whalex.interior.db.search.in.Order;
import com.meiya.whalex.interior.db.search.in.Page;
import com.meiya.whalex.interior.db.search.in.QueryParamCondition;
import com.meiya.whalex.interior.db.search.in.Where;
import com.meiya.whalex.keyword.KeyWordHandler;
import com.meiya.whalex.util.AggResultTranslateUtil;
import com.meiya.whalex.util.JsonUtil;
import lombok.extern.slf4j.Slf4j;
import oracle.spatial.geometry.JGeometry;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 关系型数据库通用，解析查询实体转换为SQL
 *
 * @author 蔡荣桂
 * @date 2021/4/14
 * @project whale-cloud-platformX
 */
@Slf4j
public class BaseOracleParserUtil {

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

    /**
     * 建表模板
     */
    private final static String CREATE_TABLE_TEMPLATE = "CREATE TABLE %s ( %s )\n";

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
    private final static String CREATE_INDEX_TEMPLATE = "CREATE %s INDEX \"%s\" ON %s(%s)";

    /**
     * 删除索引
     */
    private final static String DROP_INDEX_TEMPLATE = "DROP INDEX \"%s\"";

    /**
     * 插入模板
     */
    private final static String UPSERT_DATA_TEMPLATE = "INSERT INTO ${tableName} ( %s ) VALUES %s";

    /**
     * 分页模板
     */
    private final static String PAGE_TEMPLATE = "SELECT * FROM (SELECT A.*, ROWNUM ORACLE_PAGE_FIELD FROM (${realSql}) A WHERE ROWNUM <= %s) WHERE ORACLE_PAGE_FIELD >= %s";

    /**
     * 时间函数模板
     */
    private final static String DATE_FUNC_TEMPLATE = "TO_DATE(%s, 'SYYYY-MM-DD HH24:MI:SS')";

    /**
     *新增表主键
     */
    protected final static String ADD_PRIMARY_KEY = "ALTER TABLE %s ADD PRIMARY KEY(%s)";

    /**
     *删除主键
     */
    protected final static String DROP_PRIMARY_KEY = "ALTER TABLE %s DROP PRIMARY KEY";

    /**
     * 修改字段名称
     */
    protected final static String UPDATE_TABLE_FIELD_NAME = "ALTER TABLE %s RENAME COLUMN  %s TO %s";

    /**
     * 修改表名
     */
    protected final static String UPDATE_TABLE_NAME = "ALTER TABLE %s RENAME TO %s";

    /**
     * 修改表模板
     * ALTER TABLE ppz_test DROP name,
     * DROP COLUMN IF EXISTS pass;
     */
    protected final static String ALTER_TABLE_TEMPLATE = "ALTER TABLE %s %s";

    /**
     *新增表外键
     */
    protected final static String ADD_FOREIGN_KEY = "ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY(%s) REFERENCES %s(%s)%s";

    /**
     *新增表外键
     */
    protected final static String DROP_FOREIGN_KEY = "ALTER TABLE %s DROP CONSTRAINT %s";

    /**
     * 设置字段备注模板
     */
    protected final static String SET_FIELD_COMMENT_TEMPLATE = "COMMENT ON COLUMN %s.%s IS '%s'";

    /**
     * 创建序列模板
     */
    protected final static String CREATE_SEQUENCE_TEMPLATE = "create sequence %s start with %s increment by %s nominvalue nomaxvalue nocycle nocache\n";

    /**
     * 创建触发器
     */
    protected final static String CREATE_TRIGGER_TEMPLATE = "CREATE TRIGGER %s BEFORE INSERT ON %s FOR EACH ROW DECLARE v_sequence NUMBER; BEGIN IF :NEW.%s IS NULL THEN SELECT %s.NEXTVAL INTO v_sequence FROM DUAL; :NEW.%s := v_sequence; END IF; END;\n";

    /**
     * 坐标
     */
    protected final static String POINT_TEMPLATE = "SDO_GEOMETRY(2001, NULL, SDO_POINT_TYPE(%s, %s, NULL), NULL, NULL)";

    private KeyWordHandler keyWordHandler = new KeywordConstant();

    /**
     * 构建聚合查询
     * @param paramCondition
     * @param tableName
     * @return
     */
    public AniHandler.AniQuery buildAggQuerySql(QueryParamCondition paramCondition, boolean ignoreCase, String tableName) {
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
        // 先使用占位符
        sb.append(tableName);
        sb.append(" ");

        List<Object> paramList = new ArrayList<>();

        //where
        if (CollectionUtils.isNotEmpty(paramCondition.getWhere())) {
            sb.append(buildWhereSql(paramCondition.getWhere(), paramList, ignoreCase));
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
        sb.append(buildOrderSql(orderList, ignoreCase));

        // 分页
        Page page = new Page();
        page.setOffset(aggs.getOffset());
        page.setLimit(aggs.getLimit());
        String pageTemplateSql = buildLimitSql(page);
        if(pageTemplateSql.equals("")){
            // 不分页
            aniQuery.setSql(sb.toString());
        }else{
            //分页
            String pageSql = pageTemplateSql.replace("${realSql}", sb.toString());
            aniQuery.setSql(pageSql);
        }

        // 详细数据
        aniQuery.setLimit(aggs.getLimit());
        aniQuery.setOffset(aggs.getOffset());
        aniQuery.setParamArray(paramList.toArray());
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
        if(!type.equals(AggOpType.GROUP) && !type.equals(AggOpType.TERMS)) {
            throw new BusinessException("关系型数据库，只支持 group 和 terms聚合操作");
        }
        if (selectSb.length() == 0) {
            selectSb.append("select ").append(fieldHandler(aggs.getField(), ignoreCase)).append(" as \"").append(aggs.getAggName()).append("\"");
        } else {
            selectSb.append(",").append(fieldHandler(aggs.getField(), ignoreCase)).append(" as \"").append(aggs.getAggName()).append("\"");
        }
        if (groupSb.length() == 0) {
            groupSb.append("group by ").append(fieldHandler(aggs.getField(), ignoreCase));
        } else {
            groupSb.append(",").append(fieldHandler(aggs.getField(), ignoreCase));
        }
        if (CollectionUtil.isNotEmpty(aggs.getOrders())) {
            orderList.addAll(aggs.getOrders());
        }
        if (CollectionUtil.isNotEmpty(aggs.getAggList())) {
            aggResolver(aggs.getAggList().get(0), selectSb, groupSb, orderList, aggFunctionList, ignoreCase);
        } else {
            selectSb.append(",count(1) as \"doc_count\"");
            List<AggFunction> aggFunctions = aggs.getAggFunctions();
            if(CollectionUtils.isNotEmpty(aggFunctions)) {
                aggFunctionList.addAll(aggFunctions);
                aggFunctions.stream().forEach(item->{
                    // 关系型数据库中，distinct不做操作
                    if(item.getAggFunctionType() != AggFunctionType.DISTINCT) {
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
     * @param databaseInfo
     * @param tableInfo
     * @return
     */
    public AniHandler.AniQuery parserQuerySql(QueryParamCondition paramCondition, BaseOracleDatabaseInfo databaseInfo, BaseOracleTableInfo tableInfo) {

        boolean ignoreCase = databaseInfo.isIgnoreCase();

        String schema = fieldHandler(databaseInfo.getSchema(), ignoreCase);
        String tableName = fieldHandler(tableInfo.getTableName(), ignoreCase);
        tableName = schema + "." + tableName;

        if(CollectionUtils.isNotEmpty(paramCondition.getAggList())) {
            return buildAggQuerySql(paramCondition, ignoreCase, tableName);
        }

        List<Object> paramList = new LinkedList<>();
        AniHandler.AniQuery aniQuery = new AniHandler.AniQuery();
        // 查询sql
        StringBuilder sb = new StringBuilder();
        // 总数sql
        StringBuilder sbCount = new StringBuilder();
        // 公用sql部分
        StringBuilder sbCom = new StringBuilder();

        sb.append("select ");
        sb.append(buildSelectSql(paramCondition.getSelect()));
        sb.append(" from ");

        sbCount.append("select count(1) as count from ");

        // 表名
        // 先使用占位符
        sbCom.append(tableName);
        sb.append(" ");

        // 查询条件, 生成占位符的sql
        if (CollectionUtils.isNotEmpty(paramCondition.getWhere())) {
            //  是否包含范围查询
            List<Map<String, Object>> geoDistanceList = new ArrayList<>(1);

            sbCom.append(buildWhereSql(paramCondition.getWhere(), paramList, ignoreCase));
        }

        sbCount.append(sbCom);

        // 总数
        aniQuery.setSqlCount(sbCount.toString());

        sb.append(sbCom);

        //分组
        sb.append(buildGroupSql(paramCondition.getGroup(), ignoreCase));

        // 排序
        sb.append(buildOrderSql(paramCondition.getOrder(), ignoreCase));

        // 分页
        String pageTemplateSql = buildLimitSql(paramCondition.getPage());
        if(pageTemplateSql.equals("")){
            // 不分页
            aniQuery.setSql(sb.toString());
        }else{
            //分页
            String pageSql = pageTemplateSql.replace("${realSql}", sb.toString());
            aniQuery.setSql(pageSql);
        }

        // 设置分页信息
        aniQuery.setLimit(paramCondition.getPage().getLimit());
        aniQuery.setOffset(paramCondition.getPage().getOffset());
        aniQuery.setParamArray(paramList.toArray());

        return aniQuery;
    }

    public String fieldHandler(String field, boolean ignoreCase) {
        if(ignoreCase) {
            if(keyWordHandler.isKeyWord(field)) {
                return keyWordHandler.handler(field);
            }
            return field.toUpperCase();
        } else {
            return DOUBLE_QUOTATION_MARKS +  field + DOUBLE_QUOTATION_MARKS;
        }
    }

    /**
     * 解析 建表 SQL
     *  Oracle 建表语句中不能同时给字段写注释，因此需要分成多条sql执行
     * @return
     */
    public AniHandler.AniCreateTable parserCreateTableSql(CreateTableParamCondition createTableParamCondition,
                                                          BaseOracleDatabaseInfo databaseInfo,
                                                          BaseOracleTableInfo tableInfo) {
        boolean ignoreCase = databaseInfo.isIgnoreCase();

        String schema = fieldHandler(databaseInfo.getSchema(), ignoreCase);
        String tableName = fieldHandler(tableInfo.getTableName(), ignoreCase);
        tableName = schema + "." + tableName;

        AniHandler.AniCreateTable createTable = new AniHandler.AniCreateTable();
        List<CreateTableParamCondition.CreateTableFieldParam> createTableFieldParamList = createTableParamCondition.getCreateTableFieldParamList();
        StringBuilder createSqlBuilder = new StringBuilder();
        // 存放注释sql
        StringBuilder commentSqlBuilder = new StringBuilder();
        // 存放主键
        List<String> primaryKeyList = new LinkedList<>();
        //存放序列sql
        StringBuilder sequenceSqlBuilder = new StringBuilder();
        // 触发器
        StringBuilder triggerSqlBuilder = new StringBuilder();
        // 表注释
        String tableComment = createTableParamCondition.getTableComment();
        if (StringUtils.isNotBlank(tableComment)) {
            commentSqlBuilder.append(String.format(TABLE_COMMENT_TEMPLATE, tableName, tableComment));
        }

        for (CreateTableParamCondition.CreateTableFieldParam createTableFieldParam : createTableFieldParamList) {
            String fieldType = createTableFieldParam.getFieldType();
            String fieldComment = createTableFieldParam.getFieldComment();
            Integer fieldLength = createTableFieldParam.getFieldLength();
            String fieldName = fieldHandler(createTableFieldParam.getFieldName(), ignoreCase);
            Integer fieldDecimalPoint = createTableFieldParam.getFieldDecimalPoint();
            createSqlBuilder.append(fieldName);
            createSqlBuilder.append(" ").append(buildSqlDataType(fieldName, getAdapter(fieldType), fieldLength, fieldDecimalPoint));

            //主键自增 使用序列+默认值实现
            if (createTableFieldParam.isAutoIncrement()) {
                Long startIncrement = createTableFieldParam.getStartIncrement() == null ? 1L : createTableFieldParam.getStartIncrement();
                Long incrementBy = createTableFieldParam.getIncrementBy() == null ? 1L : createTableFieldParam.getIncrementBy();
                //生成序列名
                String sequenceName = tableInfo.getTableName() + "_" + createTableFieldParam.getFieldName();
                sequenceName = fieldHandler(sequenceName, ignoreCase);
                sequenceSqlBuilder.append((String.format(CREATE_SEQUENCE_TEMPLATE, sequenceName, startIncrement, incrementBy)));
                // 生成触发器
                triggerSqlBuilder.append(String.format(CREATE_TRIGGER_TEMPLATE, sequenceName, tableName, fieldName, sequenceName, fieldName));
            }

            if (createTableFieldParam.isNotNull()) {
                createSqlBuilder.append(" NOT NULL");
            }
            if (StringUtils.isNotBlank(fieldComment)) {
                commentSqlBuilder.append(String.format(COLUMN_COMMENT_TEMPLATE, tableName, fieldName, fieldComment));
            }
            createSqlBuilder.append(",");
            if (createTableFieldParam.isPrimaryKey()) {
                primaryKeyList.add(fieldName);
            }
        }

        createSqlBuilder.deleteCharAt(createSqlBuilder.length() - 1);
        if (CollectionUtils.isNotEmpty(primaryKeyList)) {
            createSqlBuilder.append(",PRIMARY KEY (");
            primaryKeyList.forEach(primaryKey -> {
                createSqlBuilder.append(primaryKey).append(",");
            });
            createSqlBuilder.deleteCharAt(createSqlBuilder.length() - 1);
            createSqlBuilder.append(")");
        }
        createTable.setSql(sequenceSqlBuilder.toString()
                        + String.format(CREATE_TABLE_TEMPLATE, tableName, createSqlBuilder.toString())
                        + commentSqlBuilder.toString()
                        + triggerSqlBuilder.toString()
        );
        createTable.setNotExists(createTableParamCondition.isNotExists());
        return createTable;
    }

    /**
     * 解析 数据插入语句
     *
     * @param addParamCondition
     * @param databaseInfo
     * @param tableInfo
     * @return
     */
    public AniHandler.AniInsert parserInsertSql(AddParamCondition addParamCondition, BaseOracleDatabaseInfo databaseInfo, BaseOracleTableInfo tableInfo) {
        AniHandler.AniInsert aniInsert = new AniHandler.AniInsert();
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


        boolean ignoreCase = databaseInfo.isIgnoreCase();

        String schema = fieldHandler(databaseInfo.getSchema(), ignoreCase);
        String tableName = fieldHandler(tableInfo.getTableName(), ignoreCase);
        tableName = schema + "." + tableName;

        StringBuilder fieldBuilder = new StringBuilder();

        for (String key : fieldSet) {
            fieldBuilder.append(fieldHandler(key, ignoreCase)).append(",");
        }
        fieldBuilder.deleteCharAt(fieldBuilder.length() - 1);

        boolean returnGeneratedKey = addParamCondition.isReturnGeneratedKey();
        List<String> returnFields = addParamCondition.getReturnFields();
        StringBuilder insertSql = new StringBuilder();
        insertSql.append("BEGIN ");
        List<Object> paramList = new ArrayList<>();
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
                paramList.add(o);
                valueSb.append("?,");
            }

            valueSb.deleteCharAt(valueSb.length() - 1);

            insertSb.append("(");
            insertSb.append(valueSb);
            insertSb.append("),");
            insertSb.deleteCharAt(insertSb.length() - 1);
            // 返回自增主键值
            if (returnGeneratedKey) {
                insertSb.append(" RETURNING ${@@IDENTITY} INTO ?");
            } else if (CollectionUtil.isNotEmpty(returnFields)) {
                insertSb.append(" RETURNING ");
                StringBuilder placeholderRegisterParams = new StringBuilder(" INTO ");
                for (String returnField : returnFields) {
                    insertSb.append(fieldHandler(returnField, ignoreCase)).append(",");
                    placeholderRegisterParams.append("?,");
                }
                insertSb.deleteCharAt(insertSb.length() - 1);
                placeholderRegisterParams.deleteCharAt(placeholderRegisterParams.length() - 1);
                insertSb.append(placeholderRegisterParams);
            }
            insertSb.append(";");

            String format = String.format(INSERT_DATA_TEMPLATE, tableName, fieldBuilder.toString(), insertSb.toString());
            insertSql.append(format);
        }


        insertSql.append(" END;");

        aniInsert.setSql(insertSql.toString());
        aniInsert.setParamArray(paramList.toArray());
        aniInsert.setAddTotal(fieldValueList.size());
        aniInsert.setReturnFields(returnFields);
        aniInsert.setReturnGeneratedKey(addParamCondition.isReturnGeneratedKey());
        return aniInsert;
    }

    /**
     * 解析 数据删除语句
     *
     * @param delParamCondition
     * @param databaseInfo
     * @param tableInfo
     * @return
     */
    public AniHandler.AniDel parserDelSql(DelParamCondition delParamCondition, BaseOracleDatabaseInfo databaseInfo, BaseOracleTableInfo tableInfo) {
        AniHandler.AniDel aniDel = new AniHandler.AniDel();

        boolean ignoreCase = databaseInfo.isIgnoreCase();

        String schema = fieldHandler(databaseInfo.getSchema(), ignoreCase);
        String tableName = fieldHandler(tableInfo.getTableName(), ignoreCase);
        tableName = schema + "." + tableName;

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
     * @param databaseConf
     * @param tableConf
     * @return
     */
    public AniHandler.AniCreateIndex parserCreateIndexSql(IndexParamCondition indexParamCondition, BaseOracleDatabaseInfo databaseConf, BaseOracleTableInfo tableConf) {
        boolean ignoreCase = databaseConf.isIgnoreCase();
        //表名
        String schema = fieldHandler(databaseConf.getSchema(), ignoreCase);
        String tableName = fieldHandler(tableConf.getTableName(), ignoreCase);
        tableName = schema + "." + tableName;
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
            indexName = "index_" + tableConf.getTableName() + "_" + CollectionUtil.join(indexNameByFields, "_");
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
    public AniHandler.AniDropIndex parserDropIndexSql(IndexParamCondition indexParamCondition, BaseOracleDatabaseInfo databaseConf, BaseOracleTableInfo tableConf) {
        String indexName = indexParamCondition.getIndexName();

        if (StringUtils.isBlank(indexName)) {
            throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "删除索引必须指定 indexName 索引名称!");
        }

        AniHandler.AniDropIndex aniDropIndex = new AniHandler.AniDropIndex();
        aniDropIndex.setSql(String.format(DROP_INDEX_TEMPLATE, indexName));
        return aniDropIndex;
    }

    /**
     * 解析 数据更新语句
     *
     * @param updateParamCondition
     * @param databaseInfo
     * @param tableInfo
     * @return
     */
    public AniHandler.AniUpdate parserUpdateSql(UpdateParamCondition updateParamCondition, BaseOracleDatabaseInfo databaseInfo, BaseOracleTableInfo tableInfo) {
        AniHandler.AniUpdate aniUpdate = new AniHandler.AniUpdate();
        Map<String, Object> updateParamMap = updateParamCondition.getUpdateParamMap();
        boolean ignoreCase = databaseInfo.isIgnoreCase();

        String schema = fieldHandler(databaseInfo.getSchema(), ignoreCase);
        String tableName = fieldHandler(tableInfo.getTableName(), ignoreCase);
        tableName = schema + "." + tableName;

        List<Object> paramList = new ArrayList<>();
        StringBuilder updateSb = new StringBuilder();
        updateParamMap.forEach((key, value) -> {
            updateSb.append(fieldHandler(key, ignoreCase))
                    .append(" = ");
            paramList.add(translateValue(value));
            updateSb.append("?,");
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

    /**
     * 设置值
     *
     * @param sqlSb
     * @param value
     */
    protected void setValue(StringBuilder sqlSb, Object value) {
        if (value == null) {
            sqlSb.append("null");
            return;
        }
        if (value instanceof Date) {
            StringBuffer format = new StringBuffer();
            format.append(SYMBOL_SINGLE_QUOTES);
            format.append(DateUtil.format((Date) value, DatePattern.NORM_DATETIME_PATTERN));
            format.append(SYMBOL_SINGLE_QUOTES);
            sqlSb.append(String.format(DATE_FUNC_TEMPLATE, format.toString()));
        } else if (value instanceof Timestamp) {
            StringBuffer format = new StringBuffer();
            format.append(SYMBOL_SINGLE_QUOTES);
            format.append(DateUtil.format((Timestamp) value, DatePattern.NORM_DATETIME_PATTERN));
            format.append(SYMBOL_SINGLE_QUOTES);
            sqlSb.append(String.format(DATE_FUNC_TEMPLATE, format.toString()));
        } else if (value instanceof Boolean || value instanceof Integer || value instanceof Double || value instanceof Long || value instanceof Float) {
            sqlSb.append(value);
        } else if (value instanceof List || (value.getClass().isArray() && !(value instanceof byte[])) || value instanceof Map) {
            sqlSb.append(SYMBOL_SINGLE_QUOTES);
            sqlSb.append(JsonUtil.objectToStr(value));
            sqlSb.append(SYMBOL_SINGLE_QUOTES);
        } else {
            sqlSb.append(SYMBOL_SINGLE_QUOTES);
            sqlSb.append(formatter(String.valueOf(value)));
            sqlSb.append(SYMBOL_SINGLE_QUOTES);
        }
    }

    private Object translateValue(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Date) {
            return java.sql.Timestamp.valueOf(DateUtil.toLocalDateTime((Date) value));
        } else if (value instanceof Boolean || value instanceof Integer || value instanceof Double || value instanceof Long || value instanceof Float) {
            return value;
        } else if (value instanceof List || (value.getClass().isArray() && !(value instanceof byte[])) || value instanceof Map) {
            return JsonUtil.objectToStr(value);
        } else if (value instanceof Point) {
            Point point = (Point) value;
            JGeometry jGeometry = new JGeometry(point.getLon(), point.getLat(), JGeometry.GTYPE_POINT);
            return jGeometry;
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
    private String buildSelectSql(List<String> selects) {
        StringBuilder sb = new StringBuilder();
        if (selects != null && !selects.isEmpty()) {
            for (String select : selects) {
                if (StringUtils.isBlank(select)) {
                    continue;
                }
                String checkMethod = checkMethod(select);
                if (StringUtils.isBlank(checkMethod)) {
                    sb.append(DOUBLE_QUOTATION_MARKS);
                    sb.append(select);
                    sb.append(DOUBLE_QUOTATION_MARKS);
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
    private String checkMethod(String select) {
        boolean flag = StringUtils.startsWithIgnoreCase(select, "f(");
        if (flag) {
            select = select.substring(2, select.lastIndexOf(")"));
            String methodName = select.substring(0, select.indexOf("("));
            String parse = Method.parse(methodName.toLowerCase()).getName();
            String field = select.substring(select.indexOf("(") + 1, select.lastIndexOf(")"));
            StringBuffer stringBuffer = new StringBuffer();
            StringBuffer queryField = stringBuffer.append(parse).append("(").append(DOUBLE_QUOTATION_MARKS).append(field).append(DOUBLE_QUOTATION_MARKS).append(") AS ").append(DOUBLE_QUOTATION_MARKS)
                    .append(parse)
                    .append("(")
                    .append(field)
                    .append(")")
                    .append(DOUBLE_QUOTATION_MARKS);
            return queryField.toString();
        } else {
            return null;
        }
    }

    /**
     * 构造 WHERE
     *
     * @param wheres
     * @param ignoreCase
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
                            sb.insert(0,where.getType() + " ");
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
                && StringUtils.equalsAnyIgnoreCase(where.getParamType()
                , ItemFieldTypeEnum.YEAR.getVal()
                , ItemFieldTypeEnum.DATE.getVal()
                , ItemFieldTypeEnum.DATETIME.getVal()
                , ItemFieldTypeEnum.TIME.getVal()
                , ItemFieldTypeEnum.SMART_TIME.getVal()
                , ItemFieldTypeEnum.TIMESTAMP.getVal()
        )) {
            prepareFlag = String.format(DATE_FUNC_TEMPLATE, "?");
        }

        Rel rel = where.getType();
        StringBuilder sb = new StringBuilder();
        sb.append(fieldHandler(where.getField(), ignoreCase));
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
        // 0 20 --> 1 20    20 20 --> 21 40
        int start = page.getOffset() + 1;
        int end = page.getOffset() + page.getLimit();
        return String.format(PAGE_TEMPLATE, end, start);
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
     * @param ignoreCase
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


    private String buildSqlDataType(String fieldName, FieldTypeAdapter adapter, Integer fieldLength, Integer fieldDecimalPoint) {
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


    private List<String> buildAlterTableAddColumn(AlterTableParamCondition.AddTableFieldParam addTableFieldParam, String tableName, boolean ignoreCase)  {

        String fieldType = addTableFieldParam.getFieldType();
        String fieldComment = addTableFieldParam.getFieldComment();
        Integer fieldLength = addTableFieldParam.getFieldLength();
        Integer fieldDecimalPoint = addTableFieldParam.getFieldDecimalPoint();
        String fieldName = fieldHandler(addTableFieldParam.getFieldName(), ignoreCase);


        StringBuilder sb  = new StringBuilder();
        //基本语法
        sb.append("ALTER TABLE " + tableName + " ADD ")
                .append(fieldName).append(" ")
                .append(buildSqlDataType(fieldName, getAdapter(fieldType), fieldLength, fieldDecimalPoint));


        if(addTableFieldParam.getDefaultValue() != null) {
            sb.append(" DEFAULT ").append("'").append(addTableFieldParam.getDefaultValue()).append("'");
        }

        if (addTableFieldParam.getNotNull() != null && addTableFieldParam.getNotNull()) {
            sb.append(" NOT NULL");
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

    private List<String> buildAlterTableModifyColumn(AlterTableParamCondition.UpdateTableFieldParam updateTableFieldParam, String tableName, boolean ignoreCase) {

        List<String> sqlList = new ArrayList<>(3);

        String fieldName = updateTableFieldParam.getFieldName();
        String newFieldName = updateTableFieldParam.getNewFieldName();

        //修改字段名
        if(StringUtils.isNotBlank(newFieldName)) {
            sqlList.add(String.format(UPDATE_TABLE_FIELD_NAME,
                    tableName,
                    fieldHandler(fieldName, ignoreCase),
                    fieldHandler(newFieldName, ignoreCase)));
            return sqlList;
        }

        //例子： alter TABLE dat_test alter COLUMN name1 set default '2222', alter COLUMN name1 set not null
        String fieldType = updateTableFieldParam.getFieldType();
        String fieldComment = updateTableFieldParam.getFieldComment();
        Integer fieldLength = updateTableFieldParam.getFieldLength();
        Integer fieldDecimalPoint = updateTableFieldParam.getFieldDecimalPoint();
        String defaultValue = updateTableFieldParam.getDefaultValue();

        StringBuilder sb = new StringBuilder();

//        sb.append("ALTER MODIFY ${tableName} ADD ")
//                .append(fieldName)



        if(fieldType != null) {
            sb.append(buildSqlDataType(fieldName, getAdapter(fieldType), fieldLength, fieldDecimalPoint));
        }


        if(defaultValue != null) {
            sb.append(" DEFAULT ").append("'").append(defaultValue).append("'");
        }

        //约束条件[不为空]
        Boolean notNull = updateTableFieldParam.getNotNull();
        if (notNull != null) {
            if(notNull) {
                sb.append(" NOT NULL");
            }else {
                sb.append(" NULL");
            }
        }

        //有长度说明有进行修改操作
        if(sb.length() > 0) {
            sb.insert(0, "MODIFY " + fieldHandler(fieldName, ignoreCase));
            sqlList.add(String.format(ALTER_TABLE_TEMPLATE, tableName, sb.toString()));
        }


        //注释
        if (StringUtils.isNotBlank(fieldComment)) {
            String setComment = String.format(SET_FIELD_COMMENT_TEMPLATE, tableName, fieldHandler(fieldName, ignoreCase), fieldComment);
            sqlList.add(setComment);
        }

        if(updateTableFieldParam.isPrimaryKey()) {
            sqlList.add(String.format(ADD_PRIMARY_KEY, tableName, fieldHandler(fieldName, ignoreCase)));
        }

        return sqlList;
    }

    /**
     * 解析 修改表 SQL
     *
     * @return
     */
    public AniHandler.AniAlterTable parserAlterTableSql(AlterTableParamCondition alterTableParamCondition, BaseOracleDatabaseInfo databaseInfo, BaseOracleTableInfo tableInfo) {
        AniHandler.AniAlterTable aniAlterTable = new AniHandler.AniAlterTable();
        List<String> delTableFieldParamList = alterTableParamCondition.getDelTableFieldParamList();
        List<AlterTableParamCondition.AddTableFieldParam> addTableFieldParamList = alterTableParamCondition.getAddTableFieldParamList();
        List<AlterTableParamCondition.UpdateTableFieldParam> updateTableFieldParamList = alterTableParamCondition.getUpdateTableFieldParamList();
        List<AlterTableParamCondition.ForeignParam> delForeignParamList = alterTableParamCondition.getDelForeignParamList();
        List<AlterTableParamCondition.ForeignParam> addForeignParamList = alterTableParamCondition.getAddForeignParamList();
        String newTableName = alterTableParamCondition.getNewTableName();

        boolean ignoreCase = databaseInfo.isIgnoreCase();
        String schema = fieldHandler(databaseInfo.getSchema(), ignoreCase);
        String tableName = fieldHandler(tableInfo.getTableName(), ignoreCase);
        tableName = schema + "." + tableName;

        String finalTableName = tableName;

        List<String> sqlList = new ArrayList<>();

        // 构造修改表名sql
        if(StringUtils.isNotBlank(newTableName)) {
            sqlList.add(String.format(UPDATE_TABLE_NAME, tableName, fieldHandler(newTableName, ignoreCase)));
        }


        // 构造删除sql
        StringBuilder delSqlBuilder = new StringBuilder();
        if(CollectionUtils.isNotEmpty(delTableFieldParamList)){
            delSqlBuilder.append("DROP (");
            delTableFieldParamList.forEach(element->{
                delSqlBuilder.append(fieldHandler(element, ignoreCase)).append(",");
            });
            delSqlBuilder.deleteCharAt(delSqlBuilder.length() - 1);
            delSqlBuilder.append(")");
            sqlList.add(String.format(ALTER_TABLE_TEMPLATE, tableName, delSqlBuilder.toString()));
        }

        // 构造字段添加sql
        addTableFieldParamList.forEach(addTableFieldParam -> {
            sqlList.addAll(buildAlterTableAddColumn(addTableFieldParam, finalTableName, ignoreCase));

        });

        // 构造字段修改sql
        updateTableFieldParamList.forEach(updateTableFieldParam -> {
            sqlList.addAll(buildAlterTableModifyColumn(updateTableFieldParam, finalTableName, ignoreCase));
        });

        //新增外键
        if(CollectionUtils.isNotEmpty(addForeignParamList)){
            addForeignParamList.forEach(element->{
                String foreignName = element.getForeignName();
                String foreignKey = element.getForeignKey();
                String referencesTableName = element.getReferencesTableName();
                String referencesField = element.getReferencesField();
                ForeignKeyActionEnum onDeletion = element.getOnDeletion();
                String onDeletionStr = "";
                if(onDeletion != null) {
                    onDeletionStr = " ON DELETE " + onDeletion.val;
                }

                if(StringUtils.isAnyBlank(foreignKey, referencesTableName, referencesField)) {
                    throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION,
                            "参数foreignKey, referencesTableName, referencesField不能为空");
                }
                if(StringUtils.isBlank(foreignName)) {
                    foreignName = getForeignName(foreignKey, referencesTableName, referencesField);
                }
                sqlList.add(String.format(
                        ADD_FOREIGN_KEY,
                        finalTableName,
                        fieldHandler(foreignName, ignoreCase),
                        fieldHandler(foreignKey, ignoreCase),
                        fieldHandler(referencesTableName, ignoreCase),
                        fieldHandler(referencesField, ignoreCase),
                        onDeletionStr));
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
                    foreignName = getForeignName(foreignKey, referencesTableName, referencesField);
                }
                sqlList.add(String.format(DROP_FOREIGN_KEY, finalTableName, fieldHandler(foreignName, ignoreCase)));
            });
        }

        if(alterTableParamCondition.isDelPrimaryKey()) {
            sqlList.add(String.format(DROP_PRIMARY_KEY, tableName));
        }

        aniAlterTable.setSqlList(sqlList);

        return aniAlterTable;
    }

    private String getForeignName(String foreignKey, String referencesTableName, String referencesField) {
        String foreignName = foreignKey + "To" + referencesTableName + referencesField;
        foreignName = foreignName.replaceAll("_", "");
        int length = foreignName.length();
        if(length > 30) {
            foreignName = foreignName.substring(length - 30);
        }
        return foreignName;
    }

    protected FieldTypeAdapter getAdapter(String fieldType) {
        return BaseOracleFieldTypeEnum.findFieldTypeEnum(fieldType);
    }
}
