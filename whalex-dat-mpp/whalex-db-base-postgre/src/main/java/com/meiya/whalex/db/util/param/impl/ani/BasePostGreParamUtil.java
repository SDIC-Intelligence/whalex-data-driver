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
import com.meiya.whalex.db.util.param.AbstractDbModuleParamUtil;
import com.meiya.whalex.exception.BusinessException;
import com.meiya.whalex.exception.ExceptionCode;
import com.meiya.whalex.interior.db.constant.*;
import com.meiya.whalex.interior.db.operation.in.*;
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
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;

import java.sql.Time;
import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * PostGre 参数转换工具类
 *
 * @author 黄河森
 * @date 2019/12/23
 * @project whale-cloud-platformX
 */
@Slf4j
public class BasePostGreParamUtil<Q extends AniHandler, D extends BasePostGreDatabaseInfo,
        T extends BasePostGreTableInfo> extends AbstractDbModuleParamUtil<Q, D, T> {

    protected static final String DISTRIBUTED_KEY = " DISTRIBUTED BY "; // PG 分布表key
    protected static String DOUBLE_QUOTATION_MARKS = "\"";
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
    public final static KeyWordHandler keyWordHandler = new KeywordConstant();


    protected String getMarks() {
        return DOUBLE_QUOTATION_MARKS;
    }
    protected String getDistributedKey() {
        return DISTRIBUTED_KEY;
    }


    /**
     * 创建序列模板
     */
    private final static String CREATE_SEQUENCE_TEMPLATE = "create sequence %s start with %s increment by %s no minvalue no maxvalue no cycle";

    /**
     * 创建on update 函数sql
     */
    private final static String CREATE_ON_UPDATE_FUNC_TEMPLATE = "create or replace function %s() returns trigger as\n" +
            "$$\n" +
            "BEGIN\n" +
            "\tnew.%s = CURRENT_TIMESTAMP;\n" +
            "\tRETURN new;\n" +
            "END\n" +
            "$$\n" +
            "LANGUAGE plpgsql;";

    private final static String CREATE_TRIGGER_TEMPLATE = "CREATE TRIGGER %s BEFORE UPDATE ON %s FOR EACH ROW EXECUTE PROCEDURE %s()";

    private final static String DROP_TRIGGER_TEMPLATE = "DROP TRIGGER IF EXISTS %s  ON %s";

    /**
     * 建表模板
     */
    protected final static String CREATE_TABLE_TEMPLATE = "CREATE TABLE %s ( %s )";

    /**
     * like建表模板
     */
    protected final static String CREATE_TABLE_LIKE_TEMPLATE = "CREATE TABLE %s (LIKE %s INCLUDING CONSTRAINTS INCLUDING INDEXES INCLUDING COMMENTS INCLUDING DEFAULTS)";

    /**
     * 修改表模板
     * ALTER TABLE ppz_test DROP COLUMN IF EXISTS name,
     * DROP COLUMN IF EXISTS pass;
     */
    protected final static String ALTER_TABLE_TEMPLATE = "ALTER TABLE %s %s";

    /**
     *新增表外键
     */
    protected final static String ADD_FOREIGN_KEY = "ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY(%s) REFERENCES %s(%s) ON DELETE %s ON UPDATE %s";

    /**
     *新增表外键
     */
    protected final static String DROP_FOREIGN_KEY = "ALTER TABLE %s DROP CONSTRAINT %s";


    /**
     * list分区表
     */
    protected final static String CREATE_TABLE_PARTITION_LIST = "CREATE TABLE %s  PARTITION OF %s FOR VALUES IN (%s)";

    /**
     * range分区表
     */
    protected final static String CREATE_TABLE_PARTITION_RANGE = "CREATE TABLE %s  PARTITION OF %s FOR VALUES FROM (%s) TO (%s)";

    /**
     * hash分区表
     */
    protected final static String CREATE_TABLE_PARTITION_HASH = "CREATE TABLE %s  PARTITION OF %s FOR VALUES WITH (MODULUS %s, REMAINDER %s)";

    /**
     * 插入模板
     */
    protected final static String INSERT_DATA_TEMPLATE = "INSERT INTO %s ( %s ) VALUES %s";

    /**
     * 删除模板
     */
    protected final static String DELETE_DATA_TEMPLATE = "DELETE FROM %s %s";

    /**
     * 更新模板
     */
    protected final static String UPDATE_DATA_TEMPLATE = "UPDATE %s SET %s";

    /**
     * 创建索引
     */
    protected final static String CREATE_INDEX_TEMPLATE = "CREATE %s INDEX \"%s\" ON %s(%s)";


    protected final static String CREATE_INDEX_IF_NOT_EXISTS_TEMPLATE = "CREATE %s INDEX IF NOT EXISTS \"%s\" ON %s(%s)";

    /**
     * 删除索引
     */
    private final static String DROP_INDEX_TEMPLATE = "DROP INDEX %s.\"%s\"";

    /**
     * 设置字段备注模板
     */
    protected final static String SET_FIELD_COMMENT_TEMPLATE = "COMMENT ON COLUMN %s.%s IS '%s'";

    /**
     * 设置字段备注模板
     */
    protected final static String SET_TABLE_COMMENT_TEMPLATE = "COMMENT ON TABLE %s IS '%s'";

    /**
     * 圆形区域范围查询
     */
    protected final static String GEO_DISTANCE_TEMPLATE = "circle'((%s,%s),%s)' @> %s";

    /**
     * 新增或者更新
     */
    protected final static String UPSERT_DATA_TEMPLATE = "INSERT INTO %s (%s) VALUES %s ON CONFLICT(%s) DO UPDATE SET %s";

    /**
     * 数组更新
     */
    protected final static String ARRAY_UPDATE_SET = "CASE $value = ANY($column) WHEN TRUE THEN $column ELSE array_append($column, $value) END";

    /**
     * 数组追加
     */
    protected final static String ARRAY_UPDATE_APPEND = "array_cat($column, $value)";

    /**
     *新增表主键
     */
    protected final static String ADD_PRIMARY_KEY = "ALTER TABLE %s ADD PRIMARY KEY(%s)";

    /**
     * 修改字段名称
     */
    protected final static String UPDATE_TABLE_FIELD_NAME = "ALTER TABLE %s RENAME  %s TO %s";

    /**
     * 修改表名
     */
    protected final static String UPDATE_TABLE_NAME = "ALTER TABLE %s RENAME TO %s";

    /**
     * 坐标
     */
    protected final static String POINT_TEMPLATE = "point(%s,%s)";

    protected final static String DROP_PARTITION_TABLE = "drop table %s";

    /**
     * 删除字段约束
     */
    protected final static String DROP_COLUMN_CONSTRAINT = "ALTER TABLE %s ALTER COLUMN %s DROP %s";

    protected String getCreateTableLikeSqlTmp() {
        return CREATE_TABLE_LIKE_TEMPLATE;
    }

    @Override
    protected Q transitionListTableParam(QueryTablesCondition queryTablesCondition, D databaseConf) throws Exception {
        AniHandler aniHandler = new AniHandler();
        AniHandler.AniListTable aniListTable = new AniHandler.AniListTable();
        aniHandler.setListTable(aniListTable);
        String tableMatch = queryTablesCondition.getTableMatch();
        tableMatch = StringUtils.replaceEach(tableMatch, new String[]{"*", "?"}, new String[]{"%", "_"});
        aniListTable.setTableMatch(tableMatch);
        return (Q) aniHandler;
    }

    @Override
    protected Q transitionListDatabaseParam(QueryDatabasesCondition queryDatabasesCondition, D databaseConf) throws Exception {
        AniHandler aniHandler = new AniHandler();
        AniHandler.AniListDatabase aniListDatabase = new AniHandler.AniListDatabase();
        aniHandler.setListDatabase(aniListDatabase);
        String databaseMatch = queryDatabasesCondition.getDatabaseMatch();
        databaseMatch = StringUtils.replaceEach(databaseMatch, new String[]{"*", "?"}, new String[]{"%", "_"});
        aniListDatabase.setDatabaseMatch(databaseMatch);
        return (Q) aniHandler;
    }

    @Override
    protected Q transitionCreateTableParam(CreateTableParamCondition createTableParamCondition, D databaseConf, T tableConf) {
        AniHandler aniHandler = new AniHandler();
        aniHandler.setAniCreateTable(parserCreateTableSql(createTableParamCondition, databaseConf, tableConf));
        return (Q) aniHandler;
    }

    @Override
    protected Q transitionAlterTableParam(AlterTableParamCondition alterTableParamCondition, D databaseConf, T tableConf) throws Exception {
        AniHandler aniHandler = new AniHandler();
        aniHandler.setAniAlterTable(parserAlterTableSql(alterTableParamCondition, databaseConf, tableConf));
        return (Q) aniHandler;
    }

    @Override
    protected Q transitionCreateIndexParam(IndexParamCondition indexParamCondition, D databaseConf, T tableConf) {
        AniHandler aniHandler = new AniHandler();
        AniHandler.AniCreateIndex aniCreateIndex = parserCreateIndexSql(indexParamCondition, databaseConf, tableConf);
        aniHandler.setAniCreateIndex(aniCreateIndex);
        return (Q) aniHandler;
    }

    @Override
    protected Q transitionDropIndexParam(IndexParamCondition indexParamCondition, D databaseConf, T tableConf) {
        AniHandler aniHandler = new AniHandler();
        AniHandler.AniDropIndex aniDropIndex = parserDropIndexSql(indexParamCondition, databaseConf, tableConf);
        aniHandler.setAniDropIndex(aniDropIndex);
        return (Q) aniHandler;
    }

    @Override
    protected Q transitionQueryParam(QueryParamCondition queryParamCondition, D databaseConf, T tableConf) {
        AniHandler aniHandler = new AniHandler();
        AniHandler.AniQuery aniQuery;
        // 如果存在自定义SQL，则不需要转换
//        String sql = queryParamCondition.getSql();
//        if (StringUtils.isNotBlank(sql)) {
//            aniQuery = new AniHandler.AniQuery();
//            aniQuery.setSql(sql);
//        } else {
//        }
        aniQuery = parserQuerySql(queryParamCondition, databaseConf, tableConf);
        aniQuery.setCount(queryParamCondition.isCountFlag());
        if(queryParamCondition.getBatchSize() != null){
            aniQuery.setBatchSize(queryParamCondition.getBatchSize());
        }
        aniHandler.setAniQuery(aniQuery);
        return (Q) aniHandler;
    }

    @Override
    protected Q transitionUpdateParam(UpdateParamCondition updateParamCondition, D databaseConf, T tableConf) {
        AniHandler aniHandler = new AniHandler();
        AniHandler.AniUpdate aniUpdate = parserUpdateSql(updateParamCondition, databaseConf, tableConf);
        aniHandler.setAniUpdate(aniUpdate);
        return (Q) aniHandler;
    }

    @Override
    protected Q transitionInsertParam(AddParamCondition addParamCondition, D databaseConf, T tableConf) {
        AniHandler aniHandler = new AniHandler();
        AniHandler.AniInsert aniInsert = parserInsertSql(addParamCondition, databaseConf, tableConf);
        aniHandler.setAniInsert(aniInsert);
        return (Q) aniHandler;
    }

    @Override
    protected Q transitionDeleteParam(DelParamCondition delParamCondition, D databaseConf, T tableConf) {
        AniHandler aniHandler = new AniHandler();
        aniHandler.setAniDel(parserDelSql(delParamCondition, databaseConf, tableConf));
        return (Q) aniHandler;
    }

    @Override
    protected Q transitionDropTableParam(DropTableParamCondition dropTableParamCondition, D databaseConf, T tableConf) throws Exception {
        AniHandler.AniDropTable dropTable = new AniHandler.AniDropTable();
        dropTable.setIfExists(dropTableParamCondition.isIfExists());
        AniHandler aniHandler = new AniHandler();
        aniHandler.setAniDropTable(dropTable);
        return (Q) aniHandler;
    }

    @Override
    public Q transitionUpsertParam(UpsertParamCondition paramCondition, D databaseConf, T tableConf) throws Exception {
        AniHandler aniHandler = new AniHandler();
        aniHandler.setAniUpsert(parserUpsertSql(paramCondition, databaseConf, tableConf));
        return (Q) aniHandler;
    }

    @Override
    public Q transitionUpsertParamBatch(UpsertParamBatchCondition paramBatchCondition, D databaseConf, T tableConf) throws Exception {
        AniHandler aniHandler = new AniHandler();
        aniHandler.setAniUpsertBatch(parserUpsertBatchSql(paramBatchCondition, databaseConf, tableConf));
        return (Q) aniHandler;
    }

    protected FieldTypeAdapter getAdapter(String fieldType) {
        return BasePostGreFieldTypeEnum.findFieldTypeEnum(fieldType);
    }

    /**
     * 构建聚合查询
     * @param paramCondition
     * @param tableName
     * @param ignoreCase
     * @return
     */
    public AniHandler.AniQuery buildAggQuerySql(QueryParamCondition paramCondition, String tableName, boolean ignoreCase, boolean likeToiLike) {
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

        //where
        if (CollectionUtils.isNotEmpty(paramCondition.getWhere())) {
            List<Object> paramList = new LinkedList<>();
            sb.append(buildWhereSql(paramCondition.getWhere(), paramList, ignoreCase, likeToiLike));
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
     *  @param aggs
     * @param selectSb
     * @param groupSb
     * @param orderList
     * @param aggFunctionList
     * @param ignoreCase
     */
    public void aggResolver(Aggs aggs, StringBuilder selectSb, StringBuilder groupSb, List<Order> orderList, List<AggFunction> aggFunctionList, boolean ignoreCase) {
        AggOpType type = aggs.getType();
        if(!type.equals(AggOpType.GROUP) && !type.equals(AggOpType.TERMS)) {
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
     * @param databaseConf
     * @param tableConf
     * @return
     */
    public AniHandler.AniQuery parserQuerySql(QueryParamCondition paramCondition, D databaseConf, T tableConf) {

        //是否忽略大小写
        boolean ignoreCase = databaseConf.isIgnoreCase();
        boolean likeToiLike = databaseConf.isLikeToiLike();
        //表名
        String tableName = getTableName(databaseConf.getSchema(), tableConf.getTableName(), ignoreCase);

        if(CollectionUtils.isNotEmpty(paramCondition.getAggList())) {
            return buildAggQuerySql(paramCondition, tableName, ignoreCase, likeToiLike);
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
            whereSql = buildWhereSql(paramCondition.getWhere(), paramList, ignoreCase, likeToiLike);
        }

        for (int i= 0 ; i < unionIndex ; i ++){
            // 公用sql部分
            StringBuilder sbCom = new StringBuilder();
            if(i != 0){
                sb.append(" union all ");
                sbCount.append(" union all ");
            }
            sb.append("select ");
            sb.append(buildSelectSql(paramCondition.getSelect(), ignoreCase));
            sb.append(" from ");
            sbCount.append("select count(1) as count from ");
            // 表名
            // 先使用占位符
            sbCom.append(tableName);
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

        // 设置分页信息
        aniQuery.setLimit(paramCondition.getPage().getLimit());
        aniQuery.setOffset(paramCondition.getPage().getOffset());

        return aniQuery;
    }

    private String buildSqlDataType(FieldTypeAdapter adapter, Integer fieldLength, Integer fieldDecimalPoint, boolean tinyInt1isBit) {

        //boolean 转 int
        if(!tinyInt1isBit) {
            if(adapter == BasePostGreFieldTypeEnum.BOOLEAN) {
                adapter = BasePostGreFieldTypeEnum.INT2;
            }
        }

        StringBuilder columnSb = new StringBuilder(adapter.getDbFieldType());

        // 判断是否需要长度
        if (!ItemFieldTypeEnum.ParamStatus.NO.equals(adapter.getNeedDataLength())) {
            // 如果当前没有传入长度，并且必须需要长度，则取默认值
            if (fieldLength == null && ItemFieldTypeEnum.ParamStatus.MUST.equals(adapter.getNeedDataLength())) {
                fieldLength = adapter.getFiledLength();
            }
            if (fieldLength != null) {
                // 包含 [] 代表数组
                if (StringUtils.containsIgnoreCase(adapter.getDbFieldType(), "[]")) {
                    columnSb.insert(columnSb.lastIndexOf("["), "(")
                        .insert(columnSb.lastIndexOf("["), fieldLength)
                        .insert(columnSb.lastIndexOf("["), ")");
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
                        // 包含 [] 代表数组
                        if (StringUtils.containsIgnoreCase(adapter.getDbFieldType(), "[]")) {
                            columnSb.insert(columnSb.lastIndexOf(")"), ",")
                                    .insert(columnSb.lastIndexOf(")"), fieldDecimalPoint);
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


    private List<String> buildAlterTableAddColumn(AlterTableParamCondition.AddTableFieldParam addTableFieldParam, String schema, String rowTableName, String tableName, boolean ignoreCase, boolean tinyInt1isBit)  {

        String fieldType = addTableFieldParam.getFieldType();
        String fieldComment = addTableFieldParam.getFieldComment();
        Integer fieldLength = addTableFieldParam.getFieldLength();
        Integer fieldDecimalPoint = addTableFieldParam.getFieldDecimalPoint();
        boolean isNotExists = addTableFieldParam.isNotExists();
        String fieldName = fieldHandler(addTableFieldParam.getFieldName(), ignoreCase);
        StringBuilder sb  = new StringBuilder();
        //基本语法
        sb.append("ALTER TABLE ").append(tableName).append(" ADD COLUMN ");

        if(isNotExists) {
            sb.append("IF NOT EXISTS ");
        }

        sb.append(fieldName).append(SYMBOL_REPLACE)
                .append(buildSqlDataType(getAdapter(fieldType), fieldLength, fieldDecimalPoint, tinyInt1isBit));


        if (addTableFieldParam.getNotNull() != null && addTableFieldParam.getNotNull()) {
            sb.append(" NOT NULL");
        }

        if(addTableFieldParam.getDefaultValue() != null) {
            sb.append(" ").append(buildDefaultValue(fieldType, addTableFieldParam.getDefaultValue()));
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

        List<String> funcSqlList = getFuncSqlList(addTableFieldParam.getOnUpdate(),
                schema, rowTableName, addTableFieldParam.getFieldName(), ignoreCase, false);

        sqlList.addAll(funcSqlList);

        return sqlList;
    }

    private List<String> getFuncSqlList(String onUpdate, String schema, String tableName, String fieldName, boolean ignoreCase, boolean isDelete) {
        List<String> sqlList = new ArrayList<>();
        String triggerName = fieldHandler(tableName + "_" + fieldName + "_trigger", ignoreCase);
        if(StringUtils.isNotBlank(onUpdate)) {
            String funcName = getTableName(schema, fieldName + "_timestamp", ignoreCase);
            String funcSql = String.format(CREATE_ON_UPDATE_FUNC_TEMPLATE, funcName, fieldName);
            sqlList.add(funcSql);
            String triggerSql = String.format(CREATE_TRIGGER_TEMPLATE, triggerName, getTableName(schema, tableName, ignoreCase), funcName);
            sqlList.add(triggerSql);
        }else if(isDelete) {
            String dropTriggerSql = String.format(DROP_TRIGGER_TEMPLATE, triggerName, getTableName(schema, tableName, ignoreCase));
            sqlList.add(dropTriggerSql);
        }
        return sqlList;
    }

    private List<String> buildAlterTableModifyColumn(AlterTableParamCondition.UpdateTableFieldParam updateTableFieldParam, String schema, String rowTableName, String tableName, boolean ignoreCase, boolean tinyInt1isBit) {

        List<String> sqlList = new ArrayList<>(3);

        String fieldName = fieldHandler(updateTableFieldParam.getFieldName(), ignoreCase);
        String newFieldName = updateTableFieldParam.getNewFieldName();

        //修改字段名
        if(StringUtils.isNotBlank(newFieldName)) {
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
        StringBuilder sb = new StringBuilder();
        boolean dropFlag = updateTableFieldParam.isDropFlag();
        if(dropFlag) {
            boolean dropDefaultValue = updateTableFieldParam.isDropDefaultValue();
            boolean dropNotNull = updateTableFieldParam.isDropNotNull();
            if(dropDefaultValue) {
                String sql = String.format(DROP_COLUMN_CONSTRAINT, tableName, fieldName, "DEFAULT");
                sqlList.add(sql);
            }
            if(dropNotNull) {
                String sql = String.format(DROP_COLUMN_CONSTRAINT, tableName, fieldName, "NOT NULL");
                sqlList.add(sql);
            }
            return sqlList;
        }

        if(fieldType != null) {
            FieldTypeAdapter adapter = getAdapter(fieldType);
            String sqlDataType = buildSqlDataType(adapter, fieldLength, fieldDecimalPoint, tinyInt1isBit);
            sb.append(",").append("ALTER COLUMN ")
                    .append(fieldName).append(" TYPE ")
                    .append(sqlDataType);

            // PG 修改字段类型时需要使用 USING 语句进行强制转换
            sb.append(" USING ").append(fieldName).append("::").append(sqlDataType);
        }

        //约束条件[不为空]
        Boolean notNull = updateTableFieldParam.getNotNull();
        if (notNull != null) {
            if(notNull) {
                sb.append(",").append("ALTER COLUMN ").append(fieldName).append(" SET NOT NULL");
            }else {
                sb.append(",").append("ALTER COLUMN ").append(fieldName).append(" DROP NOT NULL");
            }
        }

        //约束条件[默认值]
        if(defaultValue != null) {
            sb.append(",").append("ALTER COLUMN ").append(fieldName).append(" SET ").append(buildDefaultValue(fieldType, defaultValue));
        }

        //有长度说明有进行修改操作
        if(sb.length() > 0) {
            String updateSql = sb.replace(0, 1, "").toString();

            sqlList.add(String.format(ALTER_TABLE_TEMPLATE, tableName, updateSql));
        }

        //注释
        if (StringUtils.isNotBlank(fieldComment)) {
            String setComment = String.format(SET_FIELD_COMMENT_TEMPLATE, tableName, fieldName, fieldComment);
            sqlList.add(setComment);
        }

        if(updateTableFieldParam.isPrimaryKey()) {
            sqlList.add(String.format(ADD_PRIMARY_KEY, tableName, fieldName));
        }

        List<String> funcSqlList = getFuncSqlList(updateTableFieldParam.getOnUpdate(),
                schema, rowTableName, updateTableFieldParam.getFieldName(), ignoreCase, true);

        sqlList.addAll(funcSqlList);
        return sqlList;
    }

    /**
     * 解析 修改表 SQL
     *
     * @return
     */
    private AniHandler.AniAlterTable parserAlterTableSql(AlterTableParamCondition alterTableParamCondition, D databaseConf, T tableConf) {

        //是否忽略大小写
        boolean ignoreCase = databaseConf.isIgnoreCase();
        //表名
        String tableName = getTableName(databaseConf.getSchema(), tableConf.getTableName(), ignoreCase);

        AniHandler.AniAlterTable aniAlterTable = new AniHandler.AniAlterTable();
        List<String> delTableFieldParamList = alterTableParamCondition.getDelTableFieldParamList();
        List<AlterTableParamCondition.AddTableFieldParam> addTableFieldParamList = alterTableParamCondition.getAddTableFieldParamList();
        List<AlterTableParamCondition.UpdateTableFieldParam> updateTableFieldParamList = alterTableParamCondition.getUpdateTableFieldParamList();
        List<AlterTableParamCondition.ForeignParam> addForeignParamList = alterTableParamCondition.getAddForeignParamList();
        List<AlterTableParamCondition.ForeignParam> delForeignParamList = alterTableParamCondition.getDelForeignParamList();
        PartitionInfo addPartition = alterTableParamCondition.getAddPartition();
        PartitionInfo delPartition = alterTableParamCondition.getDelPartition();
        String newTableName = alterTableParamCondition.getNewTableName();
        String tableComment = alterTableParamCondition.getTableComment();

        List<String> sqlList = new ArrayList<>();

        // 构造修改表名sql
        if(StringUtils.isNotBlank(newTableName)) {
            newTableName = fieldHandler(newTableName, ignoreCase);
            sqlList.add(String.format(UPDATE_TABLE_NAME, tableName, newTableName));
        }

        // 修改表描述
        if(StringUtils.isNotBlank(tableComment)) {
            sqlList.add(String.format(SET_TABLE_COMMENT_TEMPLATE, tableName, tableComment));
        }


        // 构造删除sql
        List<String> funcSqlList = new ArrayList<>();
        StringBuilder delSqlBuilder = new StringBuilder();
        if(CollectionUtils.isNotEmpty(delTableFieldParamList)){
            delTableFieldParamList.forEach(element->{
                funcSqlList.addAll(getFuncSqlList(null, databaseConf.getSchema(), tableConf.getTableName(), element, ignoreCase, true));
                delSqlBuilder.append("DROP COLUMN IF EXISTS ").append(fieldHandler(element, ignoreCase)).append(",");
            });
        }
        if (delSqlBuilder.length() != 0){
            delSqlBuilder.deleteCharAt(delSqlBuilder.length() - 1).append(";");
            sqlList.add(String.format(ALTER_TABLE_TEMPLATE, tableName, delSqlBuilder.toString()));
            sqlList.addAll(funcSqlList);
        }

        // 构造字段添加sql
        addTableFieldParamList.forEach(addTableFieldParam -> {
            sqlList.addAll(buildAlterTableAddColumn(addTableFieldParam, databaseConf.getSchema(), tableConf.getTableName(), tableName, ignoreCase, databaseConf.isTinyInt1isBit()));

        });

        // 构造字段修改sql
        updateTableFieldParamList.forEach(updateTableFieldParam -> {
            sqlList.addAll(buildAlterTableModifyColumn(updateTableFieldParam, databaseConf.getSchema(), tableConf.getTableName(), tableName, ignoreCase, databaseConf.isTinyInt1isBit()));
        });

        //添加分区
        if(addPartition != null) {
            sqlList.addAll(buildAddPartitionTable(addPartition, databaseConf, tableConf));
        }

        //删除分区
        if(delPartition != null) {
            sqlList.addAll(buildDelPartitionTable(delPartition, databaseConf, tableConf));
        }



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
                    onDeletion = ForeignKeyActionEnum.NO_ACTION;
                }
                if(onUpdate == null) {
                    onUpdate = ForeignKeyActionEnum.NO_ACTION;
                }
                if(StringUtils.isAnyBlank(foreignKey, referencesTableName, referencesField)) {
                    throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION,
                            "参数foreignKey, referencesTableName, referencesField不能为空");
                }
                if(StringUtils.isBlank(foreignName)) {
                    foreignName = foreignKey + "_to_" + referencesTableName + "_" + referencesField;
                }

                referencesTableName = getTableName(databaseConf.getSchema(), referencesTableName, ignoreCase);

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
                sqlList.add(String.format(DROP_FOREIGN_KEY, tableName, fieldHandler(foreignName, ignoreCase)));
            });
        }

        //是否删除主键
        aniAlterTable.setDelPrimaryKey(alterTableParamCondition.isDelPrimaryKey());
        aniAlterTable.setSqlList(sqlList);

        return aniAlterTable;
    }

    protected List<String> buildDelPartitionTable(PartitionInfo partitionInfo, D databaseConf, T tableConf) {

        //是否忽略大小写
        boolean ignoreCase = databaseConf.isIgnoreCase();
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
                list.add(String.format(DROP_PARTITION_TABLE,
                        getTableName(databaseConf.getSchema(), partitionName, ignoreCase)));
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
                String partitionTableName = getTableName(databaseConf.getSchema(), partitionName, ignoreCase);
                list.add(String.format(DROP_PARTITION_TABLE, partitionTableName));
            }
            return list;
        }

        if(PartitionType.HASH.equals(type)) {
            List<PartitionInfo.PartitionHash> hash = partitionInfo.getHash();
            if(CollectionUtil.isEmpty(hash)) {
                return list;
            }

            for (int i = 0; i < hash.size(); i++) {
                PartitionInfo.PartitionHash partition = hash.get(i);
                String partitionName = partition.getPartitionName();
                if(StringUtils.isBlank(partitionName)) {
                    throw new RuntimeException("分区表名称不能为空");
                }
                String partitionTableName = getTableName(databaseConf.getSchema(), partitionName, ignoreCase);
                list.add(String.format(DROP_PARTITION_TABLE, partitionTableName));
            }
            return list;
        }
        throw new RuntimeException("未知的分区表类型[" + type + "]");
    }

    protected List<String> buildAddPartitionTable(PartitionInfo partitionInfo, D databaseConf, T tableConf) {
        //是否忽略大小写
        boolean ignoreCase = databaseConf.isIgnoreCase();
        String partitionField = partitionInfo.getPartitionField();
        if(StringUtils.isNotBlank(partitionField)) {
            partitionField = fieldHandler(partitionField, ignoreCase);
        }
        return parserPartitionSql(partitionInfo, databaseConf, tableConf, partitionField);
    }

    /**
     * 解析 建表 SQL
     *
     * @return
     */
    public AniHandler.AniCreateTable parserCreateTableSql(CreateTableParamCondition createTableParamCondition, D databaseConf, T tableConf) {

        //是否忽略大小写
        boolean ignoreCase = databaseConf.isIgnoreCase();
        AniHandler.AniCreateTable createTable = new AniHandler.AniCreateTable();
        //表名
        String tableName = getTableName(databaseConf.getSchema(), tableConf.getTableName(), ignoreCase);
        CreateTableLikeParam createTableLike = createTableParamCondition.getCreateTableLike();
        if (createTableLike != null) {
            String copyTableName = createTableLike.getCopyTableName();
            if (StringUtils.isNotBlank(copyTableName)) {
                copyTableName = getTableName(databaseConf.getSchema(), copyTableName, ignoreCase);
                String sql = String.format(getCreateTableLikeSqlTmp(), tableName, copyTableName);
                createTable.setNotExists(createTableParamCondition.isNotExists());
                createTable.setSql(sql);
                return createTable;
            }
        }

        List<CreateTableParamCondition.CreateTableFieldParam> createTableFieldParamList = createTableParamCondition.getCreateTableFieldParamList();
        StringBuilder createSqlBuilder = new StringBuilder();
        // 存放分布键
        Set<String> distributedList = new LinkedHashSet<>();
        List<String> fieldCommentList = new ArrayList<>();
        //存放序列sql
        List<String> sequenceSqlList = new ArrayList();

        List<String> funcSqlList = new ArrayList<>();

        String partitionKey = null;
        String partitionType = null;
        List<String> primaryKeys = new ArrayList<>();
        for (CreateTableParamCondition.CreateTableFieldParam createTableFieldParam : createTableFieldParamList) {

            String fieldType = createTableFieldParam.getFieldType();
            String fieldComment = createTableFieldParam.getFieldComment();
            Integer fieldLength = createTableFieldParam.getFieldLength();
            Integer fieldDecimalPoint = createTableFieldParam.getFieldDecimalPoint();
            String fieldName = fieldHandler(createTableFieldParam.getFieldName(), ignoreCase);

            if (createTableFieldParam.isDistributed()) {
                distributedList.add(fieldName);
            }

            if(createTableFieldParam.isPartition()) {
                partitionKey = fieldName;
                PartitionInfo partitionInfo = createTableFieldParam.getPartitionInfo();
                if(partitionInfo == null) {
                    throw new RuntimeException("分区表信息不能空");
                }
                List<String> partitionSql = parserPartitionSql(partitionInfo, databaseConf, tableConf, fieldName);
                createTable.setPartitionSqlList(partitionSql);
                partitionType = partitionInfo.getType().name();
            }

            createSqlBuilder.append(fieldName)
                    .append(SYMBOL_REPLACE)
                    .append(buildSqlDataType(getAdapter(fieldType), fieldLength, fieldDecimalPoint, databaseConf.isTinyInt1isBit()));

            if (createTableFieldParam.isNotNull()) {
                createSqlBuilder.append(" NOT NULL");
            }

            if(createTableFieldParam.getDefaultValue() != null) {
                String defaultValue = buildDefaultValue(fieldType, createTableFieldParam.getDefaultValue());
                createSqlBuilder.append(" " + defaultValue);
            }

            funcSqlList.addAll(getFuncSqlList(createTableFieldParam.getOnUpdate(),
                    databaseConf.getSchema(), tableConf.getTableName(), createTableFieldParam.getFieldName(), ignoreCase, false));

            //主键自增 使用序列+默认值实现
            if(createTableFieldParam.isAutoIncrement()) {
                Long startIncrement = createTableFieldParam.getStartIncrement() == null ? 1L : createTableFieldParam.getStartIncrement();
                Long incrementBy = createTableFieldParam.getIncrementBy() == null ? 1L : createTableFieldParam.getIncrementBy();
                //生成序列名
                String sequenceName = tableConf.getTableName() + "_" + createTableFieldParam.getFieldName();
                sequenceName = getTableName(databaseConf.getSchema(), sequenceName, ignoreCase);
                sequenceSqlList.add(String.format(CREATE_SEQUENCE_TEMPLATE, sequenceName, startIncrement, incrementBy));
                createSqlBuilder.append(" DEFAULT ").append("NEXTVAL('"+sequenceName+"')");
            }

            if (StringUtils.isNotBlank(fieldComment)) {
                String setComment = String.format(SET_FIELD_COMMENT_TEMPLATE, tableName, fieldName, fieldComment);
                fieldCommentList.add(setComment);
            }
            if (createTableFieldParam.isPrimaryKey()) {
                primaryKeys.add(fieldName);
            }
            createSqlBuilder.append(",");

        }

        // 若存在主键，则 拼接 PRIMARY KEY
        if (CollectionUtil.isNotEmpty(primaryKeys)) {
            createSqlBuilder.append("PRIMARY KEY (");
            for (String primaryKey : primaryKeys) {
                createSqlBuilder.append(primaryKey).append(",");
            }
            createSqlBuilder.deleteCharAt(createSqlBuilder.length() - 1);
            createSqlBuilder.append(")");
        } else {
            //去逗号
            createSqlBuilder.deleteCharAt(createSqlBuilder.length() - 1);
        }

        String createTableSql = String.format(CREATE_TABLE_TEMPLATE, tableName, createSqlBuilder.toString());

        // 判断是否存在分布键
        if (CollectionUtils.isNotEmpty(distributedList)) {
            StringBuilder createTableSqlBuilder = new StringBuilder(createTableSql);
                createTableSqlBuilder.append(getDistributedKey() + "(");
            for (String distributed : distributedList) {
                createTableSqlBuilder
                        .append(distributed)
                        .append(",");
            }
            createTableSqlBuilder.deleteCharAt(createTableSqlBuilder.length() - 1);
            createTableSqlBuilder.append(")");
            createTableSql = createTableSqlBuilder.toString();
        }

        if(StringUtils.isNotBlank(partitionKey)) {
            createTableSql = buildCreatePartitionTableSql(
                    createTableSql,
                    partitionType,
                    partitionKey,
                    createTable.getPartitionSqlList()
            );
        }

        createTable.setSql(createTableSql);

        //字段备注
        if (CollectionUtils.isNotEmpty(fieldCommentList)) {
            createTable.setFieldCommentList(fieldCommentList);
        }

        //表备注
        String tableComment = createTableParamCondition.getTableComment();
        if (StringUtils.isNotBlank(tableComment)) {
            String setComment = String.format(SET_TABLE_COMMENT_TEMPLATE, tableName, tableComment);
            createTable.setTableComment(setComment);
        }

        //序列
        createTable.setSequenceSqlList(sequenceSqlList);
        createTable.setOnUpdateFuncSqlList(funcSqlList);
        createTable.setNotExists(createTableParamCondition.isNotExists());
        return createTable;
    }

    protected String buildDefaultValue(String fieldType, String value) {

        if(value.equalsIgnoreCase("null")) return "DEFAULT NULL";

        if(StringUtils.isBlank(value)) {
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

    protected String buildCreatePartitionTableSql(String createTableSql,
                                                  String partitionType,
                                                  String partitionKey,
                                                  List<String> partitionSqlList){
        StringBuilder sb = new StringBuilder();
        sb.append(createTableSql).append(" PARTITION BY ").append(partitionType).append("(").append(partitionKey).append(")");
        return sb.toString();
    }

    protected List<String> parserPartitionSql(PartitionInfo partitionInfo,
                                            D databaseConf,
                                            T tableConf,
                                            String fieldName) {

        //是否忽略大小写
        boolean ignoreCase = databaseConf.isIgnoreCase();
        //表名
        String tableName = getTableName(databaseConf.getSchema(), tableConf.getTableName(), ignoreCase);
        
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
                String partitionTableName = getTableName(databaseConf.getSchema(), partitionName, ignoreCase);
                List<String> inParams = partition.getList();
                if(CollectionUtil.isEmpty(inParams)) {
                    throw new RuntimeException("LIST类型分区表，参数不能为空");
                }

                StringBuilder sb = new StringBuilder();
                sb.append("'").append(inParams.get(0)).append("'");
                for (int i = 1; i < inParams.size(); i++) {
                    sb.append(", '").append(inParams.get(i)).append("'");
                }
                String sql = String.format(CREATE_TABLE_PARTITION_LIST, partitionTableName, tableName, sb.toString());
                list.add(sql);
                if(StringUtils.isNotBlank(fieldName)) {
                    String indexName = "index_" + partitionName + "_" + fieldName.replaceAll(DOUBLE_QUOTATION_MARKS, "");
                    String indexSql = String.format(CREATE_INDEX_TEMPLATE, "", indexName, partitionTableName, fieldName);
                    list.add(indexSql);
                }

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
                String partitionTableName = getTableName(databaseConf.getSchema(), partitionName, ignoreCase);
                String left = partition.getLeft();
                String right = partition.getRight();
                left = StringUtils.isBlank(left) ? "MINVALUE" : "'" + left + "'";
                right = StringUtils.isBlank(right) ? "MAXVALUE" : "'" + right + "'";
                String sql = String.format(CREATE_TABLE_PARTITION_RANGE, partitionTableName, tableName, left, right);
                list.add(sql);
                if(StringUtils.isNotBlank(fieldName)) {
                    String indexName = "index_" + partitionName + "_" + fieldName.replaceAll(DOUBLE_QUOTATION_MARKS, "");
                    String indexSql = String.format(CREATE_INDEX_TEMPLATE, "", indexName, partitionTableName, fieldName);
                    list.add(indexSql);
                }
            }
            return list;
        }

        if(PartitionType.HASH.equals(type)) {
            List<PartitionInfo.PartitionHash> hash = partitionInfo.getHash();
            if(CollectionUtil.isEmpty(hash)) {
                return list;
            }

            for (int i = 0; i < hash.size(); i++) {
                PartitionInfo.PartitionHash partition = hash.get(i);
                String partitionName = partition.getPartitionName();
                if(StringUtils.isBlank(partitionName)) {
                    throw new RuntimeException("分区表名称不能为空");
                }
                String partitionTableName = getTableName(databaseConf.getSchema(), partitionName, ignoreCase);
                String sql = String.format(CREATE_TABLE_PARTITION_HASH, partitionTableName, tableName, hash.size(), i);
                list.add(sql);
                if(StringUtils.isNotBlank(fieldName)) {
                    String indexName = "index_" + partitionName + "_" + fieldName.replaceAll(DOUBLE_QUOTATION_MARKS, "");
                    String indexSql = String.format(CREATE_INDEX_TEMPLATE, "", indexName, partitionTableName, fieldName);
                    list.add(indexSql);
                }
            }
            return list;
        }
        throw new RuntimeException("未知的分区表类型[" + type + "]");
    }

    /**
     * 解析 数据插入语句
     *
     * @param addParamCondition
     * @param databaseConf
     * @param tableConf
     * @return
     */
    public AniHandler.AniInsert parserInsertSql(AddParamCondition addParamCondition, D databaseConf, T tableConf) {

        //是否忽略大小写
        boolean ignoreCase = databaseConf.isIgnoreCase();
        //表名
        String tableName = getTableName(databaseConf.getSchema(), tableConf.getTableName(), ignoreCase);

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

        StringBuilder fieldBuilder = new StringBuilder();

        for (String key : fieldSet) {
            fieldBuilder.append(fieldHandler(key, ignoreCase))
                    .append(",");
        }
        fieldBuilder.deleteCharAt(fieldBuilder.length() - 1);

        List<Object> paramList = new ArrayList<>();
        StringBuilder insertSb = new StringBuilder();
        for (int i = 0; i < fieldValueList.size(); i++) {
            Map<String, Object> recordMap = fieldValueList.get(i);
            if (MapUtils.isEmpty(recordMap)) {
                continue;
            }

            StringBuilder valueSb = new StringBuilder();

            for (String field : fieldSet) {
                Object value = recordMap.get(field);
                addParams(value, paramList);
                valueSb.append(getPlaceholder(value)).append(",");
            }

            valueSb.deleteCharAt(valueSb.length() - 1);

            insertSb.append("(");
            insertSb.append(valueSb);
            insertSb.append("),");
        }

        insertSb.deleteCharAt(insertSb.length() - 1);


        String insertSql = String.format(INSERT_DATA_TEMPLATE, tableName, fieldBuilder.toString(), insertSb.toString());

        //只能支持一条的情况
        if(fieldValueList.size() == 1) {
            boolean returnGeneratedKey = addParamCondition.isReturnGeneratedKey();
            List<String> returnFields = addParamCondition.getReturnFields();
            StringBuilder returnFieldBuilder = new StringBuilder();
            // 返回自增主键值
            if (returnGeneratedKey) {
                returnFieldBuilder.append(" RETURNING ${@@IDENTITY}");
            } else if (CollectionUtil.isNotEmpty(returnFields)) {
                returnFieldBuilder.append(" RETURNING ");
                for (String returnField : returnFields) {
                    returnFieldBuilder.append(fieldHandler(returnField, ignoreCase)).append(",");
                }
                returnFieldBuilder.deleteCharAt(returnFieldBuilder.length() - 1);
            }

            if(returnFieldBuilder.length() > 0) {
                insertSql = insertSql + " " + returnFieldBuilder.toString();
            }
        }

        aniInsert.setSql(insertSql);
        aniInsert.setParamArray(paramList.toArray());
        aniInsert.setAddTotal(fieldValueList.size());
        aniInsert.setReturnFields(addParamCondition.getReturnFields());
        aniInsert.setReturnGeneratedKey(addParamCondition.isReturnGeneratedKey());
        return aniInsert;
    }

    private void addParams(Object value, List<Object> paramList) {
        Object tValue = translateValue(value);
        if(tValue instanceof List) {
            paramList.addAll((List) tValue);
        } else if (value instanceof Map) {
            // 不进行处理放在 getPlaceholder() 方法中处理
        } else {
            paramList.add(tValue);
        }
    }

    private String getPlaceholder(Object value) {

        if (value instanceof List || (value != null && (value.getClass().isArray() && !(value instanceof byte[])))) {
            List valueList;
            if (value.getClass().isArray() && !(value instanceof byte[])) {
                valueList = Arrays.asList(((Object[]) value));
            } else {
                valueList = (List) value;
            }
            if (CollectionUtil.isEmpty(valueList)) {
                return "null";
            }
            StringBuilder arraySb = new StringBuilder();
            arraySb.append("ARRAY[");
            for (int i = 0; i < valueList.size(); i++) {
                arraySb.append("?,");
            }
            if (arraySb.toString().endsWith(",")) {
                arraySb.deleteCharAt(arraySb.length() - 1);
            }
            arraySb.append("]");
            return arraySb.toString();
        } else if (value instanceof Point) {
            return String.format(POINT_TEMPLATE, "?", "?");
        } else if (value instanceof Map) {
            return SYMBOL_SINGLE_QUOTES + JsonUtil.objectToStr(value) + SYMBOL_SINGLE_QUOTES;
        }

        return "?";
    }

    /**
     * 设置字段值
     *
     * @param value
     */
    private Object translateValue(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Point) {
            Point point = (Point) value;
            return Arrays.asList(point.getLon(), point.getLat());
        } else if (value instanceof List || (value.getClass().isArray() && !(value instanceof byte[]))) {
            if (value.getClass().isArray()) {
                return Arrays.asList(((Object[]) value));
            } else {
                return value;
            }
        } else if (value instanceof Map) {
            // Map 对象不进行处理，认为是一个 json 对象，放在 getPlaceholder() 方法中处理
            return null;
        } else if (value instanceof String) {
//            return formatter(String.valueOf(value));
            return String.valueOf(value);
        } else if(value instanceof Timestamp || value instanceof Time){
            return value;
        }else if (value instanceof Date){
            Date date = (Date) value;
            return new Timestamp(date.getTime());
        } else {
            return value;
        }
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
        } else if (value instanceof Map) {
            valueSb.append(SYMBOL_SINGLE_QUOTES);
            valueSb.append(JsonUtil.objectToStr(value));
            valueSb.append(SYMBOL_SINGLE_QUOTES);
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
     * @param databaseConf
     * @param tableConf
     * @return
     */
    public AniHandler.AniDel parserDelSql(DelParamCondition delParamCondition, D databaseConf, T tableConf) {

        //是否忽略大小写
        boolean ignoreCase = databaseConf.isIgnoreCase();
        boolean likeToiLike = databaseConf.isLikeToiLike();
        //表名
        String tableName = getTableName(databaseConf.getSchema(), tableConf.getTableName(), ignoreCase);

        AniHandler.AniDel aniDel = new AniHandler.AniDel();
        List<Where> where = delParamCondition.getWhere();
        if (CollectionUtils.isNotEmpty(where)) {
            List<Object> paramList = new LinkedList<>();
            String whereSql = buildWhereSql(where, paramList, ignoreCase, likeToiLike);
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
    public AniHandler.AniCreateIndex parserCreateIndexSql(IndexParamCondition indexParamCondition, D databaseConf, T tableConf) {
        //是否忽略大小写
        boolean ignoreCase = databaseConf.isIgnoreCase();
        //表名
        String tableName = getTableName(databaseConf.getSchema(), tableConf.getTableName(), ignoreCase);
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

        Boolean isNotExists = indexParamCondition.getIsNotExists();

        if (isNotExists != null && isNotExists) {
            aniCreateIndex.setSql(String.format(CREATE_INDEX_IF_NOT_EXISTS_TEMPLATE, indexType, indexName, tableName, CollectionUtil.join(indexFields, ",")));
        } else {
            aniCreateIndex.setSql(String.format(CREATE_INDEX_TEMPLATE, indexType, indexName, tableName, CollectionUtil.join(indexFields, ",")));
        }
        return aniCreateIndex;
    }

    /**
     * 解析 删除索引语句
     *
     * @param indexParamCondition
     * @return
     */
    public AniHandler.AniDropIndex parserDropIndexSql(IndexParamCondition indexParamCondition, D databaseConf, T tableConf) {
        String indexName = indexParamCondition.getIndexName();

        if (StringUtils.isBlank(indexName)) {
            throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "删除索引必须指定 indexName 索引名称!");
        }

        //是否忽略大小写
        boolean ignoreCase = databaseConf.isIgnoreCase();
        AniHandler.AniDropIndex aniDropIndex = new AniHandler.AniDropIndex();
        aniDropIndex.setSql(String.format(DROP_INDEX_TEMPLATE, fieldHandler(databaseConf.getSchema(), ignoreCase), indexName));
        return aniDropIndex;
    }

    /**
     * 解析 数据更新语句
     *
     * @param updateParamCondition
     * @param databaseConf
     * @param tableConf
     * @return
     */
    public AniHandler.AniUpdate parserUpdateSql(UpdateParamCondition updateParamCondition, D databaseConf, T tableConf) {

        //是否忽略大小写
        boolean ignoreCase = databaseConf.isIgnoreCase();
        boolean likeToiLike = databaseConf.isLikeToiLike();
        //表名
        String tableName = getTableName(databaseConf.getSchema(), tableConf.getTableName(), ignoreCase);

        AniHandler.AniUpdate aniUpdate = new AniHandler.AniUpdate();
        Map<String, Object> updateParamMap = updateParamCondition.getUpdateParamMap();
        UpdateParamCondition.ArrayProcessMode arrayProcessMode = updateParamCondition.getArrayProcessMode();
        StringBuilder updateSb = new StringBuilder();
        List<StringBuilder> updateForArraySbList = new ArrayList<>();
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
                        arrayInUpdate(updateSb, key, v.get(0), ARRAY_UPDATE_SET, paramList);
                        updateSb.append(",");
                    } else {
                        for (int i = 0; i < v.size(); i++) {
                            Object o = v.get(i);
                            StringBuilder currentSb;
                            if (i == 0) {
                                currentSb = updateSb;
                                arrayInUpdate(currentSb, key, o, ARRAY_UPDATE_SET, paramList);
                                currentSb.append(",");
                            } else {
                                currentSb = new StringBuilder();
                                updateForArraySbList.add(currentSb);
                                List<Object> subParam = new ArrayList<>();
                                arrayInUpdate(currentSb, key, o, ARRAY_UPDATE_SET, subParam);
                                subParamList.add(subParam);
                            }
                        }
                    }
                } else {
                    arrayInUpdate(updateSb, key, value, ARRAY_UPDATE_APPEND, paramList);
                    updateSb.append(",");
                }
            } else {
                updateSb.append(fieldHandler(key, ignoreCase))
                        .append(" = ");
                addParams(value, paramList);
                updateSb.append(getPlaceholder(value)).append(",");
            }
        });
        updateSb.deleteCharAt(updateSb.length() - 1);
        List<Where> where = updateParamCondition.getWhere();
        List<Object> whereParams = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(where)) {
            String whereSql = buildWhereSql(where, whereParams, ignoreCase, likeToiLike);
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
        aniUpdate.setSql(String.format(UPDATE_DATA_TEMPLATE, tableName, updateSb.toString()));
        if (CollectionUtil.isNotEmpty(updateForArraySbList)) {
            List<String> subSQL = new ArrayList<>();
            for (int i = 0; i < updateForArraySbList.size(); i++) {
                StringBuilder builder = updateForArraySbList.get(i);
                subSQL.add(String.format(UPDATE_DATA_TEMPLATE, tableName, builder.toString()));
            }
            aniUpdate.setSubSQL(subSQL);
        }
        return aniUpdate;
    }

    protected void arrayInUpdate(StringBuilder updateSb, String field, Object value, String template, List<Object> paramList) {
        String valuePlaceholder = "?";
        if (value instanceof List) {
            StringBuilder valueSb = new StringBuilder();
            valueSb.append("ARRAY[");
            List valueList;
            if (value.getClass().isArray()) {
                valueList = CollectionUtil.newArrayList(ArrayUtil.wrap(value));
            } else {
                valueList = (List) value;
            }
            for (int i = 0; i < valueList.size(); i++) {
                Object o = valueList.get(i);
                valueSb.append("?").append(",");
                paramList.add(o);
            }
            if (valueSb.toString().endsWith(",")) {
                valueSb.deleteCharAt(valueSb.length() - 1);
            }
            valueSb.append("]");
            valuePlaceholder = valueSb.toString();
        } else {
            // 命中的是 ARRAY_UPDATE_SET，所以需要两个参数站位
            if (value instanceof String) {
                valuePlaceholder = SYMBOL_SINGLE_QUOTES + value + SYMBOL_SINGLE_QUOTES;
            } else {
                paramList.add(value);
                paramList.add(value);
            }
        }
        updateSb.append(DOUBLE_QUOTATION_MARKS)
                .append(field)
                .append(DOUBLE_QUOTATION_MARKS)
                .append(" = ")
                .append(StringUtils.replaceEach(template, new String[]{"$column", "$value"}, new String[]{getMarks() + KeywordConstant.findKeyword(field, getMarks()) + getMarks(), valuePlaceholder}));
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
     * @param ignoreCase
     * @return
     */
    private String buildSelectSql(List<String> selects, boolean ignoreCase) {
        StringBuilder sb = new StringBuilder();
        if (selects != null && !selects.isEmpty()) {
            for (String select : selects) {
                if (StringUtils.isBlank(select)) {
                    continue;
                }
                String checkMethod = checkMethod(select, ignoreCase);
                if (StringUtils.isBlank(checkMethod)) {

                    String checkAlias = checkAlias(select, ignoreCase);
                    if (StringUtils.isBlank(checkAlias)) {
                        sb.append(fieldHandler(select, ignoreCase));
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

    protected String checkAlias(String select, boolean ignoreCase) {
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
            stringBuffer.append(fieldHandler(field, ignoreCase))
                    .append(" as ")
                    .append(fieldHandler(alias, ignoreCase));
            return stringBuffer.toString();
        } else {
            return null;
        }
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
    private String checkMethod(String select, boolean ignoreCase) {
        boolean flag = StringUtils.startsWithIgnoreCase(select, "f(");
        if (flag) {
            select = select.substring(2, select.lastIndexOf(")"));
            String methodName = select.substring(0, select.indexOf("("));
            String parse = Method.parse(methodName.toLowerCase()).getName();
            String field = select.substring(select.indexOf("(") + 1, select.lastIndexOf(")"));
            StringBuffer stringBuffer = new StringBuffer();
            StringBuffer queryField = stringBuffer.append(parse).append("(").append(fieldHandler(field, ignoreCase)).append(") AS ").append("\"")
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
    private String buildWhereSql(List<Where> wheres, List<Object> paramList, boolean ignoreCase, boolean likeToiLike) {
        String sql = buildWhereSql(wheres, Rel.AND, paramList, ignoreCase, likeToiLike);
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
    private String buildWhereSql(List<Where> wheres, Rel rel, List<Object> paramList, boolean ignoreCase, boolean likeToiLike) {
        StringBuilder sb = new StringBuilder();
        if (wheres != null && !wheres.isEmpty()) {
            for (Where where : wheres) {
                if (Rel.AND.equals(where.getType()) || Rel.OR.equals(where.getType()) || Rel.NOT.equals(where.getType())) {
                    List<Where> params = where.getParams();
                    String sql = buildWhereSql(params, where.getType(), paramList, ignoreCase, likeToiLike);
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
                    sb.append(buildWhereSql(where, rel, paramList, ignoreCase, likeToiLike));
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
    private String buildWhereSql(Where where, Rel rel, List<Object> paramList, boolean ignoreCase, boolean likeToiLike) {
        StringBuilder sb = new StringBuilder();
        if (whereHandler(where)) {
            sb.append(relStrHandler(where, paramList, ignoreCase, likeToiLike));
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
    private String relStrHandler(Where where, List<Object> paramList, boolean ignoreCase, boolean likeToiLike) {
        // 解决pg 日期查询问题
        String placeholder = "?";
        if(ItemFieldTypeEnum.DATE.getVal().equalsIgnoreCase(where.getParamType())
                || ItemFieldTypeEnum.TIME.getVal().equalsIgnoreCase(where.getParamType())
                || ItemFieldTypeEnum.DATETIME.getVal().equalsIgnoreCase(where.getParamType())
                || ItemFieldTypeEnum.SMART_TIME.getVal().equalsIgnoreCase(where.getParamType())
                || ItemFieldTypeEnum.TIMESTAMP.getVal().equalsIgnoreCase(where.getParamType())) {
            placeholder = "?::timestamp";
        }
        Rel rel = where.getType();
        StringBuilder sb = new StringBuilder();
        String fieldHandler = fieldHandler(where.getField(), ignoreCase);
        // 如果是数组类型，则加上 unnest()
        if (StringUtils.equals(ItemFieldTypeEnum.ARRAY.getVal(), where.getParamType())) {
            fieldHandler = "exists (select 1 from unnest(" + fieldHandler + ") as array_fun where array_fun";
        }
        sb.append(fieldHandler);
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
                if (likeToiLike) {
                    sb.append(" ilike ");
                } else {
                    sb.append(" like ");
                }
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
            case ILIKE:
                sb.append(" ilike ");
                sb.append(" ? ");
                String ilikeSqlParam = (String)where.getParam();
                if(ilikeSqlParam.startsWith("*") && ilikeSqlParam.endsWith("*")){
                    ilikeSqlParam ="%" + ilikeSqlParam.substring(1,ilikeSqlParam.length()-1) + "%";
                }
                if(!ilikeSqlParam.startsWith("%") && !ilikeSqlParam.endsWith("%")){
                    ilikeSqlParam ="%" + ilikeSqlParam + "%";
                }
                paramList.add(ilikeSqlParam);
                break;
            case LIKE:
                if (likeToiLike) {
                    sb.append(" ilike ");
                } else {
                    sb.append(" like ");
                }
                sb.append("?");
                paramList.add("%" + where.getParam() + "%");
                break;
            case FRONT_LIKE:
                if (likeToiLike) {
                    sb.append(" ilike ");
                } else {
                    sb.append(" like ");
                }
                sb.append("?");
                paramList.add("%" + where.getParam());
                break;
            case TAIL_LIKE:
                if (likeToiLike) {
                    sb.append(" ilike ");
                } else {
                    sb.append(" like ");
                }
                sb.append("?");
                paramList.add(where.getParam() + "%");
                break;
            case NOT_FRONT_LIKE:
                if (likeToiLike) {
                    sb.append(" not ilike ");
                } else {
                    sb.append(" not like ");
                }
                sb.append("?");
                paramList.add("%" + where.getParam());
                break;
            case NOT_MIDDLE_LIKE:
                if (likeToiLike) {
                    sb.append(" not ilike ");
                } else {
                    sb.append(" not like ");
                }
                sb.append("?");
                paramList.add("%" + where.getParam() +"%");
                break;
            case NOT_TAIL_LIKE:
                if (likeToiLike) {
                    sb.append(" not ilike ");
                } else {
                    sb.append(" not like ");
                }
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
        if (StringUtils.equals(ItemFieldTypeEnum.ARRAY.getVal(), where.getParamType())) {
            sb.append(")");
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


    public AniHandler.AniUpsert parserUpsertSql(UpsertParamCondition upsertParamCondition, D databaseConf, T tableConf) {

        //是否忽略大小写
        boolean ignoreCase = databaseConf.isIgnoreCase();
        //表名
        String tableName = getTableName(databaseConf.getSchema(), tableConf.getTableName(), ignoreCase);

        AniHandler.AniUpsert aniUpsert = new AniHandler.AniUpsert();
        Map<String, Object> insertParamMap = upsertParamCondition.getUpsertParamMap();
        Map<String, Object> updateParamMap = upsertParamCondition.getUpdateParamMap();
        List<String> conflictFieldList = upsertParamCondition.getConflictFieldList();
        StringBuilder insertFieldSb = new StringBuilder();
        StringBuilder insertValueSb = new StringBuilder("(");
        StringBuilder updateSb = new StringBuilder();
        StringBuilder conflictFieldSb = new StringBuilder();
        List<Object> insertParamList = new ArrayList<>();
        boolean updateFlag = MapUtil.isEmpty(updateParamMap);
        List<Object> updateParamList = new ArrayList<>();
        if (updateFlag) {
            Set<String> keySet = new HashSet<>(insertParamMap.keySet());
            keySet.removeAll(conflictFieldList);
        } else {
            Set<String> keySet = new HashSet<>(updateParamMap.keySet());
            keySet.removeAll(conflictFieldList);
        }
        for (Map.Entry<String, Object> entry : insertParamMap.entrySet()) {

            String key = entry.getKey();
            String field = fieldHandler(key, ignoreCase);
            Object value = entry.getValue();
            String placeholder = getPlaceholder(value);

            insertFieldSb.append(field).append(",");
            addParams(value, insertParamList);
            insertValueSb.append(placeholder).append(",");
            if (updateFlag) {
                if (!conflictFieldList.contains(key)) {
                    updateSb.append(field).append(" = ").append(placeholder).append(",");
                    addParams(value, updateParamList);
                }
            }
        }
        if (!updateFlag) {
            for (Map.Entry<String, Object> entry : updateParamMap.entrySet()) {

                String key = entry.getKey();
                String field = fieldHandler(key, ignoreCase);
                Object value = entry.getValue();
                String placeholder = getPlaceholder(value);

                if (!conflictFieldList.contains(key)) {
                    updateSb.append(field).append(" = ").append(placeholder).append(",");
                    addParams(value, updateParamList);
                }
            }
        }
        for (String conflictField : conflictFieldList) {
            conflictFieldSb.append(fieldHandler(conflictField, ignoreCase)).append(",");
        }
        insertFieldSb = insertFieldSb.deleteCharAt(insertFieldSb.length() - 1);
        insertValueSb = insertValueSb.deleteCharAt(insertValueSb.length() - 1);
        insertValueSb.append(")");
        updateSb = updateSb.deleteCharAt(updateSb.length() - 1);
        conflictFieldSb = conflictFieldSb.deleteCharAt(conflictFieldSb.length() - 1);
        String sql = String.format(UPSERT_DATA_TEMPLATE, tableName, insertFieldSb.toString(), insertValueSb.toString(), conflictFieldSb.toString(), updateSb.toString());
        aniUpsert.setSql(sql);
        aniUpsert.setParamArray(ArrayUtil.addAll(insertParamList.toArray(), updateParamList.toArray()));
        return aniUpsert;
    }

    public AniHandler.AniUpsertBatch parserUpsertBatchSql(UpsertParamBatchCondition upsertParamBatchCondition, D databaseConf, T tableConf) {

        //是否忽略大小写
        boolean ignoreCase = databaseConf.isIgnoreCase();
        //表名
        String tableName = getTableName(databaseConf.getSchema(), tableConf.getTableName(), ignoreCase);

        AniHandler.AniUpsertBatch aniUpsertBatch = new AniHandler.AniUpsertBatch();
        List<Map<String, Object>> insertParamMapList = upsertParamBatchCondition.getUpsertParamList();
        List<String> updateKeys = upsertParamBatchCondition.getUpdateKeys();
        List<String> conflictFieldList = upsertParamBatchCondition.getConflictFieldList();
        Set<String> keys = insertParamMapList.parallelStream().flatMap(map -> Stream.of(new HashSet(map.keySet()))).reduce((a, b) -> {a.addAll(b); return a;}).orElse(new HashSet());
        StringBuilder insertFieldSb = new StringBuilder();
        StringBuilder insertValueSb = new StringBuilder();
        StringBuilder updateSb = new StringBuilder();
        StringBuilder conflictFieldSb = new StringBuilder();
        List<Object> insertParamList = new ArrayList<>();

        boolean updateFlag = CollectionUtil.isEmpty(updateKeys);
        for (String key : keys) {
            String field = fieldHandler(key, ignoreCase);
            insertFieldSb.append(field).append(",");
            if (updateFlag) {
                if (!conflictFieldList.contains(key)) {
                    updateSb.append(field).append(" = EXCLUDED.").append(field).append(",");
                }
            }
        }

        if (!updateFlag) {
            for (String updateKey : updateKeys) {
                if (!conflictFieldList.contains(updateKey)) {
                    String field = fieldHandler(updateKey, ignoreCase);
                    updateSb.append(field).append(" = EXCLUDED.").append(field).append(",");
                }
            }
        }

        for (Map<String, Object> upsertParamMap : insertParamMapList) {
            insertValueSb.append("(");
            for (String key : keys) {
                Object value = upsertParamMap.get(key);
                insertValueSb.append(getPlaceholder(value)).append(",");
                addParams(value, insertParamList);
            }
            insertValueSb = insertValueSb.deleteCharAt(insertValueSb.length() - 1);
            insertValueSb.append("),");
        }
        for (String conflictField : conflictFieldList) {
            conflictFieldSb.append(fieldHandler(conflictField, ignoreCase)).append(",");
        }
        insertFieldSb = insertFieldSb.deleteCharAt(insertFieldSb.length() - 1);
        insertValueSb = insertValueSb.deleteCharAt(insertValueSb.length() - 1);
        conflictFieldSb = conflictFieldSb.deleteCharAt(conflictFieldSb.length() - 1);
        updateSb = updateSb.deleteCharAt(updateSb.length() - 1);
        String sql = String.format(UPSERT_DATA_TEMPLATE, tableName, insertFieldSb.toString(), insertValueSb.toString(), conflictFieldSb.toString(), updateSb.toString());
        aniUpsertBatch.setSql(CollectionUtil.newArrayList(sql));
        ArrayList<Object[]> paramList = new ArrayList<>();
        paramList.add(insertParamList.toArray());
        aniUpsertBatch.setParamArray(paramList);
        return aniUpsertBatch;
    }

    public String fieldHandler(String field, boolean ignoreCase) {

        if (ignoreCase) {
            if(keyWordHandler.isKeyWord(field)) {
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
        // 组装对象
        AniHandler aniHandler = new AniHandler();
        aniHandler.setDropdatabase(aniDropDatabase);
        return (Q) aniHandler;
    }

    @Override
    public Q getUpdateDatabaseParam(UpdateDatabaseParamCondition paramCondition, D dataConf) {
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

        // 组装对象
        AniHandler aniHandler = new AniHandler();
        aniHandler.setUpdatedatabase(aniUpdateDatabase);
        return (Q) aniHandler;

    }
}
