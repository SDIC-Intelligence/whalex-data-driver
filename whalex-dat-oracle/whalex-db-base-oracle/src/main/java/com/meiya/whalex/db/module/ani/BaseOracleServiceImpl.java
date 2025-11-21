package com.meiya.whalex.db.module.ani;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.map.MapUtil;
import cn.hutool.core.util.StrUtil;
import com.meiya.whalex.annotation.Support;
import com.meiya.whalex.db.constant.SupportPower;
import com.meiya.whalex.db.entity.ani.*;
import com.meiya.whalex.db.entity.QueryMethodResult;
import com.meiya.whalex.db.entity.table.infomaration.MysqlTableInformation;
import com.meiya.whalex.db.entity.table.infomaration.TableInformation;
import com.meiya.whalex.db.error.OracleFunctionErrorHandlerInfoTips;
import com.meiya.whalex.db.util.helper.impl.ani.BaseOracleConfigHelper;
import com.meiya.whalex.db.util.param.impl.ani.BaseOracleParamUtil;
import com.meiya.whalex.db.util.param.impl.ani.BaseOracleParserUtil;
import com.meiya.whalex.interior.db.constant.ItemFieldTypeEnum;
import com.meiya.whalex.sql.ani.RdbmsCursorCache;
import com.meiya.whalex.sql.module.AbstractRdbmsModuleBaseService;
import com.meiya.whalex.exception.BusinessException;
import com.meiya.whalex.exception.ExceptionCode;
import com.meiya.whalex.sql.module.SequenceHandler;
import com.meiya.whalex.sql.module.SqlParseHandler;
import com.meiya.whalex.util.JsonUtil;
import lombok.extern.slf4j.Slf4j;
import oracle.jdbc.OracleConnection;
import oracle.jdbc.OracleTypes;
import oracle.spatial.geometry.JGeometry;
import oracle.sql.STRUCT;
import org.apache.commons.dbutils.BasicRowProcessor;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.MapHandler;
import org.apache.commons.dbutils.handlers.MapListHandler;
import org.apache.commons.lang3.StringUtils;


import java.sql.*;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Oracle 服务
 *
 * @author 蔡荣桂
 * @date 2021/4/14
 * @project whale-cloud-platformX
 */
@Slf4j
@Support(value = {
        SupportPower.SHOW_SCHEMA,
        SupportPower.QUERY_INDEX,
        SupportPower.CREATE_INDEX,
        SupportPower.CREATE_TABLE,
        SupportPower.DROP_TABLE,
        SupportPower.MODIFY_TABLE,
        SupportPower.SHOW_TABLE_LIST
})
public class BaseOracleServiceImpl extends AbstractRdbmsModuleBaseService<QueryRunner, AniHandler, BaseOracleDatabaseInfo, BaseOracleTableInfo, RdbmsCursorCache> {

    /**
     * 查询库中表信息SQL
     */
    private static final String SHOW_TABLE_SQL = "select table_name,owner AS userName FROM all_tables where owner = ?";

    /**
     * 查询表索引信息SQL
     */
    private static final String SHOW_INDEX_SQL = "SELECT\n" +
            "\tt1.*,\n" +
            "\tt2.UNIQUENESS,\n" +
            "\tt2.INDEX_TYPE,\n" +
            "\t(\n" +
            "\tSELECT\n" +
            "\t\tCONSTRAINT_TYPE \n" +
            "\tFROM\n" +
            "\t\tALL_CONSTRAINTS \n" +
            "\tWHERE\n" +
            "\t\tTABLE_NAME = t1.TABLE_NAME \n" +
            "\t\tAND OWNER = t1.TABLE_OWNER \n" +
            "\t\tAND STATUS = 'ENABLED' \n" +
            "\t\tAND CONSTRAINT_TYPE = 'P' \n" +
            "\t\tAND INDEX_NAME = t1.INDEX_NAME \n" +
            "\t) AS CONSTRAINT_TYPE,\n" +
            "\tt3.COLUMN_EXPRESSION \n" +
            "FROM\n" +
            "\t( SELECT INDEX_NAME, TABLE_NAME, COLUMN_NAME, COLUMN_POSITION, DESCEND, TABLE_OWNER FROM ALL_IND_COLUMNS WHERE TABLE_OWNER = ? AND INDEX_NAME NOT LIKE 'BIN$%' AND (TABLE_NAME = ? OR TABLE_NAME = ?)) t1\n" +
            "\tLEFT JOIN ALL_INDEXES t2 ON t2.INDEX_NAME = t1.INDEX_NAME \n" +
            "\tAND t2.TABLE_NAME = t1.TABLE_NAME \n" +
            "\tAND t2.TABLE_OWNER = t1.TABLE_OWNER\n" +
            "\tLEFT JOIN ALL_IND_EXPRESSIONS t3 ON t3.INDEX_NAME = t1.INDEX_NAME \n" +
            "\tAND t3.TABLE_NAME = t1.TABLE_NAME \n" +
            "\tAND t3.COLUMN_POSITION = t1.COLUMN_POSITION \n" +
            "\tAND t3.TABLE_OWNER = t1.TABLE_OWNER \n" +
            "ORDER BY\n" +
            "\tt1.INDEX_NAME,\n" +
            "\tt1.COLUMN_POSITION";

    /**
     * 删表SQL
     */
    private static final String DROP_TABLE_SQL = "DROP TABLE %s";

    /**
     * 判断表是否存在
     */
    private static final String EXISTS_TABLE_SQL = "select COUNT(*) AS count FROM all_tables WHERE owner = ? and table_name = ?";

    /**
     * 查询所有的数据库列表
     */
    private static final String SHOW_DATABASE_SQL = "SELECT NAME as \"Database\" FROM v$database";

    /**
     * 查询数据库信息
     */
    private static final String SHOW_DATABASE_INFO_SQL = "SELECT * FROM v$database where NAME = ? or DB_UNIQUE_NAME = ?";

    /**
     * 分页模板
     */
    private final static String PAGE_TEMPLATE = "SELECT * FROM (SELECT A.*, ROWNUM ORACLE_PAGE_FIELD FROM (%s) A WHERE ROWNUM <= %s) WHERE ORACLE_PAGE_FIELD >= %s";

    /**
     * 查询序列
     */
    private static final String SHOW_SEQUENCE_SQL = "SELECT SEQUENCE_NAME FROM ALL_SEQUENCES WHERE SEQUENCE_OWNER = ? AND SEQUENCE_NAME LIKE ?";

    /**
     * 删除序列
     */
    private static final String DROP_SEQUENCE_SQL = "DROP SEQUENCE %s";

    /**
     * 触发器查询
     */
    private static final String QUERY_TRIGGER_SQL = "SELECT TRIGGER_BODY FROM ALL_TRIGGERS WHERE OWNER = ? AND TABLE_NAME = ? AND TRIGGERING_EVENT = 'INSERT' AND STATUS = 'ENABLED' AND BASE_OBJECT_TYPE = 'TABLE'";

    private String TABLE_META = "SELECT * from all_tables where owner = ? and table_name = ?";

    private String GET_TABLE_COMMENT = "select * from all_tab_comments where owner = ? and table_name = ?";

    @Override
    protected QueryMethodResult insertMethod(QueryRunner connect, AniHandler queryEntity, BaseOracleDatabaseInfo databaseConf, BaseOracleTableInfo tableConf) throws Exception {
        QueryMethodResult queryMethodResult = super.insertMethod(connect, queryEntity, databaseConf, tableConf);
        Integer addTotal = queryEntity.getAniInsert().getAddTotal();
        queryMethodResult.setTotal(addTotal);
        return queryMethodResult;
    }

    @Override
    protected Integer insertReturnValue(Connection connection, BaseOracleDatabaseInfo databaseConf, BaseOracleTableInfo tableConf, AniHandler queryEntity, String sql, List<Map<String, Object>> keys) throws Exception {
        Map<String, Integer> tableColumn = getTypeEnumMap(databaseConf, tableConf);
        // 替换自增主键占位符
        if (queryEntity.getAniInsert().isReturnGeneratedKey()) {
            if (MapUtil.isNotEmpty(tableColumn)) {
                Map.Entry<String, Integer> next = tableColumn.entrySet().iterator().next();
                String key = next.getKey();
                BaseOracleParamUtil dbModuleParamUtil = (BaseOracleParamUtil) this.getDbModuleParamUtil();
                String field = dbModuleParamUtil.getBaseOracleParserUtil().fieldHandler(key, databaseConf.isIgnoreCase());
                sql = StringUtils.replace(sql, "${@@IDENTITY}", field);
            } else {
                throw new BusinessException("未查询到 " + tableConf.getTableName() + " 对应的自增主键信息!");
            }
        }
        CallableStatement callStatement = null;
        Integer addTotal = queryEntity.getAniInsert().getAddTotal();
        List<String> returnFields = queryEntity.getAniInsert().getReturnFields();
        boolean returnGeneratedKey = queryEntity.getAniInsert().isReturnGeneratedKey();
        try {
            callStatement = connection.prepareCall(sql);

            //设置参数
            if (queryEntity.getAniInsert().isReturnGeneratedKey()) {
                callStatementSetObject(queryEntity.getAniInsert().getParamArray(), callStatement, addTotal, returnGeneratedKey ? 1 : returnFields.size(), new int[]{OracleTypes.NUMBER});
            } else {
                int[] oracleTypes =  new int[returnFields.size()];
                for (int i = 0; i < returnFields.size(); i++) {
                    String returnField = returnFields.get(i);
                    Integer oracleType = tableColumn.get(returnField);
                    if (oracleType == null) {
                        oracleType = OracleTypes.VARCHAR;
                    }
                    oracleTypes[i] = oracleType;
                }
                callStatementSetObject(queryEntity.getAniInsert().getParamArray(), callStatement, addTotal, returnGeneratedKey ? 1 : returnFields.size(), oracleTypes);
            }

            callStatement.executeUpdate();

            if (returnGeneratedKey) {
                if (addTotal == 1) {
                    // 单条
                    Object identity = callStatement.getLong(queryEntity.getAniInsert().getParamArray().length + 1);
                    Map<String, Object> columnMap = new LinkedHashMap<>();
                    keys.add(columnMap);
                    columnMap.put("GENERATED_KEY", identity);
                } else {
                    // 批量
                    for (Integer i = 1; i <= addTotal; i++) {
                        // 单行占位符数量
                        int singleParamSize = queryEntity.getAniInsert().getParamArray().length / addTotal;
                        Object identity = callStatement.getLong((singleParamSize + 1) * i);
                        Map<String, Object> columnMap = new LinkedHashMap<>();
                        keys.add(columnMap);
                        columnMap.put("GENERATED_KEY", identity);
                    }
                }
            } else {
                if (addTotal == 1) {
                    // 单条
                    Map<String, Object> columnMap = new LinkedHashMap<>();
                    keys.add(columnMap);
                    for (int i = 0; i < returnFields.size(); i++) {
                        String returnField = returnFields.get(i);
                        Integer oracleType = tableColumn.get(returnField);
                        Object returnVal = getReturnVal(callStatement, queryEntity.getAniInsert().getParamArray().length + i + 1, oracleType);
                        columnMap.put(returnField, returnVal);
                    }
                } else {
                    // 批量
                    for (Integer i = 1; i <= addTotal; i++) {
                        Map<String, Object> columnMap = new LinkedHashMap<>();
                        keys.add(columnMap);
                        // 单行占位符数量
                        int singleParamSize = queryEntity.getAniInsert().getParamArray().length / addTotal;
                        int singleRegisterTotal = returnFields.size();
                        int currentBaseIndex = (singleParamSize * i) + (singleRegisterTotal * (i - 1));
                        for (int t = 1; t <= returnFields.size(); t++) {
                            String returnField = returnFields.get(t - 1);
                            Integer oracleType = tableColumn.get(returnField);
                            Object returnVal = getReturnVal(callStatement, currentBaseIndex + t, oracleType);
                            columnMap.put(returnField, returnVal);
                        }
                    }
                }
            }

            return addTotal;
        } finally {
            if (callStatement != null) callStatement.close();
        }
    }

    protected Object getReturnVal(CallableStatement callStatement, int index, Integer oracleType) throws Exception {
        Object object;
        switch (oracleType) {
            case OracleTypes.VARCHAR:
            case OracleTypes.CHAR:
            case OracleTypes.LONGVARCHAR:
                object = callStatement.getString(index);
                break;
            case OracleTypes.TINYINT:
            case OracleTypes.SMALLINT:
            case OracleTypes.INTEGER:
                object = callStatement.getInt(index);
                break;
            case OracleTypes.BIGINT:
                object = callStatement.getLong(index);
                break;
            case OracleTypes.FLOAT:
            case OracleTypes.REAL:
                object = callStatement.getFloat(index);
                break;
            case OracleTypes.DOUBLE:
                object = callStatement.getDouble(index);
                break;
            case OracleTypes.DECIMAL:
            case OracleTypes.NUMERIC:
                object = callStatement.getBigDecimal(index);
                break;
            case OracleTypes.DATE:
            case OracleTypes.TIME:
                object = callStatement.getDate(index);
                break;
            case OracleTypes.TIMESTAMP:
                object = callStatement.getTimestamp(index);
                break;
            default:
                object = callStatement.getObject(index);
        }
        return object;
    }

    /**
     * 获取字段类型映射信息
     *
     * @param databaseConf
     * @param tableConf
     * @return
     */
    private Map<String, Integer> getTypeEnumMap(BaseOracleDatabaseInfo databaseConf, BaseOracleTableInfo tableConf) {
        BaseOracleConfigHelper oracleConfigHelper = (BaseOracleConfigHelper) this.getHelper();
        // 获取表字段映射
        Map<String, Integer> tableColumn = oracleConfigHelper.getTableColumn(databaseConf, tableConf, new Function<String, Map<String, Integer>>() {
            @Override
            public Map<String, Integer> apply(String tableName) {
                QueryRunner dbConnect = oracleConfigHelper.getDbConnect(oracleConfigHelper.getCacheKey(databaseConf, tableConf));
                try {
                    Map<String, Integer> columnMap = new LinkedHashMap<>();
                    QueryMethodResult result = querySchemaMethod(dbConnect, databaseConf, tableConf);
                    List<Map<String, Object>> rows = result.getRows();
                    if (CollectionUtil.isNotEmpty(rows)) {
                        // 获取自增字段信息
                        Map<String, Object> autoincrementMap = rows.stream().filter(row -> {
                            Integer autoincrement = row.get("autoincrement") == null ? null : (Integer) row.get("autoincrement");
                            Integer isKey = row.get("isKey") == null ? null : (Integer) row.get("isKey");
                            if (autoincrement != null && autoincrement.equals(1) && isKey != null && isKey.equals(1)) {
                                return true;
                            }
                            return false;
                        }).findFirst().orElseGet(null);
                        String autoincrementField = null;
                        if (MapUtil.isNotEmpty(autoincrementMap)) {
                            autoincrementField = String.valueOf(autoincrementMap.get("col_name"));
                            extracted(columnMap, autoincrementField, autoincrementMap);
                        }
                        // 判断是否存在自增字段，如果存在则过滤，不重复处理
                        boolean isFilterAutoIncrement = StringUtils.isNotBlank(autoincrementField);
                        for (Map<String, Object> row : rows) {
                            String colName = String.valueOf(row.get("col_name"));
                            if (isFilterAutoIncrement && StringUtils.equals(colName, autoincrementField)) {
                                continue;
                            }
                            extracted(columnMap, colName, row);
                        }
                    }
                    return columnMap;
                } catch (Exception e) {
                    throw new BusinessException("查询 " + tableName + " 对应的自增主键信息异常!", e);
                }
            }
        });
        return tableColumn;
    }

    /**
     * 解析提取schema信息
     *
     * @param columnMap
     * @param row
     */
    private static void extracted(Map<String, Integer> columnMap, String colName, Map<String, Object> row) {
        ItemFieldTypeEnum stdDataType = row.get("std_data_type") == null ? null : ItemFieldTypeEnum.findFieldTypeEnum((String) row.get("std_data_type"));
        int oracleType = OracleTypes.VARCHAR;
        switch (stdDataType) {
            case TINYINT:
                oracleType = OracleTypes.TINYINT;
                break;
            case SMALLINT:
                oracleType = OracleTypes.SMALLINT;
                break;
            case MEDIUMINT:
            case INTEGER:
                oracleType = OracleTypes.INTEGER;
                break;
            case LONG:
                oracleType = OracleTypes.BIGINT;
                break;
            case FLOAT:
                oracleType = OracleTypes.FLOAT;
                break;
            case REAL:
                oracleType = OracleTypes.REAL;
                break;
            case DOUBLE:
                oracleType = OracleTypes.DOUBLE;
                break;
            case DECIMAL:
            case NUMERIC:
            case MONEY:
                oracleType = OracleTypes.DECIMAL;
                break;
            case BIT:
            case BINARY:
                oracleType = OracleTypes.RAW;
                break;
            case YEAR:
            case DATE:
                oracleType = OracleTypes.DATE;
                break;
            case TIMESTAMP:
            case DATETIME:
            case SMART_TIME:
            case TIME:
                oracleType = OracleTypes.TIMESTAMP;
                break;
            case CHAR:
                oracleType = OracleTypes.CHAR;
                break;
            case STRING:
            case ENUM:
            case SET:
                oracleType = OracleTypes.VARCHAR;
                break;
            case TEXT:
            case TINYTEXT:
            case MEDIUMTEXT:
            case LONGTEXT:
            case OBJECT:
                oracleType = OracleTypes.CLOB;
                break;
            case BLOB:
            case BYTES:
            case TINYBLOB:
            case MEDIUMBLOB:
            case LONGBLOB:
            case IMAGE:
            case VARBINARY:
                oracleType = OracleTypes.BLOB;
                break;
            case POINT:
                oracleType = OracleTypes.STRUCT;
                break;
            case BOOLEAN:
                oracleType = OracleTypes.BOOLEAN;
                break;

        }
        columnMap.put(colName, oracleType);
    }

    @Override
    protected void statementSetObject(Object[] paramArray, PreparedStatement statement) throws SQLException {
        if (paramArray != null && paramArray.length > 0) {
            for (int i = 0; i < paramArray.length; i++) {
                Object param = paramArray[i];
                if (param instanceof JGeometry) {
                    Connection connection = statement.getConnection();
                    OracleConnection unwrap = connection.unwrap(OracleConnection.class);
                    STRUCT store = JGeometry.store((JGeometry) param, unwrap);
                    statement.setObject(i + 1, store);
                } else if (param instanceof AniBinaryStreamParameter) {
                    AniBinaryStreamParameter binaryStreamParameter = (AniBinaryStreamParameter) param;
                    statement.setBinaryStream(i + i, binaryStreamParameter.getInputStream(), binaryStreamParameter.getLength());
                } else {
                    statement.setObject(i + 1, param);
                }
            }
        }
    }

    protected void callStatementSetObject(Object[] paramArray, CallableStatement statement, Integer totalSize, Integer singleRegisterTotal, int[] oracleTypes) throws SQLException {
        // 单行占位符数量
        int singleParamSize = paramArray.length / totalSize;
        int currentRegisterIndex = singleParamSize + singleRegisterTotal;
        int registerCount = 0;
        if (paramArray != null && paramArray.length > 0) {
            for (int i = 1; i <= (paramArray.length + (totalSize * singleRegisterTotal)); i++) {
                int paramsIndex = singleParamSize * (registerCount + 1) + (registerCount * singleRegisterTotal);
                if (i > paramsIndex && i <= currentRegisterIndex) {
                    // 设置返回参数
                    statement.registerOutParameter(i, oracleTypes[i - paramsIndex - 1]);
                    if (i + 1 > currentRegisterIndex) {
                        currentRegisterIndex = singleParamSize + currentRegisterIndex + singleRegisterTotal;
                        registerCount++;
                    }
                } else {
                    Object param = paramArray[i - 1 - (registerCount * singleRegisterTotal)];
                    if (param instanceof JGeometry) {
                        Connection connection = statement.getConnection();
                        OracleConnection unwrap = connection.unwrap(OracleConnection.class);
                        STRUCT store = JGeometry.store((JGeometry) param, unwrap);
                        statement.setObject(i, store);
                    } else {
                        statement.setObject(i, param);
                    }
                }
            }
        }
    }

    @Override
    protected QueryMethodResult showTablesMethod(QueryRunner connect, AniHandler queryEntity, BaseOracleDatabaseInfo databaseConf) throws Exception {
        String sql = SHOW_TABLE_SQL;
        if (StringUtils.isNotBlank(queryEntity.getListTable().getTableMatch())) {
            sql = "select * from (" + sql + ") d where d.table_name like \'" + queryEntity.getListTable().getTableMatch() + "\'";
        }
        List<Map<String, Object>> query = queryExecute(databaseConf, connect, sql, new MapListHandler(), new Object[]{databaseConf.getSchema()});
        for (Map<String, Object> map : query) {
            map.put("tableName", map.get("TABLE_NAME"));
            map.remove("TABLE_NAME");
            map.put("tableComment", map.get("TABLE_COMMENT"));
            map.remove("TABLE_COMMENT");
            map.put("schemaName", databaseConf.getSchema());
        }
        return new QueryMethodResult(query.size(), query);
    }

    @Override
    protected QueryMethodResult getIndexesMethod(QueryRunner connect, BaseOracleDatabaseInfo databaseConf, BaseOracleTableInfo tableConf) throws Exception {
        QueryMethodResult queryMethodResult = new QueryMethodResult();
        List<Map<String, Object>> dataList = new ArrayList<>();
        queryMethodResult.setRows(dataList);
        List<Map<String, Object>> queryList = queryExecute(databaseConf, connect, SHOW_INDEX_SQL, new MapListHandler(), new Object[]{databaseConf.getSchema(), tableConf.getTableName().toUpperCase(), tableConf.getTableName()});
        if (CollectionUtil.isNotEmpty(queryList)) {
            String primaryKey = null;
            Map<String, Map<String, Object>> indexesMap = new LinkedHashMap<>();
            Map<String, Boolean> isUniqueMap = new HashMap<>();
            for (int i = 0; i < queryList.size(); i++) {
                Map<String, Object> map = queryList.get(i);
                String columnName = (String) map.get("COLUMN_EXPRESSION");
                if (StringUtils.isNotBlank(columnName)) {
                    if (StringUtils.contains(columnName, BaseOracleParserUtil.DOUBLE_QUOTATION_MARKS)) {
                        columnName = StringUtils.replace(columnName, BaseOracleParserUtil.DOUBLE_QUOTATION_MARKS, "");
                    }
                } else {
                    columnName = (String) map.get("COLUMN_NAME");
                }
                String indexName = (String) map.get("INDEX_NAME");
                Object sort = map.get("DESCEND");
                Object constraintType = map.get("CONSTRAINT_TYPE");
                if (constraintType != null) {
                    primaryKey = indexName;
                }
                String isUnique = (String) map.get("UNIQUENESS");
                isUniqueMap.put(indexName, StringUtils.equalsIgnoreCase(isUnique, "UNIQUE"));
                Map<String, Object> indexMap = indexesMap.get(indexName);
                if (indexMap == null) {
                    indexMap = new LinkedHashMap<>();
                    indexesMap.put(indexName, indexMap);
                }
                indexMap.put(columnName, sort);
            }

            for (Map.Entry<String, Map<String, Object>> entry : indexesMap.entrySet()) {
                Map<String, Object> resultMap = new HashMap<>(1);
                dataList.add(resultMap);
                resultMap.put("indexName", entry.getKey());
                Map<String, Object> value = entry.getValue();
                resultMap.put("column", CollectionUtil.join(value.keySet(), ","));
                resultMap.put("columns", value);
                if (StringUtils.isNotBlank(primaryKey) && StringUtils.equalsIgnoreCase(entry.getKey(), primaryKey)) {
                    // 主键
                    resultMap.put("isUnique", true);
                    resultMap.put("isPrimaryKey", true);
                } else {
                    resultMap.put("isUnique", isUniqueMap.get(entry.getKey()));
                    resultMap.put("isPrimaryKey", false);
                }
            }


        }
        return queryMethodResult;
    }

    @Override
    protected QueryMethodResult createTableMethod(QueryRunner connect, AniHandler queryEntity, BaseOracleDatabaseInfo databaseConf, BaseOracleTableInfo tableConf) throws Exception {

        boolean isNotExists = queryEntity.getAniCreateTable().isNotExists();
        if(isNotExists) {
            if(tableExists(connect, databaseConf, tableConf)) {
                return new QueryMethodResult();
            }
        }

        String sql = queryEntity.getAniCreateTable().getSql();
        //oracle 注释和建表不能同时完成，分开多条sql执行
        String[] split = sql.split("\n");
        executeByConnection(connect, databaseConf, (connection)->{
            Statement statement = null;
            try {
                statement = connection.createStatement();
                for (String item : split){
                    if(StringUtils.isBlank(item)){
                        continue;
                    }
                    if (log.isDebugEnabled()) {
                        log.debug("建表执行的sql是: {}", item);
                    }
                    recordExecuteStatementLog(item, null);
                    statement.execute(item);
                }
            }catch (Exception e)  {
                throw new RuntimeException(e);
            }finally {
                if (statement != null) {
                    try {
                        statement.close();
                    } catch (SQLException e) {
                        log.error("MD ResultSet 对象关闭失败!", e);
                    }
                }
            }
        });

        return new QueryMethodResult();
    }

    @Override
    protected QueryMethodResult dropTableMethod(QueryRunner connect, AniHandler queryEntity, BaseOracleDatabaseInfo databaseConf, BaseOracleTableInfo tableConf) throws Exception {
        BaseOracleParamUtil paramUtil = (BaseOracleParamUtil) getDbModuleParamUtil();
        BaseOracleParserUtil baseOracleParserUtil = paramUtil.getBaseOracleParserUtil();
        String schema =baseOracleParserUtil.fieldHandler(databaseConf.getSchema(), databaseConf.isIgnoreCase());
        String tableName = baseOracleParserUtil.fieldHandler(tableConf.getTableName(), databaseConf.isIgnoreCase());
        tableName = schema + "." + tableName;
        String dropTableSQL = String.format(DROP_TABLE_SQL, tableName);
        int update = updateExecute(databaseConf, connect, dropTableSQL, new Object[0]);

        String sequenceTableName = tableConf.getTableName();
        if (databaseConf.isIgnoreCase()) {
            sequenceTableName = tableConf.getTableName().toUpperCase();
        }

        // 查序列
        List<Map<String, Object>> sequenceResult = queryExecute(databaseConf, connect, SHOW_SEQUENCE_SQL, new MapListHandler(), new Object[]{databaseConf.getSchema(), sequenceTableName + "_%"});

        // 删序列
        if(CollectionUtil.isNotEmpty(sequenceResult)) {
            for (Map<String, Object> sequence : sequenceResult) {
                String sequenceName = (String) sequence.values().iterator().next();
                sequenceName = baseOracleParserUtil.fieldHandler(sequenceName, databaseConf.isIgnoreCase());
                String dropSequenceSQL = String.format(DROP_SEQUENCE_SQL, sequenceName);
                updateExecute(databaseConf, connect, dropSequenceSQL, new Object[0]);
            }
        }

        return new QueryMethodResult(update, null);
    }

    @Override
    protected QueryMethodResult deleteIndexMethod(QueryRunner connect, AniHandler queryEntity, BaseOracleDatabaseInfo databaseConf, BaseOracleTableInfo tableConf) throws Exception {
        String sql = queryEntity.getAniDropIndex().getSql();
        updateExecute(databaseConf, connect, sql, new Object[0]);
        return new QueryMethodResult();
    }

    @Override
    protected QueryMethodResult createIndexMethod(QueryRunner connect, AniHandler queryEntity, BaseOracleDatabaseInfo databaseConf, BaseOracleTableInfo tableConf) throws Exception {
        String sql = queryEntity.getAniCreateIndex().getSql();
        updateExecute(databaseConf, connect, sql, new Object[0]);
        return new QueryMethodResult();
    }

    @Override
    protected BasicRowProcessor getRowProcessor(BaseOracleDatabaseInfo databaseConf) {
        return new BaseOracleRowProcessor();
    }

    @Override
    protected String formatSql(String sql, BaseOracleDatabaseInfo databaseConf, BaseOracleTableInfo tableConf) {
        return sql;
    }

    @Override
    protected SqlParseHandler getSqlParseHandler(BaseOracleDatabaseInfo database) {
        return new OracleSqlParseHandler(database);
    }

    @Override
    protected SequenceHandler getSequenceHandler(BaseOracleDatabaseInfo database) {
        throw new RuntimeException("oracle暂不支持序列");
    }

    @Override
    protected QueryMethodResult monitorStatusMethod(QueryRunner connect, BaseOracleDatabaseInfo databaseConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "OracleServiceImpl.monitorStatusMethod");
    }

    @Override
    protected QueryMethodResult querySchemaMethod(QueryRunner connect, BaseOracleDatabaseInfo databaseConf, BaseOracleTableInfo tableConf) throws Exception {
        List<Map<String, Object>> schemaInfoList = new ArrayList<>();

        executeByConnection(connect, databaseConf, (connection -> {
            ResultSet primaryKeys = null;
            ResultSet columnSet = null;
            String tableName = tableConf.getTableName();
            if(databaseConf.isIgnoreCase()) {
                tableName = tableName.toUpperCase();
            }
            try {

                // 获取主键
                primaryKeys = connection.getMetaData().getPrimaryKeys(databaseConf.getDatabaseName(), databaseConf.getSchema(), tableName);
                Map<String, String> primaryKeyMap = new HashMap<>();
                while (primaryKeys.next()) {
                    String primaryKey = primaryKeys.getString("COLUMN_NAME");
                    String pkName = primaryKeys.getString("pk_name");
                    primaryKeyMap.put(primaryKey, pkName);
                }

                // 查询触发器
                List<Map<String, Object>> triggers = queryExecute(databaseConf, connect, QUERY_TRIGGER_SQL, new MapListHandler(), new Object[]{databaseConf.getSchema(), tableName});
                List<String> triggerBodies = new ArrayList<>();
                if (CollectionUtil.isNotEmpty(triggers)) {
                    for (Map<String, Object> trigger : triggers) {
                        String triggerBody = (String) trigger.get("TRIGGER_BODY");
                        triggerBodies.add(triggerBody);
                    }
                }

                columnSet = connection.getMetaData().getColumns(databaseConf.getDatabaseName(), databaseConf.getSchema(), tableName, "%");

                while (columnSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    String columnName = columnSet.getString("COLUMN_NAME");
                    map.put("col_name", columnName);
                    int isKey = 0;
                    if (primaryKeyMap.containsKey(columnName)) {
                        //主键
                        isKey = 1;
                        map.put("pkName", primaryKeyMap.get(columnName));
                    }
                    map.put("isKey", isKey);
                    String columnDesc = columnSet.getString("REMARKS");
                    columnDesc = columnDesc == null ? "" : columnDesc;
                    map.put("comment", columnDesc);
                    String columnType = columnSet.getString("TYPE_NAME").toLowerCase();
                    map.put("data_type", columnType);
                    String columnSize = columnSet.getString("COLUMN_SIZE");
                    if (StringUtils.isEmpty(columnSize)) {
                        map.put("columnSize", "");
                    } else {
                        map.put("columnSize", columnSize);
                    }
                    // 小数点
                    String decimalDigits = columnSet.getString("DECIMAL_DIGITS");
                    if (StringUtils.isEmpty(decimalDigits)) {
                        map.put("decimalDigits", "");
                    } else {
                        map.put("decimalDigits", decimalDigits);
                    }
                    map.put("std_data_type", BaseOracleFieldTypeEnum.dbFieldType2FieldType(columnType
                            , StringUtils.isNotBlank(columnSize) && StringUtils.isNumeric(columnSize) ? Integer.valueOf(columnSize) : null
                            , StringUtils.isNotBlank(decimalDigits) && StringUtils.isNumeric(decimalDigits) ? Integer.valueOf(decimalDigits) : null
                    ).getVal());
                    int nullableInt = columnSet.getInt("NULLABLE");
                    map.put("nullable", nullableInt);
                    String columnDef = columnSet.getString("COLUMN_DEF");
                    columnDef = columnDef == null ? "" : columnDef;
                    map.put("columnDef", columnDef);

                    if (isKey == 1 && CollectionUtil.isNotEmpty(triggerBodies)) {
                        for (String trigger : triggerBodies) {
                            if (StringUtils.containsIgnoreCase(trigger, ":NEW." + columnName) || StringUtils.containsIgnoreCase(trigger, ":NEW.\"" + columnName + "\"")) {
                                map.put("autoincrement", 1);
                                break;
                            }
                        }
                    } else {
                        map.put("autoincrement", 0);
                    }

                    schemaInfoList.add(map);
                }
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }finally {
                if (columnSet != null) {
                    try {
                        columnSet.close();
                    } catch (SQLException e) {
                        log.error("Oracle ResultSet 对象关闭失败!", e);
                    }
                }

                if (primaryKeys != null) {
                    try {
                        primaryKeys.close();
                    } catch (SQLException e) {
                        log.error("Oracle ResultSet 对象关闭失败!", e);

                    }
                }
            }
        }));

        return new QueryMethodResult(schemaInfoList.size(), schemaInfoList);
    }

    @Override
    protected QueryMethodResult queryDatabaseSchemaMethod(QueryRunner connect, BaseOracleDatabaseInfo databaseConf, BaseOracleTableInfo tableConf) throws Exception {
        List<Map<String, Object>> schemasList = new ArrayList<>();
        executeByConnection(connect, databaseConf, (connection)->{
            ResultSet schemasSet = null;
            try {
                schemasSet = connection.getMetaData().getSchemas();
                while (schemasSet.next()) {
                    String tableSchem = schemasSet.getString("TABLE_SCHEM");
                    Map<String, Object> map = new HashMap<>();
                    map.put("schema", tableSchem);
                    schemasList.add(map);
                }
            } catch (SQLException e) {
                e.printStackTrace();
                throw new RuntimeException(e.getCause());
            }finally {
                if(schemasSet != null) {
                    try {
                        schemasSet.close();
                    } catch (SQLException throwables) {
                        throwables.printStackTrace();
                    }
                }
            }
        });

        return new QueryMethodResult(schemasList.size(), schemasList);
    }

    @Override
    protected QueryMethodResult saveOrUpdateBatchMethod(QueryRunner connect, AniHandler queryEntity, BaseOracleDatabaseInfo databaseConf, BaseOracleTableInfo tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "OracleServiceImpl.saveOrUpdateBatchMethod");
    }

    @Override
    protected QueryMethodResult alterTableMethod(QueryRunner connect, AniHandler queryEntity, BaseOracleDatabaseInfo databaseConf, BaseOracleTableInfo tableConf) throws Exception {
        List<String> sqlList = queryEntity.getAniAlterTable().getSqlList();

        executeByConnection(connect, databaseConf, (connection)->{
            Statement statement = null;
            try {
                statement = connection.createStatement();
                for (String sql : sqlList) {
                    if (log.isDebugEnabled()) {
                        log.debug("alterTable 执行修改的sql是: {}", sql);
                    }
                    recordExecuteStatementLog(sql, null);
                    statement.execute(sql);
                }
            }catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }finally {
                if (statement != null) {
                    try {
                        statement.close();
                    } catch (SQLException e) {
                        log.error("oracle ResultSet 对象关闭失败!", e);
                    }
                }
            }
        });

        return new QueryMethodResult();
    }

    @Override
    protected QueryMethodResult tableExistsMethod(QueryRunner connect, BaseOracleDatabaseInfo databaseConf, BaseOracleTableInfo tableConf) throws Exception {

        String tableName = tableConf.getTableName();
        if(databaseConf.isIgnoreCase()) {
            tableName = tableName.toUpperCase();
        }
        Map<String, Object> query = queryExecute(databaseConf, connect, EXISTS_TABLE_SQL, new MapHandler(), new Object[]{databaseConf.getSchema(), tableName});
        Long total = Long.parseLong(query.get("count").toString());
        Map<String, Object> map = new HashMap<>(1);
        if (total > 0) {
            map.put(tableConf.getTableName(), true);
        } else {
            map.put(tableConf.getTableName(), false);
        }
        return new QueryMethodResult(1, CollectionUtil.newArrayList(map));
    }

    @Override
    protected QueryMethodResult queryTableInformationMethod(QueryRunner connect, BaseOracleDatabaseInfo databaseConf, BaseOracleTableInfo tableConf) throws Exception {
        // SQL 执行
        Map<String, Object> metaMap = queryExecute(databaseConf, connect, TABLE_META, new MapHandler(), new Object[]{databaseConf.getSchema(), tableConf.getTableName()});
        // 元数据 KEY 转换
        if (CollectionUtil.isNotEmpty(metaMap)) {
            Map<String, Object> result = new HashMap<>(metaMap.size());
            TableInformation tableInformation = TableInformation.builder().tableName(tableConf.getTableName()).extendMeta(result).build();
            for (Map.Entry<String, Object> entry : metaMap.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();
                result.put(key, value);
            }
            metaMap = queryExecute(databaseConf, connect, GET_TABLE_COMMENT, new MapHandler(), new Object[]{databaseConf.getSchema(), tableConf.getTableName()});
            if(CollectionUtil.isNotEmpty(metaMap)) {
                for (Map.Entry<String, Object> entry : metaMap.entrySet()) {
                    String key = entry.getKey();
                    Object value = entry.getValue();
                    result.put(key, value);
                }
            }
            return new QueryMethodResult(1, CollectionUtil.newArrayList(JsonUtil.entityToMap(tableInformation)));
        } else {
            throw new BusinessException(ExceptionCode.NOT_FOUND_TABLE_EXCEPTION, tableConf.getTableName());
        }
    }

    @Override
    protected QueryMethodResult queryListDatabaseMethod(QueryRunner connect, AniHandler queryEntity, BaseOracleDatabaseInfo databaseConf, BaseOracleTableInfo tableConf) throws Exception {
        AniHandler.AniListDatabase listDatabase = queryEntity.getListDatabase();
        String sql = SHOW_DATABASE_SQL;
        Object[] paramArray = new Object[0];
        if(StringUtils.isNotBlank(listDatabase.getDatabaseMatch())) {
            sql = sql + " WHERE NAME LIKE ?";
            paramArray = new Object[1];
            paramArray[0] = listDatabase.getDatabaseMatch();
        }
        List<Map<String, Object>> rows = queryExecute(databaseConf, connect, sql, new MapListHandler(), paramArray);

        return new QueryMethodResult(rows.size(), rows);
    }

    @Override
    protected QueryMethodResult queryDatabaseInformationMethod(QueryRunner connect, AniHandler queryEntity, BaseOracleDatabaseInfo databaseConf, BaseOracleTableInfo tableConf) throws SQLException {
        Object[] paramArray = new Object[2];
        paramArray[0] = databaseConf.getDatabaseName();
        paramArray[1] = databaseConf.getDatabaseName();
        List<Map<String, Object>> rows = queryExecute(databaseConf, connect, SHOW_DATABASE_INFO_SQL, new MapListHandler(), paramArray);
        return new QueryMethodResult(rows.size(), rows);
    }

    private void executeByConnection(QueryRunner connect, BaseOracleDatabaseInfo databaseConf, Consumer<Connection> consumer) throws SQLException {
        Connection connection = getConnection(databaseConf, connect);
        boolean isClose = false;
        if(connection == null) {
            connection = connect.getDataSource().getConnection();
            connection.setAutoCommit(false);
            isClose = true;
        }

        try {
            consumer.accept(connection);
            if(isClose) {
                connection.commit();
            }
        }catch (Exception e) {
            if(isClose) {
                connection.rollback();
            }
            throw e;
        } finally {
            if(isClose) {
                connection.close();
            }
        }


    }

    @Override
    public QueryMethodResult queryBySqlMethod(BaseOracleDatabaseInfo databaseConf, QueryRunner dbConnect, String sql, List<Object> params) throws Exception {
        try{
            return super.queryBySqlMethod(databaseConf, dbConnect, sql, params);
        }catch (Exception e) {
        String errorMessage = e.getMessage();
        OracleFunctionErrorHandlerInfoTips.printTips(errorMessage);
        throw e;
    }
    }
}
