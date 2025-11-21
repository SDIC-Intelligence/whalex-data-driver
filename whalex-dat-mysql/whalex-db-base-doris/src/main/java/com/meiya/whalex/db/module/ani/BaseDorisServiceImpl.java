package com.meiya.whalex.db.module.ani;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.map.MapUtil;
import cn.hutool.core.util.StrUtil;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.meiya.whalex.converter.JdbcTypeConverter;
import com.meiya.whalex.db.coverter.DorisTypeConverter;
import com.meiya.whalex.db.entity.QueryMethodResult;
import com.meiya.whalex.db.entity.ani.*;
import com.meiya.whalex.db.entity.table.expressions.Expression;
import com.meiya.whalex.db.entity.table.expressions.NamedReference;
import com.meiya.whalex.db.entity.table.expressions.literals.Literal;
import com.meiya.whalex.db.entity.table.expressions.literals.Literals;
import com.meiya.whalex.db.entity.table.expressions.transforms.Transform;
import com.meiya.whalex.db.entity.table.expressions.transforms.Transforms;
import com.meiya.whalex.db.entity.table.partition.TablePartitions;
import com.meiya.whalex.db.entity.table.parttions.Partition;
import com.meiya.whalex.db.entity.table.parttions.Partitions;
import com.meiya.whalex.db.entity.table.types.Type;
import com.meiya.whalex.db.util.DorisUtils;
import com.meiya.whalex.exception.BusinessException;
import com.meiya.whalex.interior.db.constant.PartitionType;
import com.meiya.whalex.interior.db.search.condition.Sort;
import com.meiya.whalex.sql.ani.RdbmsCursorCache;
import com.meiya.whalex.sql.module.SqlParseHandler;
import com.meiya.whalex.util.JsonUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.dbutils.handlers.MapListHandler;
import org.apache.commons.lang3.StringUtils;

import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * MySql 服务
 *
 * @author 黄河森
 * @date 2019/12/23
 * @project whale-cloud-platformX
 */
@Slf4j
public class BaseDorisServiceImpl extends BaseMySqlServiceImpl<DorisQueryRunner, AniHandler, DorisDatabaseInfo, DorisTableInfo, RdbmsCursorCache> {
    /**
     * 删表SQL
     */
    private static final String DROP_TABLE_SQL = "DROP TABLE `%s` FORCE";

    /**
     * 分区解析正则
     */
    private final static Pattern PARTITION_PATTER = Pattern.compile("keys: \\[(.*?)]");


    private static  JdbcTypeConverter typeConverter =new DorisTypeConverter();

    /**
     * 分区属性
     * */

    private static final String PARTITION_TYPE_VALUE_PATTERN_STRING =
            "types: \\[([^\\]]+)\\]; keys: \\[([^\\]]+)\\];";
    private static final Pattern PARTITION_TYPE_VALUE_PATTERN =
            Pattern.compile(PARTITION_TYPE_VALUE_PATTERN_STRING);

    public static final String PARTITION_NAME = "PartitionName";
    public static final String PARTITION_VALUES_RANGE = "Range";
    public static final String PARTITION_ID = "PartitionId";
    public static final String PARTITION_KEY = "PartitionKey";
    public static final String PARTITION_VISIBLE_VERSION = "VisibleVersion";
    public static final String PARTITION_VISIBLE_VERSION_TIME = "VisibleVersionTime";
    public static final String PARTITION_STATE = "State";
    public static final String PARTITION_DATA_SIZE = "DataSize";
    public static final String PARTITION_IS_IN_MEMORY = "IsInMemory";


    /**
     * 封装认证信息
     *
     * @param username
     * @param password
     * @return
     */
    private String basicAuthHeader(String username, String password) {
        final String tobeEncode = username + ":" + password;
        byte[] encoded = Base64.encodeBase64(tobeEncode.getBytes(StandardCharsets.UTF_8));
        return "Basic " + new String(encoded);
    }

    @Override
    protected QueryMethodResult insertMethod(DorisQueryRunner connect, AniHandler queryEntity, DorisDatabaseInfo databaseConf, DorisTableInfo tableConf) throws Exception {
        //数据库prod
        List<DorisDatabaseInfo.IpPort> ipPorts = databaseConf.getIpPorts();
        DorisDatabaseInfo.IpPort ipPort = ipPorts.get(0);
        String fePort = ipPort.getFePort();
        if (StrUtil.isBlank(fePort)) {
            return super.insertMethod(connect, queryEntity, databaseConf, tableConf);
        }
        //表名
        String tableName = tableConf.getTableName();
        //数据库库名
        String databaseName = databaseConf.getDatabaseName();
        //data（json格式），因为抽象层定义只有一个是"sql"，为了尽可能小的改动，所以直接使用这个字段来存储数据
        String jsonData = queryEntity.getAniInsert().getSql();
        // 获取提交模式
        String groupCommit = tableConf.getGroupCommit();
        //入库数据量（用于返回条数使用）
        int count = 0;
        if (queryEntity.getAniInsert().getSql() != null) {
            count = JsonUtil.jsonStrToObject(jsonData, List.class).size();
            connect.insertByStreamLoad(databaseName, tableName, jsonData, groupCommit);
        } else {
            throw new BusinessException("doris入库数据为0，请检查；表名为:" + tableName);
        }
        return new QueryMethodResult(count, null);
    }

    @Override
    protected QueryMethodResult dropTableMethod(DorisQueryRunner connect, AniHandler queryEntity, DorisDatabaseInfo databaseConf, DorisTableInfo tableConf) throws Exception {
        String dropTableSQL = String.format(DROP_TABLE_SQL, tableConf.getTableName());
        int update = updateExecute(databaseConf, connect, dropTableSQL, new Object[0]);
        return new QueryMethodResult(update, null);
    }

    @Override
    protected QueryMethodResult queryPartitionInformationMethod(DorisQueryRunner connect, AniHandler queryEntity, DorisDatabaseInfo databaseConf, DorisTableInfo tableConf) throws Exception {
        List<Map<String, Object>> result = new ArrayList<>();
        String sql = "SHOW PARTITIONS FROM " + tableConf.getTableName();
        List<Map<String, Object>> rows = queryExecute(databaseConf, connect, sql, new MapListHandler(), new Object[0]);
        if (CollectionUtil.isNotEmpty(rows)) {
            for (Map<String, Object> row : rows) {
                if (MapUtil.isNotEmpty(row)) {
                    String partitionName = (String) row.get("PartitionName");
                    String partitionKey = (String) row.get("PartitionKey");
                    String range = (String) row.get("Range");
                    //partitionKey为空，说明不是分区表
                    if(StringUtils.isBlank(partitionKey)) {
                        continue;
                    }
                    TablePartitions tablePartitions = analyzePartition(partitionName, partitionKey, range);
                    result.add(JsonUtil.entityToMap(tablePartitions));
                }
            }
        }
        return new QueryMethodResult(result.size(), result);
    }

    private Transform[] getTablePartitioning(
            DorisQueryRunner connect, AniHandler queryEntity, DorisDatabaseInfo databaseConf, DorisTableInfo tableConf) throws Exception {
        String sql = String.format("SHOW CREATE TABLE `%s`", tableConf.getTableName());
        List<Map<String, Object>> rows = queryExecute(databaseConf, connect, sql, new MapListHandler(), new Object[0]);
        StringBuilder createTableSql = new StringBuilder();
        if (CollectionUtil.isNotEmpty(rows)) {
            for (Map<String, Object> row : rows) {
                if (MapUtil.isNotEmpty(row)) {
                    createTableSql.append((String) row.get("Create Table"));

                }
            }
        }
        Optional<Transform> transform =
                DorisUtils.extractPartitionInfoFromSql(createTableSql.toString());
        return transform.map(t -> new Transform[]{t}).orElse(Transforms.EMPTY_TRANSFORM);
    }

    @Override
    public QueryMethodResult queryPartitionInformationMethodV2(DorisQueryRunner connect, AniHandler queryEntity, DorisDatabaseInfo databaseConf, DorisTableInfo tableConf) throws Exception {
        List<Map<String, Object>> result = new ArrayList<>();
        Transform[] tablePartitionings =getTablePartitioning(connect,queryEntity,databaseConf,tableConf);
        if (tablePartitionings.length ==0){
            return new QueryMethodResult(0, null);
        }
        Transform partitionInfo = tablePartitionings[0];
        Map<String, Type> columnTypes = getColumnType(connect,queryEntity,databaseConf,tableConf);
        String showPartitionsSql = String.format("SHOW PARTITIONS FROM `%s`", tableConf.getTableName());
        Connection connection =null;
        Statement statement =null;
        try {
             connection = connect.getDataSource().getConnection();
            ImmutableList.Builder<Partition> partitions = ImmutableList.builder();
              statement = connection.createStatement();
                 ResultSet resultSet = statement.executeQuery(showPartitionsSql);
                    while (resultSet.next()) {
                        Partition partition =fromDorisPartition(resultSet, partitionInfo, columnTypes);
                        if (partition == null){
                            continue;
                        }
                       // objectMapper.writeValueAsString(partition);
                        result.add(partition.toMap());
                    }


        } catch (Exception e) {
            log.error("queryPartitionInformationMethodV2 error:?,错误:?",tableConf.getTableName(),e.getMessage());
        }finally {
          if (statement !=null){
              statement.close();
          }
          if (connection!=null){
              connection.close();
          }
        }
        return new QueryMethodResult(result.size(), result);
    }
    private Partition fromDorisPartition(
            ResultSet resultSet, Transform partitionInfo, Map<String, Type> columnTypes)
            throws SQLException {
        String partitionName = resultSet.getString(PARTITION_NAME);
        String partitionKey = resultSet.getString(PARTITION_KEY);
        String partitionValues = resultSet.getString(PARTITION_VALUES_RANGE);
        if (StringUtils.isBlank(partitionName)){
              return null;
        }
        ImmutableMap.Builder<String, String> propertiesBuilder = ImmutableMap.builder();
        propertiesBuilder.put(PARTITION_ID, resultSet.getString(PARTITION_ID));
        propertiesBuilder.put(PARTITION_VISIBLE_VERSION, resultSet.getString(PARTITION_VISIBLE_VERSION));
        propertiesBuilder.put(PARTITION_VISIBLE_VERSION_TIME, resultSet.getString(PARTITION_VISIBLE_VERSION_TIME));
        propertiesBuilder.put(PARTITION_STATE, resultSet.getString(PARTITION_STATE));
        propertiesBuilder.put(PARTITION_KEY, partitionKey);
        propertiesBuilder.put(PARTITION_DATA_SIZE, resultSet.getString(PARTITION_DATA_SIZE));
        propertiesBuilder.put(PARTITION_IS_IN_MEMORY, resultSet.getString(PARTITION_IS_IN_MEMORY));
        ImmutableMap<String, String> properties = propertiesBuilder.build();

        String[] partitionKeys = partitionKey.split(", ");
        List<Expression> partitionKeysExpression =  Arrays.stream(partitionKeys).map(s -> NamedReference.field(s)).collect(Collectors.toList());

        if (partitionInfo instanceof Transforms.RangeTransform) {
            if (partitionKeys.length != 1) {
                throw new UnsupportedOperationException(
                        "Multi-column range partitioning in Doris is not supported yet");
            }
            Type partitionColumnType = columnTypes.get(partitionKeys[0]);
            Literal<?> lower = Literals.NULL;
            Literal<?> upper = Literals.NULL;
            Matcher matcher = PARTITION_TYPE_VALUE_PATTERN.matcher(partitionValues);
            if (matcher.find()) {
                String lowerValue = matcher.group(2);
                lower = Literals.of(lowerValue, partitionColumnType.simpleString());
                if (matcher.find()) {
                    String upperValue = matcher.group(2);
                    upper = Literals.of(upperValue, partitionColumnType.simpleString());
                }
            }
            return Partitions.range(partitionName, upper, lower, properties,partitionKeysExpression);
        } else if (partitionInfo instanceof Transforms.ListTransform) {
            Matcher matcher = PARTITION_TYPE_VALUE_PATTERN.matcher(partitionValues);
            ImmutableList.Builder<Literal<?>[]> lists = ImmutableList.builder();
            while (matcher.find()) {
                String[] values = matcher.group(2).split(", ");
                ImmutableList.Builder<Literal<?>> literValues = ImmutableList.builder();
                for (int i = 0; i < values.length; i++) {
                    Type partitionColumnType = columnTypes.get(partitionKeys[i]);
                    literValues.add(Literals.of(values[i], partitionColumnType.simpleString()));
                }
                lists.add(literValues.build().toArray(new Literal<?>[0]));
            }
            return Partitions.list(
                    partitionName, lists.build().toArray(new Literal<?>[0][0]), properties,partitionKeysExpression);
        } else {
            throw new UnsupportedOperationException(
                    " is not a partitioned table");
        }
    }

    private Map<String, Type> getColumnType(DorisQueryRunner connect, AniHandler queryEntity, DorisDatabaseInfo databaseConf, DorisTableInfo tableConf) throws Exception {
        Connection conn = null;
        try {
            conn = getConnection(databaseConf, connect);
            if(conn == null) {
                conn = connect.getDataSource().getConnection();

            }
            DatabaseMetaData metaData = conn.getMetaData();
            try (ResultSet result =
                         metaData.getColumns(
                                 conn.getCatalog(), conn.getSchema(), tableConf.getTableName(), null)) {
                ImmutableMap.Builder<String, Type> columnTypes = ImmutableMap.builder();
                while (result.next()) {
                    if (Objects.equals(result.getString("TABLE_NAME"), tableConf.getTableName())) {
                        JdbcTypeConverter.JdbcTypeBean typeBean =
                                new JdbcTypeConverter.JdbcTypeBean(result.getString("TYPE_NAME"));
                        typeBean.setColumnSize(result.getInt("COLUMN_SIZE"));
                        typeBean.setScale(result.getInt("DECIMAL_DIGITS"));
                        Type gravitinoType = typeConverter.toGravitino(typeBean);
                        String columnName = result.getString("COLUMN_NAME");
                        columnTypes.put(columnName, gravitinoType);
                    }
                }
                return columnTypes.build();
            }
        } finally  {
           if (conn != null){
               conn.close();
           }
        }
    }

    /**
     * 解析分区字符串
     *
     * @param partitionName
     * @param partitionKey
     * @param range
     * @return
     */
    private TablePartitions analyzePartition(String partitionName, String partitionKey, String range) {
        PartitionType type;
        String[] split;
        if (StringUtils.containsIgnoreCase(range, "..types:")) {
            type = PartitionType.RANGE;
            split = StringUtils.split(range, "..");
        } else {
            type = PartitionType.LIST;
            split = StringUtils.split(range, ",");
        }
        TablePartitions tablePartitions = null;
        List<TablePartitions.TablePartition> partitions = new ArrayList<>();
        for (String list : split) {
            Matcher matcher = PARTITION_PATTER.matcher(list);
            if (matcher.find()) {
                String group = matcher.group(1);
                TablePartitions.TablePartition tablePartition = TablePartitions.TablePartition.builder().partitionKey(partitionKey).partitionValue(group).build();
                partitions.add(tablePartition);
            }
        }
        tablePartitions = TablePartitions.builder().partitionType(type.name()).partitionName(partitionName).partitions(partitions).build();
        return tablePartitions;
    }

    @Override
    protected QueryMethodResult createIndexMethod(DorisQueryRunner connect, AniHandler queryEntity, DorisDatabaseInfo databaseConf, DorisTableInfo tableConf) throws Exception {
        String sql = queryEntity.getAniCreateIndex().getSql();
        if (queryEntity.getAniCreateIndex().isNotExists()) {
            QueryMethodResult indexesMethod = getIndexesMethod(connect, databaseConf, tableConf);
            List<Map<String, Object>> rows = indexesMethod.getRows();
            List<String> indexNames = rows.stream().flatMap(row -> {
                String indexName = (String) row.get("indexName");
                return Stream.of(indexName);
            }).collect(Collectors.toList());
            if (CollectionUtil.isNotEmpty(indexNames) && indexNames.contains(queryEntity.getAniCreateIndex().getIndexName())) {
                return new QueryMethodResult();
            }
        }
        //倒排索引时，有两条sql语句
        String[] split = sql.split(";");
        for (String s : split) {
            updateExecute(databaseConf, connect, s, new Object[0]);
        }

        return new QueryMethodResult();
    }

    @Override
    protected QueryMethodResult getIndexesMethod(DorisQueryRunner connect, DorisDatabaseInfo databaseConf, DorisTableInfo tableConf) throws Exception {
        QueryMethodResult indexesMethod = super.getIndexesMethod(connect, databaseConf, tableConf);
        if (indexesMethod.isSuccess()) {
            List<Map<String, Object>> rows = indexesMethod.getRows();
            // doris 将主键模型的 UNIQUE KEY 作主键
            String sql = String.format("select * from information_schema.columns where table_name = '%s' and table_schema = '%s'", tableConf.getTableName(), databaseConf.getDatabaseName());
            List<Map<String, Object>> columnsList = queryExecute(databaseConf, connect, sql, new MapListHandler(), new Object[0]);
            Map<String, String> primaryKeyList = new HashMap<>();
            for (Map<String, Object> map : columnsList) {
                Object column_key = map.get("COLUMN_KEY");
                if(column_key.equals("UNI")) {
                    primaryKeyList.put((String) map.get("COLUMN_NAME"), Sort.ASC.name());
                }
            }
            if (CollectionUtil.isNotEmpty(primaryKeyList)) {
                Map<String, Object> resultMap = new HashMap<>(1);
                rows.add(resultMap);
                resultMap.put("column", CollectionUtil.join(primaryKeyList.keySet(), ","));
                resultMap.put("indexName", CollectionUtil.join(primaryKeyList.keySet(), "_") + "_unique_key");
                resultMap.put("columns", primaryKeyList);
                // 主键
                resultMap.put("isUnique", true);
                resultMap.put("isPrimaryKey", true);
            }
        }
        return indexesMethod;
    }

    @Override
    protected QueryMethodResult querySchemaMethod(DorisQueryRunner connect, DorisDatabaseInfo databaseConf, DorisTableInfo tableConf) throws Exception {

        if(StringUtils.isNotBlank(databaseConf.getCatalog())) {
            return super.querySchemaMethod(connect, databaseConf, tableConf);
        }

        List<Map<String, Object>> schemaInfoList = new ArrayList<>();
        Connection conn = null;
        ResultSet columnSet = null;
        String tableName = tableConf.getTableName();
        boolean isTransaction = true;
        try {
            conn = getConnection(databaseConf, connect);
            if(conn == null) {
                conn = connect.getDataSource().getConnection();
                isTransaction = false;
            }
            String sql = String.format("select * from information_schema.columns where table_name = '%s' and table_schema = '%s'", tableName, databaseConf.getDatabaseName());
            List<Map<String, Object>> columnsList = queryExecute(databaseConf, connect, sql, new MapListHandler(), new Object[0]);
            List<String> primaryKeyList = new ArrayList<>();
            for (Map<String, Object> map : columnsList) {
                Object column_key = map.get("COLUMN_KEY");
                if(column_key.equals("UNI")) {
                    primaryKeyList.add((String) map.get("COLUMN_NAME"));
                }
            }
            // 获取主键
//            primaryKeys = conn.getMetaData().getPrimaryKeys(databaseConf.getDatabaseName(), null, tableName);
//            while (primaryKeys.next()) {
//                primaryKeyList.add(primaryKeys.getString("COLUMN_NAME"));
//            }
            columnSet = conn.getMetaData().getColumns(databaseConf.getDatabaseName(), null, tableName, "%");

            while (columnSet.next()) {
                Map<String, Object> map = new HashMap<>();
                String columnName = columnSet.getString("COLUMN_NAME");
                map.put("col_name", columnName);
                String isAutoincrement = columnSet.getString("IS_AUTOINCREMENT");
                if (StringUtils.equalsIgnoreCase(isAutoincrement, "YES")) {
                    map.put("autoincrement", 1);
                } else {
                    map.put("autoincrement", 0);
                }
                int isKey = 0;
                if (primaryKeyList.contains(columnName)) {
                    //主键
                    isKey = 1;
                }
                map.put("isKey", isKey);
                String columnDesc = columnSet.getString("REMARKS");
                columnDesc = columnDesc == null ? "" : columnDesc;
                map.put("comment", columnDesc);
                String columnType = columnSet.getString("TYPE_NAME").toLowerCase();
                // GEOMETRY 为 mysql 几何类型的基类，因为这边将基类转为 POINT 类型
                if (StringUtils.equalsIgnoreCase(columnType, "GEOMETRY")) {
                    columnType = "point";
                }
                if (StringUtils.containsIgnoreCase(columnType, "unsigned")) {
                    columnType = StringUtils.replaceIgnoreCase(columnType, "unsigned", "").trim();
                    map.put("unsigned", true);
                }

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
                map.put("std_data_type", getItemFieldTypeEnum(columnType, StringUtils.isNotBlank(columnSize) ? Integer.valueOf(columnSize) : null, databaseConf.getBit1isBoolean()).getVal());
                int nullableInt = columnSet.getInt("NULLABLE");
                map.put("nullable", nullableInt);
                String columnDef = columnSet.getString("COLUMN_DEF");
                columnDef = columnDef == null ? "" : columnDef;
                map.put("columnDef", columnDef);
                schemaInfoList.add(map);
            }
        } finally {
            if (columnSet != null) {
                try {
                    columnSet.close();
                } catch (SQLException e) {
                    log.error("MySql ResultSet 对象关闭失败!", e);
                }
            }

            if (conn != null && !isTransaction) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    log.error("MySql conn 对象关闭失败!", e);
                }
            }
        }
        return new QueryMethodResult(schemaInfoList.size(), schemaInfoList);
    }



    @Override
    protected QueryMethodResult showTablesMethod(DorisQueryRunner connect, AniHandler queryEntity, DorisDatabaseInfo databaseConf) throws Exception {

        if(StringUtils.isBlank(databaseConf.getCatalog())) {
            return super.showTablesMethod(connect, queryEntity, databaseConf);
        }

        String sql = "show tables";
        if (StringUtils.isNotBlank(queryEntity.getListTable().getTableMatch())) {
            sql = sql + " like \'" + queryEntity.getListTable().getTableMatch() + "\'";
        }
        List<Map<String, Object>> query = queryExecute(databaseConf, connect, sql, new MapListHandler(), new Object[0]);
        for (Map<String, Object> map : query) {
            String key = map.keySet().iterator().next();
            map.put("tableName", map.get(key));
            map.remove(key);
            map.put("tableComment", map.get("TABLE_COMMENT"));
            map.remove("TABLE_COMMENT");
            map.put("schemaName", map.get("TABLE_SCHEMA"));
            map.remove("TABLE_SCHEMA");
            map.put("tableType", map.get("TABLE_TYPE"));
            map.remove("TABLE_TYPE");
        }
        return new QueryMethodResult(query.size(), query);
    }

    @Override
    protected QueryMethodResult tableExistsMethod(DorisQueryRunner connect, DorisDatabaseInfo databaseConf, DorisTableInfo tableConf) throws Exception {
        if(StringUtils.isBlank(databaseConf.getCatalog())) {
            return super.tableExistsMethod(connect, databaseConf, tableConf);
        }
        String sql = "show tables like '" + tableConf.getTableName() + "'";
        List<Map<String, Object>> query = queryExecute(databaseConf, connect, sql, new MapListHandler(), new Object[0]);
        Map<String, Object> map = new HashMap<>(1);
        if (CollectionUtil.isNotEmpty(query)) {
            map.put(tableConf.getTableName(), true);
        } else {
            map.put(tableConf.getTableName(), false);
        }
        return new QueryMethodResult(1, CollectionUtil.newArrayList(map));
    }

    @Override
    protected SqlParseHandler getSqlParseHandler(DorisDatabaseInfo database) {
        return new DorisSqlParseHandler();
    }
}
