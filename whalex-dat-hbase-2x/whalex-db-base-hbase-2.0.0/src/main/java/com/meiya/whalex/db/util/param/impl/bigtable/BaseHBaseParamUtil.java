package com.meiya.whalex.db.util.param.impl.bigtable;

import cn.hutool.core.codec.Base64;
import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.map.MapUtil;
import com.meiya.whalex.db.entity.*;
import com.meiya.whalex.db.entity.bigtable.HBaseDatabaseInfo;
import com.meiya.whalex.db.entity.bigtable.HBaseHandler;
import com.meiya.whalex.db.entity.bigtable.HBaseTableInfo;
import com.meiya.whalex.db.util.param.AbstractDbModuleParamUtil;
import com.meiya.whalex.exception.BusinessException;
import com.meiya.whalex.exception.ExceptionCode;
import com.meiya.whalex.interior.db.constant.CommonConstant;
import com.meiya.whalex.interior.db.operation.in.AlterTableParamCondition;
import com.meiya.whalex.interior.db.operation.in.CreateTableParamCondition;
import com.meiya.whalex.interior.db.operation.in.QueryDatabasesCondition;
import com.meiya.whalex.interior.db.operation.in.QueryTablesCondition;
import com.meiya.whalex.interior.db.search.in.Page;
import com.meiya.whalex.interior.db.search.in.QueryParamCondition;
import com.meiya.whalex.interior.db.search.in.Where;
import com.meiya.whalex.util.date.DateUtil;
import com.meiya.whalex.util.date.JodaTimeUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * HBase组件参数转换工具
 *
 * @author 黄河森
 * @date 2019/12/16
 * @project whale-cloud-platformX
 */
@Slf4j
public class BaseHBaseParamUtil<Q extends HBaseHandler, D extends HBaseDatabaseInfo, T extends HBaseTableInfo> extends AbstractDbModuleParamUtil<Q, D, T> {
    @Override
    protected Q transitionListTableParam(QueryTablesCondition queryTablesCondition, D databaseConf) throws Exception {
        HBaseHandler hBaseHandler = new HBaseHandler();
        HBaseHandler.HBaseListTable listTable = new HBaseHandler.HBaseListTable();
        hBaseHandler.setListTable(listTable);
        listTable.setTableMatch(queryTablesCondition.getTableMatch());
        return (Q) hBaseHandler;
    }

    @Override
    protected Q transitionListDatabaseParam(QueryDatabasesCondition queryDatabasesCondition, D databaseConf) throws Exception {
        HBaseHandler hBaseHandler = new HBaseHandler();
        HBaseHandler.HBaseListDatabase listDatabase = new HBaseHandler.HBaseListDatabase();
        listDatabase.setDatabaseMatch(queryDatabasesCondition.getDatabaseMatch());
        return (Q) hBaseHandler;
    }

    @Override
    protected Q transitionCreateTableParam(CreateTableParamCondition createTableParamCondition, D databaseConf, T tableConf) {
        HBaseHandler hBaseHandler = new HBaseHandler();
        HBaseHandler.HBaseCreateTable createTable = new HBaseHandler.HBaseCreateTable();
        hBaseHandler.setCreateTable(createTable);
        String startTime = createTableParamCondition.getStartTime();
        String endTime = createTableParamCondition.getEndTime();
        if (StringUtils.isNotBlank(startTime) && StringUtils.isNotBlank(endTime)) {
            Date startDate = DateUtil.convertToDate(createTableParamCondition.getStartTime(), JodaTimeUtil.DEFAULT_YMD_FORMAT);
            Date endDate = DateUtil.convertToDate(createTableParamCondition.getEndTime(), JodaTimeUtil.DEFAULT_YMD_FORMAT);
            createTable.setStartTime(startDate);
            createTable.setStopTime(endDate);
        }

        // 字段组装
        List<CreateTableParamCondition.CreateTableFieldParam> createTableFieldParamList = createTableParamCondition.getCreateTableFieldParamList();
        if (CollectionUtil.isNotEmpty(createTableFieldParamList)) {
            List<Map<String, String>> collect = createTableFieldParamList.stream().flatMap(createTableFieldParam -> {
                String fieldName = createTableFieldParam.getFieldName();

                String compressionType = createTableFieldParam.getCompressionType();
                if(StringUtils.isBlank(compressionType)) {
                    compressionType = "SNAPPY";
                }
                Map<String, String> rowMap = buildColumnCluster(fieldName, compressionType);
                return Stream.of(rowMap);
            }).collect(Collectors.toList());
            createTable.setSchemaTemPlate(collect);
        }

        createTable.setRowKeyPartition(createTableParamCondition.getRowKeyPartition());
        return (Q) hBaseHandler;
    }

    protected Map<String, String> buildColumnCluster(String name, String compressionType) {
        return MapUtil.builder("NAME", name).put("BLOOMFILTER", "ROW")
                .put("VERSIONS", "1")
                .put("COMPRESSION", compressionType).build();
    }

    @Override
    protected Q transitionAlterTableParam(AlterTableParamCondition alterTableParamCondition, D databaseConf, T tableConf) throws Exception {
        HBaseHandler.HBaseAlterTable alterTable = new HBaseHandler.HBaseAlterTable();
        // 新增字段
        List<AlterTableParamCondition.AddTableFieldParam> addTableFieldParamList = alterTableParamCondition.getAddTableFieldParamList();
        if (CollectionUtil.isNotEmpty(addTableFieldParamList)) {
            List<HColumnDescriptor> addColumns = new ArrayList<>();
            for (AlterTableParamCondition.AddTableFieldParam addTableFieldParam : addTableFieldParamList) {
                String fieldName = addTableFieldParam.getFieldName();
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(fieldName);
                hColumnDescriptor.setMinVersions(1);
                hColumnDescriptor.setCompressionType(Compression.Algorithm.SNAPPY);
                hColumnDescriptor.setBloomFilterType(BloomType.ROW);
                addColumns.add(hColumnDescriptor);
            }
            alterTable.setAddColumns(addColumns);
        }

        // 删除字段
        List<String> delTableFieldParamList = alterTableParamCondition.getDelTableFieldParamList();
        if (CollectionUtil.isNotEmpty(delTableFieldParamList)) {
            List<byte[]> delColumns = new ArrayList<>();
            for (String fieldName : delTableFieldParamList) {
                delColumns.add(Bytes.toBytes(fieldName));
            }
            alterTable.setDelColumns(delColumns);
        }

        HBaseHandler hBaseHandler = new HBaseHandler();
        hBaseHandler.setAlterTable(alterTable);
        return (Q) hBaseHandler;
    }

    @Override
    protected Q transitionCreateIndexParam(IndexParamCondition indexParamCondition, D databaseConf, T tableConf) {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "BaseHBaseParamUtil.transitionCreateIndexParam");
    }

    @Override
    protected Q transitionDropIndexParam(IndexParamCondition indexParamCondition, D databaseConf, T tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "BaseHBaseParamUtil.transitionDropIndexParam");
    }

    @Override
    protected Q transitionQueryParam(QueryParamCondition queryParamCondition, D databaseConf, T tableConf) throws Exception {
        HBaseHandler hBaseHandler = new HBaseHandler();
        HBaseHandler.HBaseQuery hBaseQuery = new HBaseHandler.HBaseQuery();
        hBaseHandler.setQuery(hBaseQuery);

        // 存储 rowKey
        List<byte[]> rowKeyList = new ArrayList<>();

        // 存放范围查询
        Map<String, List<Object>> rangeMap = new HashMap<>();

        // 转换条件
        List<Where> whereList = queryParamCondition.getWhere();
        Scan scan = null;
        if (CollectionUtils.isNotEmpty(whereList)) {
            scan = hBaseWhereQuery(whereList, rowKeyList, null, rangeMap);
        }

        // 返回字段
        List<String> select = queryParamCondition.getSelect();
        if (CollectionUtil.isNotEmpty(select)) {
            boolean remove = select.remove(CommonConstant.BASE64_ROW_KEY);
            hBaseQuery.setReturnRowKey(remove);
        }
        Map<byte[], NavigableSet<byte[]>> familyMap = hBaseSelectQuery(select);

        // 构造 GET 集合
        List<Get> getList = new ArrayList<>(rowKeyList.size());
        hBaseQuery.setGetList(getList);
        for (byte[] row : rowKeyList) {
            Get get = new Get(row);
            /*if (family != null && qualifier != null) {
                get.addColumn(family, qualifier);
            } else if (family != null) {
                get.addFamily(family);
            }*/
            if (queryParamCondition.getMaxVersion() != null) {
                get.setMaxVersions(queryParamCondition.getMaxVersion());
            }
            if (familyMap != null && !familyMap.isEmpty()) {
                for (Map.Entry<byte[], NavigableSet<byte[]>> entry : familyMap.entrySet()) {
                    if (entry.getKey() == null) {
                        continue;
                    }
                    if (entry.getValue() == null || entry.getValue().isEmpty()) {
                        get.addFamily(entry.getKey());
                    }
                    for (byte[] bytes : entry.getValue()) {
                        get.addColumn(entry.getKey(), bytes);
                    }
                }
            }
            getList.add(get);
        }

        for (Map.Entry<String, List<Object>> entry : rangeMap.entrySet()) {
            List<Object> value = entry.getValue();
            if (CommonConstant.BURST_ZONE.equals(entry.getKey())) {
                hBaseQuery.setStartTime(value.get(0).toString());
                hBaseQuery.setStopTime(value.get(1).toString());
            }
        }

        // 设置查询页码（SCAN 使用）
        Page page = queryParamCondition.getPage();
        if (page != null) {
            hBaseQuery.setSkip(page.getOffset());
            hBaseQuery.setPageSize(page.getLimit());
        }

        hBaseQuery.setScan(scan);
        // 字段查询版本数
        hBaseQuery.setMaxVersion(queryParamCondition.getMaxVersion());

        return (Q) hBaseHandler;
    }

    @Override
    protected Q transitionUpdateParam(UpdateParamCondition updateParamCondition, D databaseConf, T tableConf) {
        HBaseHandler hBaseHandler = new HBaseHandler();
        HBaseHandler.HBaseUpdate hBaseUpdate = new HBaseHandler.HBaseUpdate();
        hBaseHandler.setUpdate(hBaseUpdate);
        List<Put> puts = new ArrayList<>();
        // 更新的值
        Map<String, Object> updateParamMap = updateParamCondition.getUpdateParamMap();
        // 更新条件，仅可设置 rowKey 和 分片范围
        List<Where> whereList = updateParamCondition.getWhere();
        // 存储 rowKey
        List<byte[]> rowKeyList = new ArrayList<>();

        // 存放范围查询
        Map<String, List<Object>> rangeMap = new HashMap<>();

        if (CollectionUtils.isNotEmpty(whereList)) {
            hBaseWhereQuery(whereList, rowKeyList, null, rangeMap);
        } else {
            throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "HBase 更新必须指定 RowKey 条件");
        }

        if (CollectionUtil.isEmpty(rowKeyList)) {
            throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "HBase 更新必须指定 RowKey 条件");
        }

        List<Map<String, Object>> updateParamMapList = new ArrayList<>();

        for (int i = 0; i < rowKeyList.size(); i++) {

            byte[] bytes = rowKeyList.get(i);
            Put put = new Put(bytes);
            for (Map.Entry<String, Object> entry : updateParamMap.entrySet()) {
                String family = entry.getKey();
                Object value = entry.getValue();
                if (!family.equalsIgnoreCase(CommonConstant.ROW_KEY) && value instanceof Map) {
                    Map<String, Object> qualifierMap = (Map<String, Object>) value;
                    for (Map.Entry<String, Object> qualifierEntry : qualifierMap.entrySet()) {
                        if (qualifierEntry.getValue() instanceof byte[]) {
                            put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifierEntry.getKey()), (byte[]) qualifierEntry.getValue());
                        } else {
                            put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifierEntry.getKey()), Bytes.toBytes(qualifierEntry.getValue().toString()));
                        }
                    }
                }
            }
            puts.add(put);

            Map<String, Object> fieldValueMap = new HashMap<>();
            fieldValueMap.put(CommonConstant.ROW_KEY, bytes);
            fieldValueMap.putAll(updateParamMap);
            updateParamMapList.add(fieldValueMap);
        }
        // 获取分片周期
        for (Map.Entry<String, List<Object>> entry : rangeMap.entrySet()) {
            List<Object> value = entry.getValue();
            if (CommonConstant.BURST_ZONE.equals(entry.getKey())) {
                hBaseUpdate.setStartTime(value.get(0).toString());
                hBaseUpdate.setStopTime(value.get(1).toString());
            }
        }

        hBaseUpdate.setFieldValueList(updateParamMapList);
        hBaseUpdate.setPutList(puts);
        return (Q) hBaseHandler;
    }

    @Override
    protected Q transitionInsertParam(AddParamCondition addParamCondition, D databaseConf, T tableConf) {
        HBaseHandler hBaseHandler = new HBaseHandler();
        HBaseHandler.HBaseInsert hBaseInsert = new HBaseHandler.HBaseInsert();
        hBaseHandler.setInsert(hBaseInsert);
        List<Put> puts = new ArrayList<>();
        List<Map<String, Object>> fieldValueList = addParamCondition.getFieldValueList();
        for (int i = 0; i < fieldValueList.size(); i++) {
            Map<String, Object> fieldMap = fieldValueList.get(i);
            Put put;
            if (fieldMap.get(CommonConstant.ROW_KEY) instanceof byte[]) {
                put = new Put((byte[]) fieldMap.get(CommonConstant.ROW_KEY));
            } else {
                put = new Put(Bytes.toBytes(fieldMap.get(CommonConstant.ROW_KEY).toString()));
            }
            for (Map.Entry<String, Object> entry : fieldMap.entrySet()) {
                String family = entry.getKey();
                Object value = entry.getValue();
                if (!family.equalsIgnoreCase(CommonConstant.ROW_KEY) && value instanceof Map) {
                    Map<String, Object> qualifierMap = (Map<String, Object>) value;
                    for (Map.Entry<String, Object> qualifierEntry : qualifierMap.entrySet()) {
                        if (qualifierEntry.getValue() instanceof byte[]) {
                            put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifierEntry.getKey()), (byte[]) qualifierEntry.getValue());
                        } else {
                            put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifierEntry.getKey()), Bytes.toBytes(qualifierEntry.getValue().toString()));
                        }
                    }
                }
            }
            puts.add(put);
        }
        hBaseInsert.setPutList(puts);
        hBaseInsert.setFieldValueList(fieldValueList);
        hBaseInsert.setCaptureTime(addParamCondition.getCaptureTime());
        return (Q) hBaseHandler;
    }

    @Override
    protected Q transitionDeleteParam(DelParamCondition delParamCondition, D databaseConf, T tableConf) {
        HBaseHandler hBaseHandler = new HBaseHandler();
        HBaseHandler.HBaseDel hBaseDel = new HBaseHandler.HBaseDel();
        hBaseHandler.setDelete(hBaseDel);
        List<Delete> deletes = new ArrayList<>();
        // 更新条件，仅可设置 rowKey 和 分片范围
        List<Where> whereList = delParamCondition.getWhere();
        // 存储 rowKey
        List<byte[]> rowKeyList = new ArrayList<>();

        List<String> rowKeyStrList = new ArrayList<>();

        // 存储删除的指定列簇和列
        List<String> columnList = new ArrayList<>();

        // 存放范围查询
        Map<String, List<Object>> rangeMap = new HashMap<>();

        if (CollectionUtils.isNotEmpty(whereList)) {
            hBaseWhereQuery(whereList, rowKeyList, columnList, rangeMap);
        } else {
            throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "HBase 删除必须指定 RowKey 条件");
        }

        if (CollectionUtil.isEmpty(rowKeyList)) {
            throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "HBase 删除必须指定 RowKey 条件");
        }

        for (int i = 0; i < rowKeyList.size(); i++) {
            byte[] bytes = rowKeyList.get(i);
            rowKeyStrList.add(new String(bytes));
            Delete delete = new Delete(bytes);
            if (CollectionUtil.isNotEmpty(columnList)) {
                for (String column : columnList) {
                    String[] split = StringUtils.split(column, ":");
                    if (split.length == 2) {
                        delete.addColumn(split[0].getBytes(), split[1].getBytes());
                    } else if (split.length == 1) {
                        delete.addFamily(split[0].getBytes());
                    } else {
                        log.warn("HBase 删除列簇或者列条件无法识别: {}", column);
                    }
                }
            }
            deletes.add(delete);
        }
        // 获取分片周期
        for (Map.Entry<String, List<Object>> entry : rangeMap.entrySet()) {
            List<Object> value = entry.getValue();
            if (CommonConstant.BURST_ZONE.equals(entry.getKey())) {
                hBaseDel.setStartTime(value.get(0).toString());
                hBaseDel.setStopTime(value.get(1).toString());
            }
        }

        hBaseDel.setDeleteList(deletes);
        hBaseDel.setDeleteStrList(rowKeyStrList);
        return (Q) hBaseHandler;
    }

    @Override
    protected Q transitionDropTableParam(DropTableParamCondition dropTableParamCondition, D databaseConf, T tableConf) throws Exception {
        HBaseHandler hBaseHandler = new HBaseHandler();
        HBaseHandler.HBaseDropTable dropTable = new HBaseHandler.HBaseDropTable();
        if (dropTableParamCondition != null) {
            if (StringUtils.isNotBlank(dropTableParamCondition.getStartTime())) {
                dropTable.setStartTime(DateUtil.convertToDate(dropTableParamCondition.getStartTime(), JodaTimeUtil.DEFAULT_YMD_FORMAT));
            }
            if (StringUtils.isNotBlank(dropTableParamCondition.getEndTime())) {
                dropTable.setStopTime(DateUtil.convertToDate(dropTableParamCondition.getEndTime(), JodaTimeUtil.DEFAULT_YMD_FORMAT));
            }
        }
        hBaseHandler.setDropTable(dropTable);
        return (Q) hBaseHandler;
    }

    @Override
    public Q transitionUpsertParam(UpsertParamCondition paramCondition, D databaseConf, T tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "BaseHBaseParamUtil.transitionUpsertParam");
    }

    @Override
    public Q transitionUpsertParamBatch(UpsertParamBatchCondition paramCondition, D databaseConf, T tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "BaseHBaseParamUtil.transitionUpsertParamBatch");
    }

    /**
     * 循环递归拼接查询条件
     *
     * @param whereList
     * @param rowKeyList
     */
    private static Scan hBaseWhereQuery(List<Where> whereList, List<byte[]> rowKeyList, List<String> columnList, Map<String, List<Object>> rangeMap) {
        Scan scan = null;
        for (Where where : whereList) {
            switch (where.getType()) {
                case AND:
                case OR:
                    scan = hBaseWhereQuery(where.getParams(), rowKeyList, columnList, rangeMap);
                    break;
                case IN:
                case EQ:
                case TERM:
                    String field = where.getField();
                    if (field.equals(CommonConstant.ROW_KEY)) {
                        Object param = where.getParam();
                        if (param instanceof List) {
                            List<String> paramList = (List<String>) param;
                            for (String val : paramList) {
                                rowKeyList.add(Bytes.toBytes(val));
                            }
                        } else {
                            rowKeyList.add(Bytes.toBytes(String.valueOf(param)));
                        }
                    } else if (field.equals(CommonConstant.BASE64_ROW_KEY)) {
                        Object param = where.getParam();
                        if (param instanceof List) {
                            List<String> paramList = (List<String>) param;
                            for (String val : paramList) {
                                rowKeyList.add(cn.hutool.core.codec.Base64.decode(val));
                            }
                        } else {
                            rowKeyList.add(cn.hutool.core.codec.Base64.decode(String.valueOf(param)));
                        }
                    } else if (field.equals(CommonConstant.HBASE_DEL_COLUMNS) && columnList != null) {
                        Object param = where.getParam();
                        if (param instanceof List) {
                            List<String> paramList = (List<String>) param;
                            columnList.addAll(paramList);
                        } else {
                            columnList.add(String.valueOf(param));
                        }
                    }
                    break;
                case LT:
                case LTE:
                    List<Object> ltList = rangeMap.get(where.getField());
                    if (CollectionUtils.isEmpty(ltList)) {
                        ltList = new ArrayList<>();
                        rangeMap.put(where.getField(), ltList);
                    }
                    ltList.add(1, where.getParam());
                    break;
                case GT:
                case GTE:
                    List<Object> gttList = rangeMap.get(where.getField());
                    if (CollectionUtils.isEmpty(gttList)) {
                        gttList = new ArrayList<>();
                        rangeMap.put(where.getField(), gttList);
                    }
                    gttList.add(0, where.getParam());
                    break;
                case LIKE:
                    String field2 = where.getField();
                    if (field2.equals(CommonConstant.ROW_KEY)) {
                        Object param = where.getParam();
                        RowFilter rowFilter = new RowFilter(CompareOperator.EQUAL, new SubstringComparator(String.valueOf(param)));
                        scan = new Scan();
                        scan.setFilter(rowFilter);
                    } else if (field2.equals(CommonConstant.BASE64_ROW_KEY)) {
                        Object param = where.getParam();
                        String decodeStr = Base64.decodeStr(String.valueOf(param));
                        RowFilter rowFilter = new RowFilter(CompareOperator.EQUAL, new SubstringComparator(decodeStr));
                        scan = new Scan();
                        scan.setFilter(rowFilter);
                    }
                    break;
                default:
                    break;
            }
        }
        return scan;
    }

    /**
     * 解析 HBase 查询字段
     *
     * @param select
     */
    private static Map<byte[], NavigableSet<byte[]>> hBaseSelectQuery(List<String> select) {
        if (CollectionUtils.isNotEmpty(select)) {
            Map<byte[], NavigableSet<byte[]>> familyMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);
            for (String s : select) {
                if (!StringUtils.contains(s, ":[")) {
                    continue;
                }
                s = StringUtils.replaceEach(s, new String[]{"[", "]"}, new String[]{"", ""});
                String[] split = StringUtils.split(s, ":");
                if (split == null || split.length != 2) {
                    continue;
                }
                String k = split[0];
                String vs = split[1];
                String[] vsarr = StringUtils.split(vs, ",");
                if (vsarr == null || Objects.equals("all", StringUtils.lowerCase(vsarr[0]))) {
                    familyMap.put(Bytes.toBytes(k), new TreeSet<>(Bytes.BYTES_COMPARATOR));
                    continue;
                }
                TreeSet<byte[]> bytes = new TreeSet<>(Bytes.BYTES_COMPARATOR);
                for (String v : vsarr) {
                    bytes.add(Bytes.toBytes(v));
                }
                familyMap.put(Bytes.toBytes(k), bytes);
            }
            return  familyMap;
        }
        return null;
    }
}
