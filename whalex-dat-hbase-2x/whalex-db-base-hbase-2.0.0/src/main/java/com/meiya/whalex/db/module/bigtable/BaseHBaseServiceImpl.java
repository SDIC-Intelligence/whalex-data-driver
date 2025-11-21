package com.meiya.whalex.db.module.bigtable;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.map.MapBuilder;
import com.meiya.whalex.db.constant.DatatypePeriodConstants;
import com.meiya.whalex.db.custom.bigtable.HBaseTemplate;
import com.meiya.whalex.db.custom.bigtable.ResultCallback;
import com.meiya.whalex.db.custom.bigtable.TableTemplateCallback;
import com.meiya.whalex.db.entity.QueryCursorMethodResult;
import com.meiya.whalex.db.entity.QueryMethodResult;
import com.meiya.whalex.db.entity.table.infomaration.HBaseTableInformation;
import com.meiya.whalex.db.entity.VersionResult;
import com.meiya.whalex.db.entity.bigtable.HBaseCursorCache;
import com.meiya.whalex.db.entity.bigtable.HBaseDatabaseInfo;
import com.meiya.whalex.db.entity.bigtable.HBaseHandler;
import com.meiya.whalex.db.entity.bigtable.HBaseTableInfo;
import com.meiya.whalex.db.module.AbstractDbModuleBaseService;
import com.meiya.whalex.db.module.DatabaseExecuteStatementLog;
import com.meiya.whalex.db.util.common.DatatypePeriodUtils;
import com.meiya.whalex.db.util.helper.impl.bigtable.BaseHBaseConfigHelper;
import com.meiya.whalex.exception.BusinessException;
import com.meiya.whalex.exception.ExceptionCode;
import com.meiya.whalex.interior.db.constant.CommonConstant;
import com.meiya.whalex.interior.db.constant.DbResourceEnum;
import com.meiya.whalex.util.JsonUtil;
import com.meiya.whalex.util.WildcardRegularConversion;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * HBase服务实现
 *
 * @author 黄河森
 * @date 2019/12/16
 * @project whale-cloud-platformX
 */
@Slf4j
public class BaseHBaseServiceImpl<S extends HBaseTemplate, Q extends HBaseHandler, D extends HBaseDatabaseInfo, T extends HBaseTableInfo, C extends HBaseCursorCache>
        extends AbstractDbModuleBaseService<S, Q, D, T, C> {

    @Override
    protected QueryMethodResult queryMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        List<Map<String, Object>> resultList = new ArrayList<>();
        HBaseHandler.HBaseQuery entityQuery = queryEntity.getQuery();
        // 根据条件获取所有时间范围内的表名
        List<String> tableNames = DatatypePeriodUtils.getPeriodTableNames(tableConf.getTableName(), tableConf.getPeriodType(), entityQuery.getStartTime(), entityQuery.getStopTime(), DbResourceEnum.hbase, tableConf.getPeriodValue());
        List<Get> getList = new ArrayList<>(entityQuery.getGetList());
        String queryStr = transitionQueryStr(tableNames, entityQuery);
        queryEntity.setQueryStr(queryStr);
        DatabaseExecuteStatementLog.set(queryStr);
        for (int i = 0; i < tableNames.size(); i++) {
            String tableName = tableNames.get(i);
            if (log.isDebugEnabled()) {
                log.debug("当前查询 HBase TableName: [{}]", tableName);
            }
            // 携带 rowKey
            if (CollectionUtil.isNotEmpty(getList)) {
                Result[] results = connect.getResult(tableName, getList);
                if (results != null && results.length > 0) {
                    if (entityQuery.getMaxVersion() != null) {
                        for (Result result : results) {
                            // 若相同则移除 rowKey，避免反复查询
                            if (tableNames.size() > 1) {
                                byte[] resultRow = result.getRow();
                                String resultRowStr = Base64.encodeBase64String(resultRow);
                                Iterator<Get> iterator = getList.iterator();
                                while (iterator.hasNext()) {
                                    Get next = iterator.next();
                                    byte[] row = next.getRow();
                                    String rowStr = Base64.encodeBase64String(row);
                                    if (StringUtils.equalsIgnoreCase(rowStr, resultRowStr)) {
                                        iterator.remove();
                                        break;
                                    }
                                }
                            }
                            for (Cell cell : result.rawCells()) {
                                Map<String, Object> resultMap = new LinkedHashMap<>();
                                String resultQualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                                long timestamp = cell.getTimestamp();
                                byte[] resultValue = CellUtil.cloneValue(cell);
                                VersionResult versionVo = new VersionResult(resultValue, timestamp);
                                resultMap.put(resultQualifier, versionVo);
                                if (!resultMap.isEmpty()) {
                                    resultList.add(resultMap);
                                }
                            }
                        }
                    } else {
                        for (Result result : results) {
                            // 若相同则移除 rowKey，避免反复查询
                            if (tableNames.size() > 1) {
                                byte[] resultRow = result.getRow();
                                String resultRowStr = Base64.encodeBase64String(resultRow);
                                Iterator<Get> iterator = getList.iterator();
                                while (iterator.hasNext()) {
                                    Get next = iterator.next();
                                    byte[] row = next.getRow();
                                    String rowStr = Base64.encodeBase64String(row);
                                    if (StringUtils.equalsIgnoreCase(rowStr, resultRowStr)) {
                                        iterator.remove();
                                        break;
                                    }
                                }
                            }
                            Map<String, Object> resultMap = new LinkedHashMap<>();
                            for (Cell cell : result.rawCells()) {
                                String resultQualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                                byte[] resultValue = CellUtil.cloneValue(cell);
                                resultMap.put(resultQualifier, resultValue);
                            }
                            if (!resultMap.isEmpty()) {
                                if (entityQuery.isReturnRowKey()) {
                                    resultMap.put("base64RowKey", result.getRow());
                                }
                                resultList.add(resultMap);
                            }
                        }
                    }
                    if (CollectionUtil.isEmpty(getList)) {
                        break;
                    }
                }
            } else {
                // 未携带 rowKey 使用 Scan
                //存在scan,直接使用,不存在时,实例化一个新的
                Scan scan1 = entityQuery.getScan();
                if(scan1 == null) {
                    scan1 =  new Scan();
                }
                final Scan scan =scan1;
                int skip = entityQuery.getSkip();
                int pageSize = entityQuery.getPageSize();
                if (pageSize == 0) {
                    pageSize = 15;
                }

                if (pageSize < 100) {
                    scan.setCaching(pageSize);
                }

                if (log.isDebugEnabled()) {
                    log.debug("tableName: [{}], zookeeperAddr: [{}] 通过SCAN获取数据, skip: [{}], pageSize: [{}]", tableName, databaseConf.getZookeeperAddr(), skip, pageSize);
                }

                FilterList filterList = new FilterList(new PageFilter(pageSize));

                //存在filter时,直接使用
                Filter filter = scan.getFilter();
                if(filter != null) {
                    filterList.addFilter(filter);
                }else{
                    //条件不能为空
                    filter = new RowFilter(CompareFilter.CompareOp.GREATER, new BinaryComparator(null));
                    filterList.addFilter(filter);
                }

                scan.setFilter(filterList);
                final ResultCallback<Map<String, Object>> resultAction = result -> {
                    Map<String, Object> resultMap = new LinkedHashMap<>();
                    for (Cell cell : result.rawCells()) {
                        String resultQualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                        byte[] resultValue = CellUtil.cloneValue(cell);
                        resultMap.put(resultQualifier, resultValue);
                    }
                    if (MapUtils.isNotEmpty(resultMap)) {
                        resultMap.put("base64RowKey", result.getRow());
                    }
                    return resultMap;
                };

                final int finalPageSize = pageSize;
                final int finalSkip = skip;
                final TableTemplateCallback<List<Map<String, Object>>> queryAction = htable -> {
                    ResultScanner scanner = null;
                    Integer count = 0;
                    List<Map<String, Object>> t = new ArrayList<Map<String, Object>>();
                    try {
                        scanner = htable.getScanner(scan);
                        Result result = scanner.next();
                        while (result != null) {
                            Map<String, Object> res = resultAction.doInRow(result);
                            if ((count >= finalSkip) && count < (finalSkip + finalPageSize)) {
                                t.add(res);
                            }
                            count++;
                            if (count >= (finalSkip + finalPageSize)) {
                                break;
                            }
                            result = scanner.next();
                        }
                    } catch (IOException e) {
                        throw new BusinessException("Exception occured in executeWithScan method, tableName="
                                + tableName + "\n scan=" + scan + "\n",
                                e);
                    } catch (BusinessException e) {
                        throw e;
                    } finally {
                        if (scanner != null) {
                            scanner.close();
                        }
                    }
                    return t;
                };

                long start = System.currentTimeMillis();
                List<Map<String, Object>> list = connect.execute(tableName, queryAction);
                if (log.isDebugEnabled()) {
                    log.debug("tableName: [{}], zookeeperAddr: [{}] 通过SCAN获取数据, skip: [{}], pageSize: [{}] cost: [{}]", tableName, databaseConf.getZookeeperAddr(), skip, pageSize, System.currentTimeMillis() - start);
                }
                resultList.addAll(list);
                // 查询满足当前分页数即可结束
                if (pageSize <= resultList.size()) {
                    break;
                }
            }
        }
        return new QueryMethodResult(0, resultList);
    }

    @Override
    protected QueryMethodResult countMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        HBaseHandler.HBaseQuery hBaseQuery = queryEntity.getQuery();
        // 获取分表表名
        List<String> periodTableNames = DatatypePeriodUtils.getPeriodTableNames(tableConf.getTableName(), tableConf.getPeriodType(), hBaseQuery.getStartTime(), hBaseQuery.getStopTime(), DbResourceEnum.hbase, tableConf.getPeriodValue());
        String queryStr = transitionQueryStr(periodTableNames, hBaseQuery);
        queryEntity.setQueryStr(queryStr);
        DatabaseExecuteStatementLog.set(queryStr);
        long total = 0;
        Admin admin = connect.getHTablePool().getAdmin();
        for (int i = 0; i < periodTableNames.size(); i++) {
            String tableName = periodTableNames.get(i);
            tableName = connect.getNameSpaceTableName(tableName);
            TableName tableNameForHBase = TableName.valueOf(tableName);
            TableDescriptor descriptor = admin.getDescriptor(tableNameForHBase);
            String coprocessorClass = "org.apache.hadoop.hbase.coprocessor.AggregateImplementation";
            if (!descriptor.hasCoprocessor(coprocessorClass)) {
                if (descriptor.isReadOnly()) {
                    throw new BusinessException("HBase HTableDescriptor 对象为只读状态，无法添加斜处理器!");
                }
                try {
                    admin.disableTable(tableNameForHBase);
                    ((TableDescriptorBuilder.ModifyableTableDescriptor) descriptor).setCoprocessor(coprocessorClass);
                    admin.modifyTable(descriptor);
                }catch (Exception e){
                    throw new BusinessException("HBase 设置斜处理器失败: [" + coprocessorClass + "]!");
                } finally {
                    if (admin.isTableDisabled(tableNameForHBase)) {
                        admin.enableTable(tableNameForHBase);
                    }
                }
            }
            StopWatch stopWatch = new StopWatch();
            AggregationClient aggregationClient = null;
            try {
                stopWatch.start();
                Scan scan = new Scan();
                aggregationClient = new AggregationClient(connect.getHTablePool().getConfiguration());
                long tableNum = aggregationClient.rowCount(tableNameForHBase, new LongColumnInterpreter(), scan);
                total += tableNum;
            } catch (Throwable throwable) {
                throw new BusinessException(throwable);
            } finally {
                if(aggregationClient != null) aggregationClient.close();
                stopWatch.stop();
            }
        }
        return new QueryMethodResult(total, null);
    }

    @Override
    protected QueryMethodResult testConnectMethod(S connect, D databaseConf) throws Exception {
        Admin admin = connect.getHTablePool().getAdmin();
        DatabaseExecuteStatementLog.set("list");
        admin.listTableNames();
        return new QueryMethodResult();
    }

    @Override
    protected QueryMethodResult showTablesMethod(S connect, Q queryEntity, D databaseConf) throws Exception {

        String namespace = databaseConf.getNamespace();

        if(namespace == null) {
            namespace = connect.getUserName();
        }

        if(namespace == null) {
            namespace = "default";
        }

        DatabaseExecuteStatementLog.set("list");
        List<String> listTable = connect.listTable(namespace);

        List<Map<String, Object>> resultList = new ArrayList<>();
        for (String tableName : listTable) {
            Map<String, Object> valueMap = new HashMap<>(1);
            valueMap.put("tableName", tableName);
            resultList.add(valueMap);
        }
        if (queryEntity != null && queryEntity.getListTable() != null) {
            resultList = WildcardRegularConversion.matchFilter(queryEntity.getListTable().getTableMatch(), resultList);
        }
        return new QueryMethodResult(resultList.size(), resultList);
    }

    @Override
    protected QueryMethodResult getIndexesMethod(S connect, D databaseConf, T tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "hBaseServiceImpl.getIndexesMethod");
    }

    @Override
    protected QueryMethodResult createTableMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        if (StringUtils.isBlank(tableConf.getPeriodType())) {
            throw new BusinessException(ExceptionCode.DATE_TYPE_NO_SET_EXCEPTION);
        }
        // 分区数量，默认 10
        Integer regionCount = tableConf.getRegionCount();
        if (regionCount == null) {
            regionCount = 10;
        }
        String schema = tableConf.getSchema();
        List<Map<String, String>> schemaMap;
        if (StringUtils.isBlank(schema)) {
            List<Map<String, String>> schemaTemPlate = queryEntity.getCreateTable().getSchemaTemPlate();
            if (CollectionUtil.isNotEmpty(schemaTemPlate)) {
                schemaMap = schemaTemPlate;
            } else {
                log.warn("HBase [{}] schema is null, use default schema!", tableConf.getTableName());
                schema = BaseHBaseConfigHelper.DEFAULT_BC_FAMILY_SCHEME;
                schemaMap = JsonUtil.jsonStrToObject(schema, List.class);
            }
        } else {
            schemaMap = JsonUtil.jsonStrToObject(schema, List.class);
        }
        Map<String, String> regionsMap = null;
        for (Map<String, String> tableDesMap : schemaMap) {
            if (tableDesMap.get("SPLITALGO") != null) {
                regionsMap = tableDesMap;
                break;
            }
        }
        // 设置默认分区算法
        if (regionsMap == null) {
            regionsMap = new HashMap<>(2);
            schemaMap.add(regionsMap);
            regionsMap.put("SPLITALGO", "HexStringSplit");
        }
        // 设置分区个数
        regionsMap.put("NUMREGIONS", String.valueOf(regionCount));
        // 执行建表操作
        // 获取周期内的所有表名
        HBaseHandler.HBaseCreateTable createTable = queryEntity.getCreateTable();
        List<String> tableNameList;
        // 判断是否有指定建表区间
        if (createTable.getStartTime() == null || createTable.getStopTime() == null) {
            tableNameList = DatatypePeriodUtils.getPeriodTableNameList(tableConf.getTableName()
                    , tableConf.getPeriodType()
                    , tableConf.getPrePeriodValue()
                    , tableConf.getFuturePeriodValue()
                    , tableConf.getPeriodValue());
        } else {
            tableNameList = DatatypePeriodUtils.getPeriodTableNameList(tableConf.getTableName()
                    , tableConf.getPeriodType()
                    , createTable.getStartTime()
                    , createTable.getStopTime());
        }
        if (log.isDebugEnabled()) {
            log.debug("HBase create tables: [{}]", tableNameList);
        }
        List<String> failTable = new ArrayList<>();
        for (int i = 0; i < tableNameList.size(); i++) {
            String tableName = tableNameList.get(i);
            // 判断表示是否已经存在
            DatabaseExecuteStatementLog.set("exists '" + connect.getNameSpaceTableName(tableName) + "'");
            if (connect.tableExists(tableName)) {
                log.error("HBase table: [{}] exists!", tableName);
                continue;
            }
            // 执行建表任务
            if (log.isDebugEnabled()) {
                log.debug("begin HBase create table: [{}]", tableName);
            }
            try {
                DatabaseExecuteStatementLog.set(buildCreateTableStatement(connect.getNameSpaceTableName(tableName), schemaMap));
                connect.createTable(tableName, schemaMap, queryEntity.getCreateTable().getRowKeyPartition());
            } catch (Exception e) {
                log.error("HBase create table fail! core: [{}]", tableName, e);
                failTable.add(tableName);
            }
            if (log.isDebugEnabled()) {
                log.debug("end HBase create table: [{}]", tableName);
            }
        }
        // 如果存在创建失败的表，则抛出异常
        if (CollectionUtil.isNotEmpty(failTable)) {
            throw new BusinessException(ExceptionCode.CREATE_TABLE_EXCEPTION, failTable.toString());
        }
        return new QueryMethodResult(tableNameList.size(), null);
    }

    private String buildCreateTableStatement(String tableName, List<Map<String, String>> tableDesMapList) {
        StringBuilder tableBuilder = new StringBuilder();
        StringBuilder familyBuilder = new StringBuilder();
//        StringBuilder splitalgoBuilder = new StringBuilder();
        for (Map<String, String> tableDesMap : tableDesMapList) {
            if (tableDesMap.get("NAME") != null) {
                if(tableDesMap.size() == 1) {
                    familyBuilder.append(", ").append("'").append(tableDesMap.get("NAME")).append("'");
                    continue;
                }
                familyBuilder.append(", ").append("{NAME => ").append("'").append(tableDesMap.get("NAME")).append("'");
                for (Map.Entry<String, String> kv : tableDesMap.entrySet()) {
                    if (kv.getKey().equals("NAME")) continue;
                    familyBuilder.append(", ").append(kv.getKey()).append(" => ").append("'").append(kv.getValue()).append("'");
                }
                familyBuilder.append("}");
            }
//            if (tableDesMap.get("SPLITALGO") != null) {
//                splitalgoBuilder.append(", ").append("{SPLITS => [");
//                String splitalgo = tableDesMap.get("SPLITALGO");
//                int regionCount = Integer.parseInt(tableDesMap.get("NUMREGIONS"));
//                for (int i = 0; i < regionCount; i++) {
//                    if(i != 0) {
//                        splitalgoBuilder.append(", ");
//                    }
//                    splitalgoBuilder.append("'").append(splitalgo).append(i + 1).append("'");
//                }
//                splitalgoBuilder.append("]}");
//            }
        }
        return tableBuilder.append("create ").append("'").append(tableName).append("'")
                .append(familyBuilder.toString())
//                .append(splitalgoBuilder.toString())
                .toString();
    }

    @Override
    protected QueryMethodResult dropTableMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        HBaseHandler.HBaseDropTable dropTable = queryEntity.getDropTable();
        List<String> tableNameList;
        String periodType = tableConf.getPeriodType();
        if (StringUtils.equalsIgnoreCase(periodType, DatatypePeriodConstants.PERIOD_TYPE_ONLY_ONE)) {
            tableNameList = CollectionUtil.newArrayList(tableConf.getTableName());
        } else {
            // 判断是否有指定建表区间
            if (dropTable.getStartTime() == null || dropTable.getStopTime() == null) {
                String firstPeriodTableName = DatatypePeriodUtils.getFirstPeriodTableName(tableConf.getTableName()
                        , tableConf.getPeriodType()
                        , tableConf.getPeriodValue());
                QueryMethodResult queryMethodResult = showTablesMethod(connect, null, databaseConf);
                List<Map<String, Object>> rows = queryMethodResult.getRows();
                if (CollectionUtil.isEmpty(rows)) {
                    log.error("HBase drop table: [{}] fail, database: [{}] not found table!", tableConf.getTableName(), databaseConf.getZookeeperAddr());
                    throw new BusinessException(ExceptionCode.DROP_TABLE_MISS_EXCEPTION, null);
                } else {
                    tableNameList = rows.stream()
                            .filter((tableNameMap) -> {
                                String tableName = (String) tableNameMap.get("tableName");
                                tableName = tableName.replaceAll("\\w+:", "");
                                return StringUtils.startsWithIgnoreCase(tableName, tableConf.getTableName())
                                        && DatatypePeriodUtils.getPattern(tableConf.getTableName(), tableConf.getPeriodType()).matcher(tableName).find()
                                        && tableName.compareTo(firstPeriodTableName) < 0;
                            })
                            .map((map -> ((String) map.get("tableName")).replaceAll("\\w+:", ""))).collect(Collectors.toList());
                    if (CollectionUtil.isEmpty(tableNameList)) {
                        log.error("HBase drop table: [{}] fail, database: [{}] not found table!", tableConf.getTableName(), databaseConf.getZookeeperAddr());
                        throw new BusinessException(ExceptionCode.DROP_TABLE_MISS_EXCEPTION, null);
                    }
                }
            } else {
                tableNameList = DatatypePeriodUtils.getPeriodTableNameList(tableConf.getTableName()
                        , tableConf.getPeriodType()
                        , dropTable.getStartTime()
                        , dropTable.getStopTime());
            }
        }

        List<String> failTable = new ArrayList<>();
        tableNameList.forEach(tableName -> {
            try {
                String nameSpaceTableName = connect.getNameSpaceTableName(tableName);
                DatabaseExecuteStatementLog.set("exists '" + nameSpaceTableName + "'");
                if (connect.tableExists(tableName)) {
                    if (log.isDebugEnabled()) {
                        log.debug("HBase drop table: [{}]", tableName);
                    }
                    DatabaseExecuteStatementLog.set("disable '" + nameSpaceTableName + "'");
                    DatabaseExecuteStatementLog.set("drop '" + nameSpaceTableName + "'");
                    connect.disableAndDropTable(tableName);
                } else {
                    log.error("HBase drop table: [{}] not found!", tableName);
                }
            } catch (IOException e) {
                log.error("HBase drop table: [{}] fail!", tableName, e);
                failTable.add(tableName);
            }
        });

        if (CollectionUtil.isNotEmpty(failTable)) {
            throw new BusinessException(ExceptionCode.DROP_TABLE_EXCEPTION, failTable.toString());
        }
        return new QueryMethodResult(tableNameList.size(), null);
    }

    @Override
    protected QueryMethodResult deleteIndexMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "hBaseServiceImpl.deleteIndexMethod");
    }

    @Override
    protected QueryMethodResult createIndexMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "hBaseServiceImpl.createIndexMethod");
    }

    @Override
    protected QueryMethodResult insertMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        HBaseHandler.HBaseInsert insert = queryEntity.getInsert();
        List<Put> putList = insert.getPutList();
        Long captureTime = insert.getCaptureTime();
        List<Map<String, Object>> fieldValueList = insert.getFieldValueList();
        String periodType = tableConf.getPeriodType();
        String tableName = tableConf.getTableName();
        if (!DatatypePeriodConstants.PERIOD_TYPE_ONLY_ONE.equals(periodType) && captureTime == null) {
            throw new BusinessException(ExceptionCode.DATE_TYPE_TABLE_TIME_NULL_EXCEPTION, "HBaseServiceImpl.insertMethod");
        } else {
            tableName = DatatypePeriodUtils.getPeriodTableName(tableName, tableConf.getPeriodType(), captureTime);
        }
        String statement = buildInsertStatement(connect.getNameSpaceTableName(tableName), fieldValueList);
        DatabaseExecuteStatementLog.set(statement);
        connect.batchPut(tableName, putList);
        return new QueryMethodResult(putList.size(), null);
    }

    private String buildInsertStatement(String tableName, List<Map<String, Object>> fieldValueList) {

        StringBuilder builder = new StringBuilder();

        for (Map<String, Object> fieldMap : fieldValueList) {
            Object rowKey = fieldMap.get(CommonConstant.ROW_KEY);
            if(rowKey instanceof byte[]) {
                rowKey = new String((byte[]) rowKey);
            }

            for (Map.Entry<String, Object> entry : fieldMap.entrySet()) {
                String family = entry.getKey();
                Object value = entry.getValue();
                if (!family.equalsIgnoreCase(CommonConstant.ROW_KEY) && value instanceof Map) {
                    Map<String, Object> qualifierMap = (Map<String, Object>) value;
                    for (Map.Entry<String, Object> qualifierEntry : qualifierMap.entrySet()) {
                        Object qualifierEntryValue = qualifierEntry.getValue();
                        if (qualifierEntryValue instanceof byte[]) {
                            qualifierEntryValue = new String((byte[]) qualifierEntryValue);
                        }
                        if(builder.length() > 0) {
                            builder.append("\n");
                        }
                        //put 'tableName', 'row_key', 'column_family:column_qualifier', 'value'
                        builder.append("put ").append("'").append(tableName).append("', ")
                                .append("'").append(rowKey).append("'").append(", ")
                                .append("'").append(family).append(":").append(qualifierEntry.getKey()).append("'").append(", ")
                                .append("'").append(qualifierEntryValue).append("'");
                    }
                }
            }

        }

        return builder.toString();
    }

    @Override
    protected QueryMethodResult updateMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        HBaseHandler.HBaseUpdate update = queryEntity.getUpdate();
        List<Put> putList = update.getPutList();
        List<Map<String, Object>> fieldValueList = update.getFieldValueList();

        List<String> tableNames = DatatypePeriodUtils.getPeriodTableNames(tableConf.getTableName(), tableConf.getPeriodType(), update.getStartTime(), update.getStopTime(), DbResourceEnum.hbase, tableConf.getPeriodValue());

        for (String tableName : tableNames) {
            DatabaseExecuteStatementLog.set(buildInsertStatement(connect.getNameSpaceTableName(tableName), fieldValueList));
            connect.batchPut(tableName, putList);
        }

        return new QueryMethodResult(putList.size(), null);
    }

    @Override
    protected QueryMethodResult delMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        HBaseHandler.HBaseDel delete = queryEntity.getDelete();
        List<Delete> deleteList = delete.getDeleteList();
        List<String> deleteStrList = delete.getDeleteStrList();
        List<String> tableNames = DatatypePeriodUtils.getPeriodTableNames(tableConf.getTableName(), tableConf.getPeriodType(), delete.getStartTime(), delete.getStopTime(), DbResourceEnum.hbase, tableConf.getPeriodValue());

        for (String tableName : tableNames) {
            buildDeleteStatement(connect.getNameSpaceTableName(tableName), deleteStrList);
            connect.batchDelete(tableName, deleteList);
        }

        return new QueryMethodResult(deleteList.size(), null);
    }

    private void buildDeleteStatement(String tableName, List<String> deleteStrList) {
        for (String rowKey : deleteStrList) {
            DatabaseExecuteStatementLog.set("delete '" + tableName + "', '" + rowKey + "'");
        }
    }

    @Override
    protected QueryMethodResult monitorStatusMethod(S connect, D databaseConf) throws Exception {
        ClusterStatus clusterStatus = connect.getHTablePool().getAdmin().getClusterStatus();
        Map<String, Object> map = JsonUtil.entityToMap(clusterStatus);
        List<Map<String, Object>> list = new ArrayList<>();
        list.add(map);
        return new QueryMethodResult(1, list);
    }

    @Override
    protected QueryMethodResult querySchemaMethod(S connect, D databaseConf, T tableConf) throws Exception {
        List<String> tableNames = DatatypePeriodUtils.getPeriodTableNames(tableConf.getTableName(), tableConf.getPeriodType(), null, null, DbResourceEnum.hbase, tableConf.getPeriodValue());
        for (String tableName : tableNames) {
            if (connect.tableExists(tableName)) {
                String nameSpaceTableName = connect.getNameSpaceTableName(tableConf.getTableName());
                if(!nameSpaceTableName.contains(":")) {
                    nameSpaceTableName  = "default:" + tableConf.getTableName();
                }
                DatabaseExecuteStatementLog.set("describe '" + nameSpaceTableName + "'");
                List<Map<String, Object>> schemaInfo = connect.getSchemaInfo(tableName);
                return new QueryMethodResult(schemaInfo.size(), schemaInfo);
            }
        }
        throw new BusinessException(ExceptionCode.NOT_FOUND_TABLE_EXCEPTION);
    }

    @Override
    protected QueryMethodResult queryDatabaseSchemaMethod(S connect, D databaseConf, T tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + ".queryDatabaseSchemaMethod");
    }

    @Override
    protected QueryCursorMethodResult queryCursorMethod(S connect, Q queryEntity, D databaseConf, T tableConf, C cursorCache, Consumer<Map<String, Object>> consumer, Boolean rollAllData) throws Exception {
        List<String> indexNameList;
        int actualTableStart = 0;
        long total = 0;
        AtomicLong row = new AtomicLong(0);
        HBaseHandler.HBaseQuery query = queryEntity.getQuery();
        if(query.getBatchSize() == null){
            throw new BusinessException("滚动查询批次数量BatchSize为空");
        }
        Integer remainSize = query.getBatchSize();
        if (cursorCache == null) {
            // 未匹配到游标缓存
            // 查询所有hbase的表名 根据条件获取所有时间范围内的表名
            indexNameList = DatatypePeriodUtils.getPeriodTableNames(tableConf.getTableName(), tableConf.getPeriodType(), query.getStartTime(), query.getStopTime(), DbResourceEnum.hbase, tableConf.getPeriodValue());
        } else {
            // 匹配到缓存
            // 如果当前表的结果扫描的每个批次的第一个结果是空的 说明当前表已经被扫描一遍了 需要开启下一张表的扫描 或者继续扫描表剩下的数据，且游标被关闭，需要开启新的游标
            // 否则就继续往下扫描 如果当前表的扫描次数不够 小于批次 也需要再开启一张表的扫描
            if (cursorCache.getFirstResult() == null || cursorByResultScanner(consumer,row,cursorCache,query.getBatchSize(),rollAllData)) {
                indexNameList = cursorCache.getIndexNameList();
                actualTableStart = indexNameList.indexOf(cursorCache.getLastIndex()) + 1;
                if (actualTableStart == indexNameList.size()) {
                    // 已经结束所有表的扫描
                    return new QueryCursorMethodResult(null);
                }
            } else {
                // 够量, 结束本次查询
                return new QueryCursorMethodResult(cursorCache);
            }
        }

        for (int i = actualTableStart; i < indexNameList.size(); i++) {
            String indexName = indexNameList.get(i);
            Connection hTablePool = connect.getHTablePool();
            Table table = hTablePool.getTable(TableName.valueOf(indexName));

            /*
            数据量统计,hbase分表，且存储数据量大，该统计方式千万级别数据量大概统计时间为几十秒，统计功能暂时不考虑
            if (query.getCount()) {
                total += rowCountByCoprocessor(connect,tableConf.getTableName(),databaseConf.getZookeeperAddr());
            }*/

            // 游标生成
            Scan scan = new Scan();
            scan.setCaching(query.getBatchSize());
            Filter filter1 = new RowFilter(CompareFilter.CompareOp.GREATER, new BinaryComparator(null));
            FilterList filterList = new FilterList(new PageFilter(query.getBatchSize()), filter1);
            scan.setFilter(filterList);
            ResultScanner resultScanner = table.getScanner(scan);

            // 扫描
            // 滚动数据量
            int rollTotal = 0;
            // 遍历游标
            Result result = resultScanner.next();
            while (result != null) {
                Map<String, Object> resultMap = new LinkedHashMap<>();
                for (Cell cell : result.rawCells()) {
                    String resultQualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                    byte[] resultValue = CellUtil.cloneValue(cell);
                    resultMap.put(resultQualifier, resultValue);
                }
                if (!resultMap.isEmpty()) {
                    rollTotal++;
                    consumer.accept(resultMap);
                }
                // 非一次滚完所有数据，并且当前滚动数据量大于等于批量数即可返回
                if (!rollAllData && rollTotal >= remainSize) {
                    break;
                }
                result = resultScanner.next();
            }
            row.addAndGet(rollTotal);
            HBaseCursorCache hBaseCursorCache = new HBaseCursorCache(null, query.getBatchSize(), resultScanner, resultScanner.next(), indexNameList, indexName);

            // 当前表的扫描数据量小于规定的每批数量 需要关闭游标 继续下一张表的扫描
            if (rollTotal ==0 || rollTotal < remainSize) {
                // 关闭游标 并且修改下次的批次数量
                resultScanner.close();
                remainSize = remainSize - rollTotal;
                // 当前表遍历完了，但是还存在其他表未遍历
                if (row.get() >= query.getBatchSize() && !rollAllData) {
                    return new QueryCursorMethodResult(hBaseCursorCache);
                }
                continue;
            }
            if (row.get() >= query.getBatchSize() && !rollAllData) {
                return new QueryCursorMethodResult(hBaseCursorCache);
            }

            // 若 size 为 -1，则单次遍历完全部数据
            if (rollAllData) {
                // 循环拉取数据
                boolean done = true;
                while (done) {
                    done = !cursorByResultScanner(consumer,row,hBaseCursorCache,query.getBatchSize(),rollAllData);
                }
            }
        }
        return new QueryCursorMethodResult(total,null);
    }

    /**
     * 继续往下扫描获取数据滚到底
     * @param consumer
     * @param row
     * @return
     * @throws SQLException
     * @throws IOException
     */
    private Boolean cursorByResultScanner(Consumer<Map<String, Object>> consumer, AtomicLong row,HBaseCursorCache cursorCache,int batchSize,Boolean rollAllData) throws SQLException, IOException {
        try {
            ResultScanner resultScanner = cursorCache.getScanner();
            // 滚动数据量
            int rollTotal = 0;
            // 遍历游标
            Result result = cursorCache.getFirstResult();
            while (result != null) {
                Map<String, Object> resultMap = new LinkedHashMap<>();
                for (Cell cell : result.rawCells()) {
                    String resultQualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                    byte[] resultValue = CellUtil.cloneValue(cell);
                    resultMap.put(resultQualifier, resultValue);
                }
                if (!resultMap.isEmpty()) {
                    rollTotal++;
                    consumer.accept(resultMap);
                }
                // 当前滚动数据量大于等于批量数即可返回
                if (rollTotal >= batchSize) {
                    break;
                }
                result = resultScanner.next();
            }
            row.addAndGet(rollTotal);
            // 判断当前表和批次是否被滚完 被滚完则开启下一张表
            if (rollTotal ==0 || rollTotal < batchSize) {
                // 关闭游标 并且修改下次的批次数量
                resultScanner.close();
                return true;
            }
        } catch (Exception e) {
            log.error("hbase query fail, table: [{}]",cursorCache.getLastIndex(),e);
        }
        return false;
    }

    @Override
    protected QueryMethodResult saveOrUpdateMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "HBaseServiceImpl.saveOrUpdateMethod");
    }

    @Override
    protected QueryMethodResult saveOrUpdateBatchMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "HBaseServiceImpl.saveOrUpdateBatchMethod");
    }

    @Override
    protected QueryMethodResult alterTableMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {

        HBaseHandler.HBaseAlterTable alterTable = queryEntity.getAlterTable();
        List<HColumnDescriptor> addColumns = alterTable.getAddColumns();
        List<byte[]> delColumns = alterTable.getDelColumns();

        Admin admin = connect.getHTablePool().getAdmin();
        String tableName = connect.getNameSpaceTableName(tableConf.getTableName());
        TableName tableNameForHBase = TableName.valueOf(tableName);
        try {
            // 先禁用表
            admin.disableTable(tableNameForHBase);

            // 添加列簇
            if (CollectionUtil.isNotEmpty(addColumns)) {
                for (HColumnDescriptor hColumnDescriptor : addColumns) {
                    admin.addColumnFamily(tableNameForHBase, hColumnDescriptor);
                }
            }

            // 删除列簇
            if (CollectionUtil.isNotEmpty(delColumns)) {
                for (byte[] delColumn : delColumns) {
                    admin.deleteColumnFamily(tableNameForHBase, delColumn);
                }
            }
        } finally {
            if (admin.isTableDisabled(tableNameForHBase)) {
                admin.enableTable(tableNameForHBase);
            }
        }
        return new QueryMethodResult();
    }

    @Override
    protected QueryMethodResult tableExistsMethod(S connect, D databaseConf, T tableConf) throws Exception {
        DatabaseExecuteStatementLog.set("exists '" + connect.getNameSpaceTableName(tableConf.getTableName()) + "'");
        boolean exists = connect.tableExists(tableConf.getTableName());
        Map<String, Object> existsMap = new HashMap<>(1);
        existsMap.put(tableConf.getTableName(), exists);
        return new QueryMethodResult(1, CollectionUtil.newArrayList(existsMap));
    }

    @Override
    protected QueryMethodResult queryTableInformationMethod(S connect, D databaseConf, T tableConf) throws Exception {
        Admin admin = connect.getHTablePool().getAdmin();
        // 获取表信息
        String nameSpaceTableName = connect.getNameSpaceTableName(tableConf.getTableName());
        TableDescriptor descriptor = admin.getDescriptor(TableName.valueOf(nameSpaceTableName));
        int regionReplication = descriptor.getRegionReplication();
        // 获取表region信息
        List<RegionInfo> tableRegions = admin.getRegions(TableName.valueOf(tableConf.getTableName()));
        Map<String, HBaseTableInformation.HBaseRegionInfo> regionInfoMap = new HashMap<>(tableRegions.size());
        for (int i = 0; i < tableRegions.size(); i++) {
            RegionInfo regionInfo = tableRegions.get(i);
            String encodedName = regionInfo.getEncodedName();
            String endKey = Base64.encodeBase64String(regionInfo.getEndKey());
            String startKey = Base64.encodeBase64String(regionInfo.getStartKey());
            long regionId = regionInfo.getRegionId();
            String regionNameAsString = regionInfo.getRegionNameAsString();
            int replicaId = regionInfo.getReplicaId();
            regionInfoMap.put(regionNameAsString, HBaseTableInformation.HBaseRegionInfo.builder().encodedName(encodedName).endKey(endKey).regionId(regionId).regionName(regionNameAsString).replicaId(replicaId).startKey(startKey).build());
        }
        HBaseTableInformation hBaseTableInformation = new HBaseTableInformation(tableConf.getTableName(), tableRegions.size(), regionReplication, "rowKey", null, MapBuilder.create(new HashMap<String, Map<String, HBaseTableInformation.HBaseRegionInfo>>(1)).put("regions", regionInfoMap).build());
        return new QueryMethodResult(1, CollectionUtil.newArrayList(hBaseTableInformation.toMap()));
    }

    @Override
    protected QueryMethodResult queryListDatabaseMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        DatabaseExecuteStatementLog.set("list_namespace");
        List<String> listDatabase = connect.getNameSpace();

        List<Map<String, Object>> resultList = new ArrayList<>();
        for (String tableName : listDatabase) {
            Map<String, Object> valueMap = new HashMap<>(1);
            valueMap.put("databaseName", tableName);
            resultList.add(valueMap);
        }

        String wildcardStr = null;
        HBaseHandler.HBaseListDatabase hBaseListDatabase = queryEntity.getListDatabase();
        if(hBaseListDatabase != null) {
            wildcardStr = hBaseListDatabase.getDatabaseMatch();
        }

        resultList = WildcardRegularConversion.matchFilterDatabaseName(wildcardStr, resultList);
        return new QueryMethodResult(resultList.size(), resultList);

    }

    @Override
    protected QueryMethodResult queryDatabaseInformationMethod(S connect, Q queryEntity, D databaseConf, T tableConf) {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + ".queryDatabaseInformationMethod");
    }

    /**
     * 查询语句（用于日志输出）
     *
     * @param operation
     * @param tableNameList
     * @param hBaseQuery
     * @return
     */
    private String transitionQueryStr(List<String> tableNameList, HBaseHandler.HBaseQuery hBaseQuery) {
        try {
            List<Get> getList = hBaseQuery.getGetList();
            if (CollectionUtil.isNotEmpty(getList)) {
                List<String> queryList = new ArrayList<>();
                for (int i = 0; i < tableNameList.size(); i++) {
                    StringBuilder querySb = new StringBuilder();
                    String tableName = tableNameList.get(i);
                    querySb.append("GET")
                            .append(" ")
                            .append("'")
                            .append(tableName)
                            .append("',");
                    for (int t = 0; t < getList.size(); t++) {
                        StringBuilder rowKeySb = new StringBuilder(querySb);
                        Get get = getList.get(t);
                        String rowKey = Base64.encodeBase64String(get.getRow());
                        rowKeySb.append("'").append(rowKey).append("'");
                        queryList.add(rowKeySb.toString());
                        rowKeySb = null;
                    }
                    querySb = null;
                }
                return queryList.toString();
            } else {
                StringBuilder scanSb = new StringBuilder();
                scanSb.append("SCAN").append(" ").append("'").append(tableNameList).append("'");
                return scanSb.toString();
            }
        } catch (Exception e) {
            log.error("HBase query string transition fail!", e);
            return null;
        }
    }
}
