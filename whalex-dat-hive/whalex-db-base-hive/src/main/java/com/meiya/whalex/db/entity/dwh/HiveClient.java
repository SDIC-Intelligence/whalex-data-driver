package com.meiya.whalex.db.entity.dwh;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

@Slf4j
@Data
public class HiveClient extends QueryRunner {

    private Map<String, HiveMetaStoreClient> metaStoreClientMap = new ConcurrentHashMap<>();

    public HiveClient(DataSource ds) {
        super(ds);
    }

    private ExecutorService executorService;

    private HiveConf hiveConf;

    public boolean metaStoreIsAvailable() {
        return executorService != null && hiveConf != null;
    }

    private HiveMetaStoreClient getMetaStoreClient() throws MetaException {
        String name = Thread.currentThread().getName();
        HiveMetaStoreClient hiveMetaStoreClient = metaStoreClientMap.get(name);
        if (hiveMetaStoreClient == null) {
            log.info("初始化线程[{}]中的元数据服务客户端", name);
            hiveMetaStoreClient = new HiveMetaStoreClient(hiveConf);
            metaStoreClientMap.put(name, hiveMetaStoreClient);
        }
        return hiveMetaStoreClient;
    }

    public List<Map<String, Object>> listTable(String dbName) throws ExecutionException, InterruptedException {
        if(!metaStoreIsAvailable()) {
            throw new RuntimeException("元数据服务未配置地址");
        }
        Future<List<String>> future = executorService.submit(() -> {
            try {
                HiveMetaStoreClient hiveMetaStoreClient = getMetaStoreClient();
                return hiveMetaStoreClient.getAllTables(dbName);
            }catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        List<String> tableList = future.get();
        if(tableList == null) return new ArrayList<>();
        List<Map<String, Object>> rows = tableList.stream()
                .map(item -> {
                    Map<String, Object> row = new HashMap<>();
                    row.put("tableName", item);
                    return row;
                })
                .collect(Collectors.toList());
        return rows;
    }

    public List<Map<String, Object>> listDatabase() throws ExecutionException, InterruptedException {
        if(!metaStoreIsAvailable()) {
            throw new RuntimeException("元数据服务未配置地址");
        }
        Future<List<String>> future = executorService.submit(() -> {
            try {
                HiveMetaStoreClient hiveMetaStoreClient = getMetaStoreClient();
                return hiveMetaStoreClient.getAllDatabases();
            }catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        List<String> databaseList = future.get();
        if(databaseList == null) return new ArrayList<>();
        List<Map<String, Object>> rows = databaseList.stream()
                .map(item -> {
                    Map<String, Object> row = new HashMap<>();
                    row.put("Database", item);
                    return row;
                })
                .collect(Collectors.toList());
        return rows;
    }


    public List<Map<String, Object>> getTable(String dbName, String tableName) throws ExecutionException, InterruptedException {
        if(!metaStoreIsAvailable()) {
            throw new RuntimeException("元数据服务未配置地址");
        }
        Future<List<Map<String, Object>>> future = executorService.submit(() -> {
            try {
                HiveMetaStoreClient hiveMetaStoreClient = getMetaStoreClient();
                Table table = hiveMetaStoreClient.getTable(dbName, tableName);
                StorageDescriptor sd = table.getSd();
                List<FieldSchema> cols = sd.getCols();
                List<FieldSchema> partitionKeys = table.getPartitionKeys();
                cols.addAll(partitionKeys);
                return buildSchemaInfoList(cols);
            }catch (NoSuchObjectException e) {
                return new ArrayList<>();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        return future.get();
    }



    private List<Map<String, Object>> buildSchemaInfoList(List<FieldSchema> cols) {
        List<Map<String, Object>> schemaInfoList = new ArrayList<>();
        for (FieldSchema col : cols) {
            Map<String, Object> map = new HashMap<>();
            map.put("col_name", col.getName());
            map.put("autoincrement", 0);
            map.put("isKey", 0);
            map.put("comment", col.getComment());
            String columnType = col.getType().toLowerCase();
            map.put("data_type", columnType);
            map.put("columnSize", "");
            map.put("decimalDigits", "");
            map.put("std_data_type", HiveFieldTypeEnum.dbFieldType2FieldType(columnType,null).getVal());
            map.put("nullable", 0);
            map.put("columnDef", "");
            schemaInfoList.add(map);
        }
        return schemaInfoList;
    }

}
