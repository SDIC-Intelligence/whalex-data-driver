package com.meiya.whalex.db.module.document;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.map.MapBuilder;
import cn.hutool.core.map.MapUtil;
import com.meiya.whalex.annotation.Support;
import com.meiya.whalex.db.constant.IsolationLevel;
import com.meiya.whalex.db.constant.SupportPower;
import com.meiya.whalex.db.entity.DatabaseSetting;
import com.meiya.whalex.db.entity.QueryCursorMethodResult;
import com.meiya.whalex.db.entity.QueryMethodResult;
import com.meiya.whalex.db.entity.document.*;
import com.meiya.whalex.db.entity.table.infomaration.MongoDbTableInformation;
import com.meiya.whalex.db.entity.table.infomaration.TableInformation;
import com.meiya.whalex.db.module.AbstractDbModuleBaseService;
import com.meiya.whalex.db.module.DatabaseExecuteStatementLog;
import com.meiya.whalex.db.module.DbTransactionModuleService;
import com.meiya.whalex.db.module.NoTransactionModuleService;
import com.meiya.whalex.exception.BusinessException;
import com.meiya.whalex.exception.ExceptionCode;
import com.meiya.whalex.interior.db.search.condition.Sort;
import com.meiya.whalex.util.AggResultTranslateUtil;
import com.meiya.whalex.util.JsonUtil;
import com.meiya.whalex.util.WildcardRegularConversion;
import com.mongodb.BasicDBObject;
import com.mongodb.WriteConcern;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.*;
import com.mongodb.client.model.CountOptions;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.UpdateManyModel;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.connection.ClusterType;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * MONGODB 组件服务
 *
 * @author 黄河森
 * @date 2019/9/13
 * @project whale-cloud-platformX
 */
@Slf4j
@Support(value = {
        SupportPower.TEST_CONNECTION,
        SupportPower.CREATE,
        SupportPower.UPDATE,
        SupportPower.DELETE,
        SupportPower.SEARCH,
        SupportPower.SHOW_SCHEMA,
        SupportPower.CREATE_TABLE,
        SupportPower.DROP_TABLE,
        SupportPower.CREATE_INDEX,
        SupportPower.QUERY_INDEX,
        SupportPower.SHOW_TABLE_LIST,
        SupportPower.SHOW_DATABASE_LIST
})
public class BaseMongoServiceImpl extends AbstractDbModuleBaseService<MongoClient, MongoHandle, MongoDatabaseInfo, MongoTableInfo, MongoCursorCache> {

    @Override
    protected QueryMethodResult queryMethod(MongoClient mongoClient, MongoHandle mongoHandle, MongoDatabaseInfo databaseConf, MongoTableInfo tableConf) throws Exception {
        List<Map<String, Object>> result = new ArrayList<>();
        QueryMethodResult cursorMethodResult;
        MongoHandle.MongoQuery mongoQuery = mongoHandle.getMongoQuery();
        if (CollectionUtil.isNotEmpty(mongoQuery.getAggregates())) {
            return queryAggregate(mongoClient, mongoHandle, databaseConf, tableConf);
        } else {
            cursorMethodResult = queryCursorMethod(mongoClient, mongoHandle, databaseConf, tableConf, null, (rowMap) -> {
                result.add(rowMap);
            }, Boolean.TRUE);
        }
        QueryMethodResult queryMethodResult = new QueryMethodResult(cursorMethodResult.getTotal(), result);
        return queryMethodResult;
    }

    /**
     * 聚合查询
     *
     * @param mongoClient
     * @param mongoHandle
     * @param databaseConf
     * @param tableConf
     * @return
     */
    private QueryMethodResult queryAggregate(MongoClient mongoClient, MongoHandle mongoHandle, MongoDatabaseInfo databaseConf, MongoTableInfo tableConf) {
        MongoHandle.MongoQuery mongoQuery = mongoHandle.getMongoQuery();
        List<MongoHandle.MongoAggQuery> aggregates = mongoQuery.getAggregates();
        mongoHandle.setQueryStr(transitionAggQueryStr(tableConf.getTableName(), mongoQuery));
        // 设置mongo服务端的超时时间
        Integer timeOut = dbThreadBaseConfig.getTimeOut();
        MongoDatabase database = mongoClient.getDatabase(databaseConf.getDatabaseName());
        MongoCollection<Document> collection = database.getCollection(tableConf.getTableName());
        List<Map<String, Object>> aggResult = new ArrayList<>(16);
        for (int i = 0; i < aggregates.size(); i++) {
            MongoHandle.MongoAggQuery mongoAggQuery = aggregates.get(i);
            BasicDBObject query = mongoQuery.getQuery();
            List<BasicDBObject> aggObj = mongoAggQuery.getAggObj();
            int limit = mongoAggQuery.getLimit();
            AggregateIterable<Document> documents;
            // 判断是否设置读取超时时间
            if (mongoQuery.isLimitReadTime()) {
                if (query != null) {
                    BasicDBObject queryFilter = new BasicDBObject();
                    queryFilter.append("$match", query);
                    List<BasicDBObject> aggregateList = new ArrayList<>();
                    aggregateList.add(queryFilter);
                    aggregateList.addAll(aggObj);
                    documents = collection.aggregate(aggregateList).maxTime(timeOut, TimeUnit.SECONDS);
                } else {
                    documents = collection.aggregate(aggObj).maxTime(timeOut, TimeUnit.SECONDS);
                }
            } else {
                if (query != null) {
                    BasicDBObject queryFilter = new BasicDBObject();
                    queryFilter.append("$match", query);
                    List<BasicDBObject> aggregateList = new ArrayList<>();
                    aggregateList.add(queryFilter);
                    aggregateList.addAll(aggObj);
                    documents = collection.aggregate(aggregateList);
                } else {
                    documents = collection.aggregate(aggObj);
                }

            }

            // 若自定义批量数，则设置
            if (mongoQuery.getBatchSize() != null && mongoQuery.getBatchSize() > 0) {
                documents.batchSize(mongoQuery.getBatchSize());
            }

            MongoCursor<Document> cursor = null;
            try {
                cursor = documents.iterator();
                while (cursor.hasNext()) {
                    if (limit != -1 && aggResult.size() >= limit) {
                        break;
                    }
                    Document next = cursor.next();
                    aggResult.add(next);
                }
            } finally {
                if (cursor != null) {
                    cursor.close();
                }
            }
        }

        List<Map<String, Object>> result = AggResultTranslateUtil.getMongodbTreeAggResult(aggResult);
        return new QueryMethodResult(0, result);
    }

    @Override
    protected QueryMethodResult countMethod(MongoClient connect, MongoHandle mongoHandle, MongoDatabaseInfo databaseConf, MongoTableInfo tableConf) throws Exception {
        // 判断表是否存在
        try {
            QueryMethodResult queryMethodResult = tableExistsMethod(connect, databaseConf, tableConf);
            List<Map<String, Object>> rows = queryMethodResult.getRows();
            Map<String, Object> existsMap = rows.get(0);
            boolean exists = (boolean) existsMap.get(tableConf.getTableName());
            if (!exists) {
                throw new BusinessException(ExceptionCode.NOT_FOUND_TABLE_EXCEPTION, tableConf.getTableName());
            }
        } catch (Exception e) {
            if (e instanceof BusinessException
                    &&
                    ((BusinessException) e).getCode() == ExceptionCode.NOT_FOUND_TABLE_EXCEPTION.getCode()) {
                throw e;
            }
            log.error("Mongodb 统计接口判断目标表是否存在失败 dbId: [{}]!", tableConf.getTableName(), e);
        }
        MongoHandle.MongoQuery mongoQuery = mongoHandle.getMongoQuery();
        // 记录查询语句
        mongoHandle.setQueryStr(transitionQueryStr(tableConf.getTableName(), mongoHandle.getMongoQuery()));
        QueryMethodResult queryMethodResult = new QueryMethodResult();
        MongoDatabase database = connect.getDatabase(databaseConf.getDatabaseName());
        MongoCollection<Document> collection = database.getCollection(tableConf.getTableName());
        long count = collection.countDocuments(mongoQuery.getQuery());
        queryMethodResult.setTotal(count);
        return queryMethodResult;
    }

    @Override
    protected QueryMethodResult testConnectMethod(MongoClient connect, MongoDatabaseInfo databaseConf) throws Exception {
        MongoCursor<String> mongoCursor = null;
        try {
            MongoIterable<String> iterable = connect.listDatabaseNames();
            mongoCursor = iterable.iterator();
            boolean b = mongoCursor.hasNext();
            if (b) {
                mongoCursor.next();
            }
        } finally {
            if (mongoCursor != null) {
                mongoCursor.close();
            }
        }
        return new QueryMethodResult();
    }

    @Override
    protected QueryMethodResult showTablesMethod(MongoClient connect, MongoHandle mongoHandle, MongoDatabaseInfo databaseConf) throws Exception {
        QueryMethodResult queryMethodResult = new QueryMethodResult();
        List<Map<String, Object>> dataList = new ArrayList<>();

        MongoCursor<String> mongoCursor = null;
        try {
            MongoDatabase database = connect.getDatabase(databaseConf.getDatabaseName());
            mongoCursor = database.listCollectionNames().iterator();
            while (mongoCursor.hasNext()) {
                Map<String, Object> resultMap = new HashMap<>(1);
                String tableName = mongoCursor.next();
                resultMap.put("tableName", tableName);
                dataList.add(resultMap);
            }
        } finally {
            if (mongoCursor != null) {
                mongoCursor.close();
            }
        }
        dataList = WildcardRegularConversion.matchFilter(mongoHandle.getMongoListTable().getTableMatch(), dataList);
        queryMethodResult.setRows(dataList);
        queryMethodResult.setTotal(dataList.size());
        return queryMethodResult;
    }

    @Override
    protected QueryMethodResult getIndexesMethod(MongoClient connect, MongoDatabaseInfo databaseConf, MongoTableInfo tableConf) throws Exception {
        QueryMethodResult queryMethodResult = new QueryMethodResult();
        List<Map<String, Object>> dataList = new ArrayList<>();
        queryMethodResult.setRows(dataList);
        MongoCursor<Document> mongoCursor = null;
        try {
            MongoDatabase database = connect.getDatabase(databaseConf.getDatabaseName());
            MongoCollection<Document> collection = database.getCollection(tableConf.getTableName());
            ListIndexesIterable<Document> documents = collection.listIndexes();
            // 设置mongo服务端的超时时间
            Integer timeOut = dbThreadBaseConfig.getTimeOut();
            documents.maxTime(timeOut, TimeUnit.SECONDS);
            mongoCursor = documents.iterator();
            while (mongoCursor.hasNext()) {
                Map<String, Object> resultMap = new HashMap<>(1);
                dataList.add(resultMap);
                Document document = mongoCursor.next();
                Document index = (Document) document.get("key");
                String name = (String) document.get("name");
                Boolean unique = (Boolean) document.get("unique");
                List<String> indexSet = new ArrayList<>(index.keySet());
                MapBuilder<String, String> builder = MapUtil.builder(new LinkedHashMap<String, String>());
                for (String key : index.keySet()) {
                    Object o = index.get(key);
                    if (o == null) {
                        continue;
                    }
                    Double aDouble = Double.valueOf(String.valueOf(o));
                    int order = aDouble.intValue();
                    String sort = null;
                    if (order == 1) {
                        sort = Sort.ASC.name();
                    } else {
                        sort = Sort.DESC.name();
                    }
                    builder.put(key, sort);
                }
                resultMap.put("column", CollectionUtil.join(indexSet, ","));
                resultMap.put("columns", builder.build());
                resultMap.put("indexName", name);
                if (resultMap.get("column").equals("_id")) {
                    // 主键
                    resultMap.put("isUnique", true);
                    resultMap.put("isPrimaryKey", true);
                } else {
                    resultMap.put("isUnique", unique);
                    resultMap.put("isPrimaryKey", false);
                }
            }
            queryMethodResult.setTotal(dataList.size());
        } finally {
            if (mongoCursor != null) {
                mongoCursor.close();
            }
        }
        return queryMethodResult;
    }

    @Override
    protected QueryMethodResult createTableMethod(MongoClient connect, MongoHandle mongoHandle, MongoDatabaseInfo databaseConf, MongoTableInfo tableConf) throws Exception {
        // 判断是否是集群
        if (ClusterType.SHARDED.equals(databaseConf.getClusterType())) {
            // 在目标数据库创建索引
            MongoDatabase database = connect.getDatabase(databaseConf.getDatabaseName());
            MongoCollection<Document> collection = database.getCollection(tableConf.getTableName());
            collection.createIndex(new BasicDBObject("_id", "hashed"));
            // ADMIN 数据创建分片集合
            MongoDatabase admin = connect.getDatabase("admin");
            BasicDBObject bson = new BasicDBObject();
            bson.append("shardcollection", databaseConf.getDatabaseName() + "." + tableConf.getTableName())
                    .append("key", new BasicDBObject("_id", "hashed"))
                    .append("unique", false)
                    .append("options", new BasicDBObject("numInitialChunks", tableConf.getNumInitialChunks()));
            DatabaseExecuteStatementLog.set("db.runCommand(" + JsonUtil.objectToStr(bson) + ")");
            admin.runCommand(bson);
        } else {
            MongoDatabase database = connect.getDatabase(databaseConf.getDatabaseName());
            if (mongoHandle.getMongoTable().getOptions() != null) {
                DatabaseExecuteStatementLog.set("db.createCollection(" + tableConf.getTableName() + ")");
                database.createCollection(tableConf.getTableName(), mongoHandle.getMongoTable().getOptions());
            } else {
                DatabaseExecuteStatementLog.set("db.createCollection(" + tableConf.getTableName() + ")");
                database.createCollection(tableConf.getTableName());
            }
        }
        return new QueryMethodResult();
    }

    @Override
    protected QueryMethodResult dropTableMethod(MongoClient connect, MongoHandle mongoHandle, MongoDatabaseInfo databaseConf, MongoTableInfo tableConf) throws Exception {
        MongoDatabase database = connect.getDatabase(databaseConf.getDatabaseName());
        database.getCollection(tableConf.getTableName()).drop();
        return new QueryMethodResult();
    }

    @Override
    protected QueryMethodResult deleteIndexMethod(MongoClient connect, MongoHandle mongoHandle, MongoDatabaseInfo databaseConf, MongoTableInfo tableConf) throws Exception {
        MongoDatabase database = connect.getDatabase(databaseConf.getDatabaseName());
        MongoHandle.MongoIndex mongoIndex = mongoHandle.getMongoIndex();
        IndexOptions options = mongoIndex.getOptions();
        String indexName = null;
        if (options != null) {
            indexName = options.getName();
        }
        if (StringUtils.isNotBlank(indexName)) {
            database.getCollection(tableConf.getTableName()).dropIndex(indexName);
        } else {
            database.getCollection(tableConf.getTableName()).dropIndex(mongoHandle.getMongoIndex().getIndex());
        }
        return new QueryMethodResult();
    }

    @Override
    protected QueryMethodResult createIndexMethod(MongoClient connect, MongoHandle mongoHandle, MongoDatabaseInfo databaseConf, MongoTableInfo tableConf) throws Exception {
        MongoDatabase database = connect.getDatabase(databaseConf.getDatabaseName());
        MongoCollection<Document> collection = database.getCollection(tableConf.getTableName());
        collection.createIndex(mongoHandle.getMongoIndex().getIndex(), mongoHandle.getMongoIndex().getOptions());
        return new QueryMethodResult();
    }

    @Override
    protected QueryMethodResult insertMethod(MongoClient connect, MongoHandle mongoHandle, MongoDatabaseInfo databaseConf, MongoTableInfo tableConf) throws Exception {
        MongoHandle.MongoInsert mongoInsert = mongoHandle.getMongoInsert();
        MongoDatabase database = connect.getDatabase(databaseConf.getDatabaseName());
        MongoCollection<Document> collection = database.getCollection(tableConf.getTableName());
        // 写模式
        WriteConcern writeConcern = mongoInsert.getWriteConcern();
        if (WriteConcern.ACKNOWLEDGED.equals(writeConcern) && tableConf.getReplica() != null && tableConf.getReplica() > 0) {
            writeConcern = new WriteConcern(tableConf.getReplica());
        }
        collection = collection.withWriteConcern(writeConcern);
        collection.insertMany(mongoInsert.getInsertList());
        return new QueryMethodResult(mongoInsert.getInsertList().size(), null);
    }

    @Override
    protected QueryMethodResult updateMethod(MongoClient connect, MongoHandle mongoHandle, MongoDatabaseInfo databaseConf, MongoTableInfo tableConf) throws Exception {
        MongoHandle.MongoUpdate mongoUpdate = mongoHandle.getMongoUpdate();
        MongoDatabase database = connect.getDatabase(databaseConf.getDatabaseName());
        MongoCollection<Document> collection = database.getCollection(tableConf.getTableName());
        WriteConcern writeConcern = mongoUpdate.getWriteConcern();
        if (WriteConcern.ACKNOWLEDGED.equals(writeConcern) && tableConf.getReplica() != null && tableConf.getReplica() > 0) {
            writeConcern = new WriteConcern(tableConf.getReplica());
        }
        collection = collection.withWriteConcern(writeConcern);
        UpdateResult updateResult;
        if (mongoUpdate.getMulti()) {
            updateResult = collection.updateMany(mongoUpdate.getQuery(), mongoUpdate.getUpdate(), mongoUpdate.getOptions());
        } else {
            updateResult = collection.updateOne(mongoUpdate.getQuery(), mongoUpdate.getUpdate(), mongoUpdate.getOptions());
        }
        return new QueryMethodResult(updateResult.wasAcknowledged() ? updateResult.getMatchedCount() : 0, null);
    }

    @Override
    protected QueryMethodResult delMethod(MongoClient connect, MongoHandle mongoHandle, MongoDatabaseInfo databaseConf, MongoTableInfo tableConf) throws Exception {
        MongoHandle.MongoDel mongoDel = mongoHandle.getMongoDel();
        MongoDatabase database = connect.getDatabase(databaseConf.getDatabaseName());
        MongoCollection<Document> collection = database.getCollection(tableConf.getTableName());
        WriteConcern writeConcern = mongoDel.getWriteConcern();
        if (WriteConcern.ACKNOWLEDGED.equals(writeConcern) && tableConf.getReplica() != null && tableConf.getReplica() > 0) {
            writeConcern = new WriteConcern(tableConf.getReplica());
        }
        collection = collection.withWriteConcern(writeConcern);
        DeleteResult deleteResult = collection.deleteMany(mongoDel.getQuery());
        return new QueryMethodResult(deleteResult.wasAcknowledged() ? deleteResult.getDeletedCount() : 0, null);
    }

    @Override
    protected QueryMethodResult monitorStatusMethod(MongoClient connect, MongoDatabaseInfo databaseConf) throws Exception {
        List<Map<String, Object>> result = new ArrayList<>();
        Map<String, Object> map = new HashMap<>(16);
        result.add(map);
        MongoDatabase database = connect.getDatabase(databaseConf.getDatabaseName());
        BasicDBObject stat = new BasicDBObject();
        stat.append("dbStats", 1);
        Document statResult = database.runCommand(stat);
        map.putAll(statResult);
        BasicDBObject serverStatus = new BasicDBObject();
        serverStatus.append("serverStatus", 1);
        Document serverStatusResult = database.runCommand(serverStatus);
        map.putAll(serverStatusResult);
        MongoDatabase authDatabase = connect.getDatabase(StringUtils.isBlank(databaseConf.getAuthDatabase()) ? "admin" : databaseConf.getAuthDatabase());
        BasicDBObject currentOp = new BasicDBObject();
        currentOp.append("currentOp", 1);
        Document currentOpResult = authDatabase.runCommand(currentOp);
        map.putAll(currentOpResult);
        return new QueryMethodResult(result.size(), result);
    }

    @Override
    protected QueryMethodResult querySchemaMethod(MongoClient connect, MongoDatabaseInfo databaseConf, MongoTableInfo tableConf) throws Exception {
        MongoHandle mongoHandle = new MongoHandle();
        MongoHandle.MongoQuery mongoQuery = new MongoHandle.MongoQuery();
        mongoHandle.setMongoQuery(mongoQuery);
        mongoQuery.setBatchSize(1);
        mongoQuery.setCount(false);
        mongoQuery.setLimit(1);
        List<Map<String, Object>> result = new ArrayList<>();
        MongoDatabase database = connect.getDatabase(databaseConf.getDatabaseName());
        MongoCollection<Document> collection = database.getCollection(tableConf.getTableName());
        FindIterable<Document> documents = collection.find().limit(1).maxTime(dbThreadBaseConfig.getTimeOut(), TimeUnit.SECONDS);
        List<Map<String, Object>> list = traverseResultSchema(documents.first());
        if (CollectionUtil.isNotEmpty(list)) {
            result.addAll(list);
        }
        return new QueryMethodResult(result.size(), result);
    }

    @Override
    protected QueryMethodResult queryDatabaseSchemaMethod(MongoClient connect, MongoDatabaseInfo databaseConf, MongoTableInfo tableConf) throws Exception {
        return new QueryMethodResult(0L, new ArrayList<>());
    }

    /**
     * 解析 Document 字段 与 类型
     *
     * @param rowMap
     * @return
     */
    private List<Map<String, Object>> traverseResultSchema(Map<String, Object> rowMap) {
        if (MapUtil.isEmpty(rowMap)) {
            return com.meiya.whalex.util.collection.CollectionUtil.EMPTY_LIST;
        }
        List<Map<String, Object>> result = new ArrayList<>();
        rowMap.forEach((key, value) -> {
            Map<String, Object> fieldMap = new HashMap<>(3);
            result.add(fieldMap);
            fieldMap.put("col_name", key);
            String type = value == null ? "Null" : value.getClass().getSimpleName();
            fieldMap.put("data_type", type);
            fieldMap.put("std_data_type", MongoDbFieldTypeEnum.dbFieldType2FieldType(type).getVal());
            if (value instanceof Document) {
                Document document = (Document) value;
                List<Map<String, Object>> maps = traverseResultSchema(document);
                fieldMap.put("fields", maps);
            }
        });
        return result;
    }

    @Override
    protected QueryCursorMethodResult queryCursorMethod(MongoClient mongoClient, MongoHandle mongoHandle, MongoDatabaseInfo databaseConf, MongoTableInfo tableConf, MongoCursorCache cursorCache, Consumer<Map<String, Object>> consumer, Boolean rollAllData) throws Exception {

        if (cursorCache == null) {
            Long total = 0L;

            MongoHandle.MongoQuery queryEntity = mongoHandle.getMongoQuery();

            BasicDBObject queryCondition = queryEntity.getQuery();
            BasicDBObject fields = queryEntity.getFields();
            int limit = queryEntity.getLimit();
            int skip = queryEntity.getSkip();
            BasicDBObject sort = queryEntity.getSort();
            boolean isCount = queryEntity.getCount();
            // 记录查询语句
            mongoHandle.setQueryStr(transitionQueryStr(tableConf.getTableName(), queryEntity));
            // 通过游标查询
            MongoCursor<Document> cursor = null;

            // 设置mongo服务端的超时时间
            Integer timeOut = dbThreadBaseConfig.getTimeOut();

            MongoDatabase database = mongoClient.getDatabase(databaseConf.getDatabaseName());
            MongoCollection<Document> collection = database.getCollection(tableConf.getTableName());
            // 若需要统计则设置超时时间
            if (isCount) {
                CountOptions countOptions = queryEntity.getCountOptions().maxTime(timeOut, TimeUnit.SECONDS);
                total = collection.countDocuments(null == queryCondition ? new BasicDBObject() : queryCondition, countOptions);
            }
            FindIterable<Document> documents;
            // 判断是否设置读取超时时间
            if (queryEntity.isLimitReadTime()) {
                documents = collection.find(null == queryCondition ? new BasicDBObject() : queryCondition)
                        .projection(fields).maxTime(timeOut, TimeUnit.SECONDS);
            } else {
                documents = collection.find(null == queryCondition ? new BasicDBObject() : queryCondition)
                        .projection(fields);
            }

            // 查询提示
            if (StringUtils.isNotBlank(queryEntity.getHintString())) {
                documents.hintString(queryEntity.getHintString());
            } else if (queryEntity.getHint() != null) {
                documents.hint(queryEntity.getHint());
            }

            // 若自定义批量数，则设置
            if (queryEntity.getBatchSize() != null && queryEntity.getBatchSize() > 0) {
                documents.batchSize(queryEntity.getBatchSize());
            }
            if (skip > 0) {
                documents.skip(skip);
            }
            if (limit > 0) {
                documents.limit(limit);
            }
            if (null != sort) {
                documents.sort(sort);
            }
            cursor = documents.iterator();
            return getQueryCursorMethodResult(consumer, rollAllData, total, queryEntity.getBatchSize(), cursor);
        } else {
            return getQueryCursorMethodResult(consumer, rollAllData, 0L, cursorCache.getBatchSize(), cursorCache.getCursor());
        }
    }

    /**
     * 滚动游标获取数据
     *
     * @param consumer
     * @param rollAllData
     * @param total
     * @param batchSize
     * @param cursor
     * @return
     */
    private QueryCursorMethodResult getQueryCursorMethodResult(Consumer<Map<String, Object>> consumer, Boolean rollAllData, Long total, Integer batchSize, MongoCursor<Document> cursor) {
        try {
            // 滚动数据量
            long rollTotal = 0;

            while (cursor.hasNext()) {
                Document next = cursor.next();
                rollTotal++;
                consumer.accept(next);
                // 非一次滚完所有数据，并且当前滚动数据量大于等于批量数即可返回
                if (!rollAllData && rollTotal >= batchSize) {
                    break;
                }
            }

            // 判断是否当前游标滚动完所有数据才结束
            if (!rollAllData) {
                // 当前只滚动批量数即返回，则判断是否还有数据，是否需要保存游标
                if (cursor.hasNext()) {
                    return new QueryCursorMethodResult(total, new MongoCursorCache(null, batchSize, cursor));
                } else {
                    rollAllData = Boolean.TRUE;
                    return new QueryCursorMethodResult(total, null);
                }
            } else {
                // 当次滚动完所有数据
                return new QueryCursorMethodResult(total, null);
            }
        } catch (Exception e) {
            rollAllData = Boolean.TRUE;
            throw e;
        } finally {
            if (cursor != null && rollAllData) {
                cursor.close();
            }
        }
    }

    @Override
    protected QueryMethodResult saveOrUpdateMethod(MongoClient connect, MongoHandle queryEntity, MongoDatabaseInfo databaseConf, MongoTableInfo tableConf) throws Exception {
        return updateMethod(connect, queryEntity, databaseConf, tableConf);
    }

    @Override
    protected QueryMethodResult saveOrUpdateBatchMethod(MongoClient connect, MongoHandle queryEntity, MongoDatabaseInfo databaseConf, MongoTableInfo tableConf) throws Exception {
        MongoHandle.MongoUpsertBatch mongoUpsertBatch = queryEntity.getMongoUpsertBatch();
        List<UpdateManyModel<Document>> updateOneModelList = mongoUpsertBatch.getUpdateOneModelList();
        WriteConcern writeConcern = mongoUpsertBatch.getWriteConcern();
        MongoDatabase database = connect.getDatabase(databaseConf.getDatabaseName());
        MongoCollection<Document> collection = database.getCollection(tableConf.getTableName());
        if (WriteConcern.ACKNOWLEDGED.equals(writeConcern) && tableConf.getReplica() != null && tableConf.getReplica() > 0) {
            writeConcern = new WriteConcern(tableConf.getReplica());
        }
        collection = collection.withWriteConcern(writeConcern);
        BulkWriteResult bulkWriteResult = collection.bulkWrite(updateOneModelList);
        return new QueryMethodResult(bulkWriteResult.wasAcknowledged() ? bulkWriteResult.getMatchedCount() : 0, null);
    }

    @Override
    protected QueryMethodResult alterTableMethod(MongoClient connect, MongoHandle queryEntity, MongoDatabaseInfo databaseConf, MongoTableInfo tableConf) throws Exception {
        MongoHandle.MongoAlterTable alterTable = queryEntity.getAlterTable();
        List<Bson> addFields = alterTable.getAddFields();
        List<Bson> updateFields = alterTable.getUpdateFields();
        List<Bson> delFields = alterTable.getDelFields();
        Document renameCommand = alterTable.getRenameCommand();

        MongoDatabase database = connect.getDatabase(databaseConf.getDatabaseName());
        MongoCollection<Document> collection = database.getCollection(tableConf.getTableName());

        if (CollectionUtil.isNotEmpty(addFields)) {
            collection.updateMany(new Document(), Updates.combine(addFields));
        }

        if (CollectionUtil.isNotEmpty(updateFields)) {
            collection.updateMany(new Document(), Updates.combine(updateFields));
        }

        if (CollectionUtil.isNotEmpty(delFields)) {
            collection.updateMany(new Document(), Updates.combine(delFields));
        }

        if (renameCommand != null) {
            MongoDatabase admin = connect.getDatabase("admin");
            admin.runCommand(renameCommand);
        }

        return new QueryMethodResult();
    }

    @Override
    protected QueryMethodResult tableExistsMethod(MongoClient connect, MongoDatabaseInfo databaseConf, MongoTableInfo tableConf) throws Exception {
        MongoCursor<String> mongoCursor = null;
        Map<String, Object> map = new HashMap<>(1);
        try {
            MongoDatabase database = connect.getDatabase(databaseConf.getDatabaseName());
            mongoCursor = database.listCollectionNames().iterator();
            List<String> tables = CollectionUtil.newArrayList(mongoCursor);
            map.put(tableConf.getTableName(), tables.contains(tableConf.getTableName()));
        } finally {
            if (mongoCursor != null) {
                mongoCursor.close();
            }
        }
        return new QueryMethodResult(1, CollectionUtil.newArrayList(map));
    }

    @Override
    protected QueryMethodResult queryTableInformationMethod(MongoClient connect, MongoDatabaseInfo databaseConf, MongoTableInfo tableConf) throws Exception {
        Map<String, Object> extendMetaMap = new HashMap<>();
        MongoDatabase database = connect.getDatabase(databaseConf.getDatabaseName());
        BasicDBObject bson = new BasicDBObject();
        bson.append("collStats", tableConf.getTableName());
        bson.append("scale", 1);
        Document document = database.runCommand(bson);
        Boolean sharded = (Boolean) document.get("sharded");
        extendMetaMap.put("sharded", sharded);
        Boolean capped = (Boolean) document.get("capped");
        extendMetaMap.put("capped", capped);
        Object countObj = document.get("count");
        if (countObj != null) {
            Long count = Long.valueOf(String.valueOf(countObj));
            extendMetaMap.put("rows", count);
        } else {
            extendMetaMap.put("rows", 0L);
        }
        Object sizeObj = document.get("size");
        if (sizeObj != null) {
            Long size = Long.valueOf(String.valueOf(sizeObj));
            extendMetaMap.put("size", size);
        } else {
            extendMetaMap.put("size", 0L);
        }
        Object storageSizeObj = document.get("storageSize");
        if (storageSizeObj != null) {
            Long storageSize = Long.valueOf(String.valueOf(storageSizeObj));
            extendMetaMap.put("storageSize", storageSize);
        } else {
            extendMetaMap.put("storageSize", 0L);
        }
        Object avgObjSizeObj = document.get("avgObjSize");
        if (avgObjSizeObj != null) {
            Double avgObjSize = Double.valueOf(String.valueOf(avgObjSizeObj));
            extendMetaMap.put("avgObjSize", avgObjSize);
        } else {
            extendMetaMap.put("avgObjSize", 0D);
        }
        Object nchunksObj = document.get("nchunks");
        if (nchunksObj != null) {
            Long nchunks = Long.valueOf(String.valueOf(nchunksObj));
            extendMetaMap.put("nchunks", nchunks);
        } else {
            extendMetaMap.put("nchunks", 0L);
        }
        // 分片信息
        Document shards = (Document) document.get("shards");
        Map<String, Object> shardInfoMap = new HashMap<>();
        extendMetaMap.put("shardInfo", shardInfoMap);
        if (shards != null) {
            for (Map.Entry<String, Object> entry : shards.entrySet()) {
                // shard 名称
                String shardName = entry.getKey();
                Document shardDocument = (Document) entry.getValue();
                // shard 文档数
                Object count = shardDocument.get("count");
                Long shardCount;
                if (count != null) {
                    shardCount = Long.valueOf(String.valueOf(count));
                } else {
                    shardCount = 0L;
                }
                shardInfoMap.put(shardName, shardCount);
            }
        }
        Object maxSizeObj = document.get("maxSize");
        if (maxSizeObj != null) {
            Long maxSize = (Long) document.get("maxSize");
            extendMetaMap.put("maxSize", maxSize);
        } else {
            extendMetaMap.put("maxSize", 0L);
        }
        TableInformation<Object> tableInformation = MongoDbTableInformation.builder()
                .tableName(tableConf.getTableName())
                .shards(shardInfoMap.size())
                .extendMeta(extendMetaMap)
                .build();
        return new QueryMethodResult(1, CollectionUtil.newArrayList(JsonUtil.entityToMap(tableInformation)));
    }

    @Override
    protected QueryMethodResult queryListDatabaseMethod(MongoClient connect, MongoHandle queryEntity, MongoDatabaseInfo databaseConf, MongoTableInfo tableConf) throws Exception {
        //获取数据库列表
        MongoIterable<String> databaseNames = connect.listDatabaseNames();
        //组装数据库列表
        List<Map<String, Object>> rows = new ArrayList<>();
        for (String databaseName : databaseNames) {
            Map<String, Object> databaseNameMap = new HashMap<>();
            databaseNameMap.put("Database", databaseName);
            rows.add(databaseNameMap);
        }

        //按条件过滤
        String databaseMatch = queryEntity.getMongoListDatabase().getDatabaseMatch();
        if (StringUtils.isNotBlank(databaseMatch)) {
            rows = WildcardRegularConversion.matchFilter(databaseMatch, rows, "Database");
        }

        return new QueryMethodResult(rows.size(), rows);
    }

    @Override
    protected QueryMethodResult queryDatabaseInformationMethod(MongoClient connect, MongoHandle queryEntity, MongoDatabaseInfo databaseConf, MongoTableInfo tableConf) {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + ".queryDatabaseInformationMethod");
    }

    @Override
    protected QueryMethodResult createDatabaseMethod(MongoClient connect, MongoHandle queryEntity, MongoDatabaseInfo databaseConf, MongoTableInfo tableConf) throws Exception {
        MongoHandle.MongoCreateDatabase mongoCreateDatabase = queryEntity.getMongoCreateDatabase();
        String dbName = mongoCreateDatabase.getDbName();
        MongoDatabase database = connect.getDatabase(dbName);
        return new QueryMethodResult(1, null);
    }

    @Override
    protected QueryMethodResult dropDatabaseMethod(MongoClient connect, MongoHandle queryEntity, MongoDatabaseInfo databaseConf, MongoTableInfo tableConf) throws Exception {
        MongoHandle.MongoDropDatabase mongoDropDatabase = queryEntity.getMongoDropDatabase();
        String dbName = mongoDropDatabase.getDbName();
        MongoDatabase database = connect.getDatabase(dbName);
        database.drop();
        return new QueryMethodResult(1, null);
    }

    /**
     * 查询语句（用于日志输出）
     *
     * @param tableName
     * @param mongoQuery
     * @return
     */
    private String transitionQueryStr(String tableName, MongoHandle.MongoQuery mongoQuery) {
        try {
            BasicDBObject query = mongoQuery.getQuery();
            BasicDBObject fields = mongoQuery.getFields();
            int limit = mongoQuery.getLimit();
            int skip = mongoQuery.getSkip();
            BasicDBObject sort = mongoQuery.getSort();
            StringBuilder queryStr = new StringBuilder().append("db.").append(tableName);
            if (query != null && query.size() > 0) {
                queryStr.append(".find(").append(query.toString());
                if (fields != null && fields.size() > 0) {
                    queryStr.append(",").append(fields.toString()).append(")");
                } else {
                    queryStr.append(")");
                }
            } else {
                queryStr.append(".find(");
                if (fields != null && fields.size() > 0) {
                    queryStr.append(fields.toString()).append(")");
                } else {
                    queryStr.append(")");
                }
            }
            if (limit > 0) {
                queryStr.append(".limit(").append(limit).append(")");
            }
            if (skip > 0) {
                queryStr.append(".skip(").append(skip).append(")");
            }
            if (sort != null && sort.size() > 0) {
                queryStr.append(".sort(").append(sort.toString()).append(")");
            }
            return queryStr.toString();
        } catch (Exception e) {
            log.error("mongodb query string transition fail!", e);
            return null;
        }
    }

    /**
     * 查询语句（用于日志输出）
     *
     * @param tableName
     * @param mongoQuery
     * @return
     */
    private String transitionAggQueryStr(String tableName, MongoHandle.MongoQuery mongoQuery) {
        try {
            StringBuilder queryStrList = new StringBuilder();
            BasicDBObject query = mongoQuery.getQuery();
            List<MongoHandle.MongoAggQuery> aggregates = mongoQuery.getAggregates();
            for (MongoHandle.MongoAggQuery aggregate : aggregates) {
                StringBuilder queryStr = new StringBuilder().append("db.").append(tableName);
                queryStr.append(".aggregate([");
                if (query != null && query.size() > 0) {
                    BasicDBObject queryFilter = new BasicDBObject();
                    queryFilter.append("$match", query);
                    queryStr.append(queryFilter.toString() + ",");
                }
                queryStr.append(aggregate.getAggObj().toString());
                queryStr.append("])");
                queryStrList.append(queryStr.toString() + "   ");
            }
            return queryStrList.toString();
        } catch (Exception e) {
            log.error("mongodb query string transition fail!", e);
            return null;
        }
    }

    @Override
    protected DbTransactionModuleService getDbTransactionModuleService(DatabaseSetting databaseSetting,
                                                                       String transactionIdKey,
                                                                       String transactionFirstKey,
                                                                       String isolationLevelKey,
                                                                       String transactionId,
                                                                       IsolationLevel isolationLevel) {
        return new NoTransactionModuleService(
                databaseSetting, this, transactionId, isolationLevel, transactionIdKey, transactionFirstKey, isolationLevelKey
        );
    }
}
