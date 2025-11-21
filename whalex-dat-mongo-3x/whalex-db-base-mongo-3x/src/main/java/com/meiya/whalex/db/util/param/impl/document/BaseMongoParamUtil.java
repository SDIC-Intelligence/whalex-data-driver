package com.meiya.whalex.db.util.param.impl.document;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.map.MapBuilder;
import com.meiya.whalex.annotation.DbParamUtil;
import com.meiya.whalex.db.constant.IndexType;
import com.meiya.whalex.db.entity.*;
import com.meiya.whalex.db.entity.document.MongoDatabaseInfo;
import com.meiya.whalex.db.entity.document.MongoHandle;
import com.meiya.whalex.db.entity.document.MongoTableInfo;
import com.meiya.whalex.db.util.param.AbstractDbModuleParamUtil;
import com.meiya.whalex.exception.BusinessException;
import com.meiya.whalex.exception.ExceptionCode;
import com.meiya.whalex.interior.db.constant.*;
import com.meiya.whalex.interior.db.operation.in.*;
import com.meiya.whalex.interior.db.search.condition.AggFunctionType;
import com.meiya.whalex.interior.db.search.condition.AggOpType;
import com.meiya.whalex.interior.db.search.condition.Rel;
import com.meiya.whalex.interior.db.search.condition.Sort;
import com.meiya.whalex.interior.db.search.in.*;
import com.meiya.whalex.util.JsonUtil;
import com.meiya.whalex.util.date.DateUtil;
import com.meiya.whalex.util.date.JodaTimeUtil;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.WriteConcern;
import com.mongodb.client.model.*;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * MONGO 组件参数转换工具类
 *
 * @author 黄河森
 * @date 2019/9/14
 * @project whale-cloud-platformX
 */
@DbParamUtil(dbType = DbResourceEnum.mongodb, version = DbVersionEnum.MONGO_3_0_1, cloudVendors = CloudVendorsEnum.OPEN)
public class BaseMongoParamUtil extends AbstractDbModuleParamUtil<MongoHandle, MongoDatabaseInfo, MongoTableInfo> {

    private final static Logger LOGGER = LoggerFactory.getLogger(BaseMongoParamUtil.class);

    public final static String AGG_HITS_TAG = "__HITS";

    public final static String AGG_HITS_TOTAL_TAG = "__HITS_TOTAL";

    @Override
    protected MongoHandle transitionListTableParam(QueryTablesCondition queryTablesCondition, MongoDatabaseInfo databaseInfo) throws Exception {
        MongoHandle mongoHandle = new MongoHandle();
        MongoHandle.MongoListTable mongoListTable = new MongoHandle.MongoListTable();
        mongoHandle.setMongoListTable(mongoListTable);
        mongoListTable.setTableMatch(queryTablesCondition.getTableMatch());
        return mongoHandle;
    }

    @Override
    protected MongoHandle transitionListDatabaseParam(QueryDatabasesCondition queryDatabasesCondition, MongoDatabaseInfo databaseInfo) throws Exception {
        MongoHandle mongoHandle = new MongoHandle();
        MongoHandle.MongoListDatabase listDatabase = new MongoHandle.MongoListDatabase();
        listDatabase.setDatabaseMatch(queryDatabasesCondition.getDatabaseMatch());
        mongoHandle.setMongoListDatabase(listDatabase);
        return mongoHandle;
    }

    @Override
    protected MongoHandle transitionCreateTableParam(CreateTableParamCondition createTableParamCondition, MongoDatabaseInfo databaseInfo, MongoTableInfo tableInfo) {
        MongoHandle mongoHandle = new MongoHandle();
        MongoHandle.MongoTable mongoTable = new MongoHandle.MongoTable();
        mongoHandle.setMongoTable(mongoTable);
        Boolean capped = tableInfo.getCapped();
        Long maxDocuments = tableInfo.getMaxDocuments();
        Long sizeInBytes = tableInfo.getSizeInBytes();
        if (capped != null && capped) {
            CreateCollectionOptions options = new CreateCollectionOptions();
            options.capped(true);
            if (sizeInBytes != null) {
                options.sizeInBytes(sizeInBytes);
            }
            if (maxDocuments != null) {
                options.maxDocuments(maxDocuments);
            }
            mongoTable.setOptions(options);
        }
        return mongoHandle;
    }

    @Override
    protected MongoHandle transitionAlterTableParam(AlterTableParamCondition alterTableParamCondition, MongoDatabaseInfo databaseInfo, MongoTableInfo tableInfo) throws Exception {
        String newTableName = alterTableParamCondition.getNewTableName();
        List<AlterTableParamCondition.AddTableFieldParam> addTableFieldParamList = alterTableParamCondition.getAddTableFieldParamList();
        List<AlterTableParamCondition.UpdateTableFieldParam> updateTableFieldParamList = alterTableParamCondition.getUpdateTableFieldParamList();
        List<String> delTableFieldParamList = alterTableParamCondition.getDelTableFieldParamList();
        MongoHandle mongoHandle = new MongoHandle();
        MongoHandle.MongoAlterTable alterTable = new MongoHandle.MongoAlterTable();
        mongoHandle.setMongoAlterTable(alterTable);

        if (StringUtils.isNotBlank(newTableName)) {
            Document renameCommand = new Document("renameCollection", databaseInfo.getDatabaseName() + "." + tableInfo.getTableName())
                    .append("to", databaseInfo.getDatabaseName() + "." + newTableName);
            alterTable.setRenameCommand(renameCommand);
        }

        if (CollectionUtil.isNotEmpty(addTableFieldParamList)) {
            List<Bson> addFields = new ArrayList<>();
            Map<String, Object> addFieldsMap = new HashMap<>();
            for (AlterTableParamCondition.AddTableFieldParam addTableFieldParam : addTableFieldParamList) {
                String fieldName = addTableFieldParam.getFieldName();
                String defaultValue = addTableFieldParam.getDefaultValue();
                String fieldType = addTableFieldParam.getFieldType();
                if (StringUtils.isBlank(defaultValue)) {
                    throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "mongodb 新增字段: " + fieldName + " 必须携带默认值!");
                }
                try {
                    Object value = convertAddFieldDefaultValue(defaultValue, ItemFieldTypeEnum.findFieldTypeEnum(fieldType));
                    Bson bson = Updates.set(fieldName, value);
                    addFieldsMap.put(fieldName, value);
                    addFields.add(bson);
                } catch (Exception e) {
                    throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "新增字段: " + fieldName + " 默认值: " + defaultValue + " 转换为 " + fieldType + " 类型失败!");
                }
            }
            Map<Object, Object> set = MapBuilder.create().put("$set", addFieldsMap).build();
            alterTable.setAddFieldsJson(JsonUtil.objectToStr(set));
            alterTable.setAddFields(addFields);
        }

        if (CollectionUtil.isNotEmpty(updateTableFieldParamList)) {
            List<Bson> updateFields = new ArrayList<>();
            Map<String, Object> updateFieldsMap = new HashMap<>();
            for (AlterTableParamCondition.UpdateTableFieldParam updateTableFieldParam : updateTableFieldParamList) {
                Bson updateFiled = Updates.rename(updateTableFieldParam.getFieldName(), updateTableFieldParam.getNewFieldName());
                updateFieldsMap.put(updateTableFieldParam.getFieldName(), updateTableFieldParam.getNewFieldName());
                updateFields.add(updateFiled);
            }
            Map<Object, Object> rename = MapBuilder.create().put("$rename", updateFieldsMap).build();
            alterTable.setUpdateFieldsJson(JsonUtil.objectToStr(rename));
            alterTable.setUpdateFields(updateFields);
        }

        if (CollectionUtil.isNotEmpty(delTableFieldParamList)) {
            List<Bson> delFields = new ArrayList<>();
            Map<String, Object> delFieldsMap = new HashMap<>();
            for (String field : delTableFieldParamList) {
                Bson unset = Updates.unset(field);
                delFieldsMap.put(field, "\"\"");
                delFields.add(unset);
            }
            Map<Object, Object> unset = MapBuilder.create().put("$unset", delFieldsMap).build();
            alterTable.setDelFieldsJson(JsonUtil.objectToStr(unset));
            alterTable.setDelFields(delFields);
        }

        return mongoHandle;
    }

    /**
     * mongodb 增加字段默认值转换
     *
     * @param defaultValue
     * @param fieldType
     * @return
     * @throws Exception
     */
    private Object convertAddFieldDefaultValue(String defaultValue, ItemFieldTypeEnum fieldType) throws Exception {
        switch (fieldType) {
            case BOOLEAN:
            case REAL:
                return Boolean.valueOf(defaultValue);
            case SMALLINT:
            case MEDIUMINT:
            case TINYINT:
            case INTEGER:
                return Integer.valueOf(defaultValue);
            case DATE:
            case TIMESTAMP:
            case TIME:
            case DATETIME:
            case SMART_TIME:
            case YEAR:
                return DateUtils.parseDateStrictly(defaultValue, JodaTimeUtil.DEFAULT_YMD_FORMAT
                        , JodaTimeUtil.DEFAULT_YMDHMS_FORMAT
                        , JodaTimeUtil.SOLR_TDATE_FORMATE
                        , JodaTimeUtil.SOLR_TDATE_RETURN_FORMATE
                        , JodaTimeUtil.COMPACT_YMDHMS_FORMAT
                        , JodaTimeUtil.COMPACT_YMD_FORMAT
                        , JodaTimeUtil.COMPACT_YMDH_FORMAT);
            case LONG:
                return Long.valueOf(defaultValue);
            case DOUBLE:
                return Double.valueOf(defaultValue);
            case FLOAT:
                return Float.valueOf(defaultValue);
            case OBJECT:
                return JsonUtil.jsonStrToMap(defaultValue);
            case DECIMAL:
            case NUMERIC:
                return new BigDecimal(defaultValue);
            case ARRAY:
            case ARRAY_INTEGER:
            case ARRAY_DECIMAL:
            case ARRAY_CHAR:
            case ARRAY_LONG:
            case ARRAY_SMALLINT:
            case ARRAY_STRING:
            case ARRAY_TEXT:
            case ARRAY_TINYINT:
            case ARRAY_BIT:
            case ARRAY_DOUBLE:
            case ARRAY_FLOAT:
            case ARRAY_MEDIUMINT:
            case ARRAY_LONGTEXT:
            case ARRAY_MEDIUMTEXT:
            case ARRAY_POINT:
            case ARRAY_TINYTEXT:
            case ARRAY_NUMERIC:
                return JsonUtil.jsonStrToObject(defaultValue, List.class);
            default:
                return defaultValue;
        }
    }

    @Override
    protected MongoHandle transitionCreateIndexParam(IndexParamCondition indexParamCondition, MongoDatabaseInfo databaseInfo, MongoTableInfo tableInfo) {
        MongoHandle mongoHandle = new MongoHandle();
        MongoHandle.MongoIndex mongoIndex = new MongoHandle.MongoIndex();
        mongoHandle.setMongoIndex(mongoIndex);
        // 索引信息
        BasicDBObject bson = new BasicDBObject();
        // 获取组合索引
        List<IndexParamCondition.IndexColumn> columns = indexParamCondition.getColumns();
        if (CollectionUtil.isNotEmpty(columns)) {
            for (IndexParamCondition.IndexColumn indexColumn : columns) {
                if (Sort.DESC.equals(indexColumn.getSort())) {
                    bson.append(indexColumn.getColumn(), -1);
                } else {
                    bson.append(indexColumn.getColumn(), 1);
                }
            }
        } else {
            if ("-1".equals(indexParamCondition.getSort())) {
                bson.append(indexParamCondition.getColumn(), -1);
            } else {
                bson.append(indexParamCondition.getColumn(), 1);
            }
        }
        // 是否后台执行
        boolean background = true;
        if (indexParamCondition.getAsync() != null) {
            background = indexParamCondition.getAsync();
        }
        IndexOptions indexOptions = new IndexOptions();
        String indexName = indexParamCondition.getIndexName();
        if (StringUtils.isBlank(indexName)) {
            indexName = "index_" + tableInfo.getTableName() + "_" + CollectionUtil.join(bson.keySet(), "_");
        }
        indexOptions.name(indexName);
        indexOptions.background(background);
        // 是否唯一索引
        IndexType indexType = indexParamCondition.getIndexType();
        if (indexType != null) {
            switch (indexType) {
                case UNIQUE:
                    indexOptions.unique(true);
                    break;
            }
        }
        mongoIndex.setIndex(bson);
        mongoIndex.setOptions(indexOptions);
        return mongoHandle;
    }

    @Override
    protected MongoHandle transitionDropIndexParam(IndexParamCondition indexParamCondition, MongoDatabaseInfo databaseInfo, MongoTableInfo tableInfo) {
        return transitionCreateIndexParam(indexParamCondition, databaseInfo, tableInfo);
    }

    @Override
    public MongoHandle transitionQueryParam(QueryParamCondition queryParamCondition, MongoDatabaseInfo databaseInfo, MongoTableInfo tableInfo) {
        MongoHandle mongoHandle = new MongoHandle();

        MongoHandle.MongoQuery mongoQuery = new MongoHandle.MongoQuery();

        mongoHandle.setMongoQuery(mongoQuery);

        CountOptions countOptions = new CountOptions();

        mongoQuery.setCountOptions(countOptions);

        // 封装查询的列
        List<String> select = queryParamCondition.getSelect();
        if (CollectionUtils.isNotEmpty(select)) {
            boolean isNeedId = false;
            BasicDBObject fields = new BasicDBObject();
            for (int i = 0; i < select.size(); i++) {
                String field = select.get(i);
                fields.put(field, i + 1);
                if (StringUtils.equalsIgnoreCase(field, "_id")) {
                    isNeedId = true;
                }
            }
            if (!isNeedId) {
                fields.put("_id", 0);
            }
            mongoQuery.setFields(fields);
        }

        // 封装查询条件
        List<Where> whereList = queryParamCondition.getWhere();
        if (CollectionUtils.isNotEmpty(whereList)) {
            if(whereList.size() > 1) {
                Where where = Where.create(Rel.AND);
                where.setParams(whereList);
                whereList = Arrays.asList(where);
            }
            BasicDBObject query = new BasicDBObject();
            mongoWhereQuery(whereList, query);
            mongoQuery.setQuery(query);
        }

        // 分页条件
        Page page = queryParamCondition.getPage();
        if (page != null) {
            Integer limit = page.getLimit();
            if (limit == null) {
                limit = 10;
            }
            if (!limit.equals(Page.LIMIT_ALL_DATA)) {
                mongoQuery.setLimit(limit);
            }
            Integer offset = page.getOffset();
            mongoQuery.setSkip(offset);
        }

        // 聚合操作
        List<Aggs> aggList = queryParamCondition.getAggList();
        if (CollectionUtils.isNotEmpty(aggList)) {
            List<MongoHandle.MongoAggQuery> aggregates = new ArrayList<>(1);
            Map<String, String> aggNameMap = new HashMap<>(1);
            for (int i = 0; i < aggList.size(); i++) {
                Aggs agg = aggList.get(i);
                List<Aggs> sttAggList = new ArrayList<>();
                restructuringAgg(agg, sttAggList);
                for (Aggs sttAgg : sttAggList) {
                    MongoHandle.MongoAggQuery mongoAggQuery = new MongoHandle.MongoAggQuery();
                    List<BasicDBObject> aggregateList = new ArrayList<>();
                    mongoAgg(sttAgg, aggregateList);
                    mongoAggQuery.setAggObj(aggregateList);
                    mongoAggQuery.setLimit(sttAgg.getLimit());
                    aggregates.add(mongoAggQuery);
                }
            }
            mongoQuery.setAggNameMap(aggNameMap);
            mongoQuery.setAggregates(aggregates);
        }else{
            List<AggFunction> aggFunctionList = queryParamCondition.getAggFunctionList();
            if(CollectionUtils.isNotEmpty(aggFunctionList)) {

                BasicDBObject groupFiled = new BasicDBObject();
                groupFiled.append("__function", null);

                BasicDBObject basicDBObject = new BasicDBObject();
                basicDBObject.append("_id", groupFiled);
                mongoAggFunction(aggFunctionList, basicDBObject);

                BasicDBObject aggregate = new BasicDBObject();
                aggregate.append("$group", basicDBObject);

                Map<String, String> aggNameMap = new HashMap<>(1);
                MongoHandle.MongoAggQuery mongoAggQuery = new MongoHandle.MongoAggQuery();
                mongoAggQuery.setAggObj(Arrays.asList(aggregate));
                mongoAggQuery.setLimit(page.getLimit());
                List<MongoHandle.MongoAggQuery> aggregates = new ArrayList<>(1);
                aggregates.add(mongoAggQuery);

                mongoQuery.setAggNameMap(aggNameMap);
                mongoQuery.setAggregates(aggregates);
            }
        }

        // 排序条件
        List<Order> orderList = queryParamCondition.getOrder();
        if (CollectionUtils.isNotEmpty(orderList)) {
            BasicDBObject orderQuery = new BasicDBObject();
            for (Order order : orderList) {
                orderQuery.append(order.getField(), order.getSort().getName().equals(Sort.ASC.getName()) ? 1 : -1);
            }
            mongoQuery.setSort(orderQuery);
        }

        // 查询提示
        Hint hint = queryParamCondition.getHint();
        if (hint != null) {
            String indexName = hint.getIndexName();
            if (StringUtils.isNotBlank(indexName)) {
                BasicDBObject queryHint = new BasicDBObject("$hint", indexName);
                mongoQuery.setHint(queryHint);
            } else {
                Hint.HintIndex index = hint.getIndex();
                if (index != null) {
                    BasicDBObject queryHint = new BasicDBObject("$hint", new BasicDBObject(index.getColumn(), index.getSort() == null ? 1 : index.getSort() == Sort.ASC ? 1 : -1));
                    mongoQuery.setHint(queryHint);
                }
            }
        }

        mongoQuery.setCount(queryParamCondition.isCountFlag());
        mongoQuery.setBatchSize(queryParamCondition.getBatchSize());
        mongoQuery.setLimitReadTime(queryParamCondition.isLimitReadTime());
        return mongoHandle;
    }

    /**
     * 重组聚合对象,只保留最后一个层级的agg对象
     *
     * @param aggs
     * @param sttAgg
     */
    public void restructuringAgg(Aggs aggs, List<Aggs> sttAgg) {
        List<Aggs> aggList = aggs.getAggList();
        if (CollectionUtil.isNotEmpty(aggList)) {
            String field = aggs.getField();
            String aggName = aggs.getAggName();
            for (int i = 0; i < aggList.size(); i++) {
                Aggs subAgg = aggList.get(i);
                subAgg.setField(field + "," + subAgg.getField());
                subAgg.setAggName(aggName + "," + subAgg.getAggName());
                restructuringAgg(subAgg, sttAgg);
            }
        } else {
            sttAgg.add(aggs);
        }
    }

    @Override
    public MongoHandle transitionUpdateParam(UpdateParamCondition updateParamCondition, MongoDatabaseInfo databaseInfo, MongoTableInfo tableInfo) {
        MongoHandle mongoHandle = new MongoHandle();
        MongoHandle.MongoUpdate mongoUpdate = new MongoHandle.MongoUpdate();
        mongoHandle.setMongoUpdate(mongoUpdate);
        if (updateParamCondition != null) {

            // 设置写模式
            if (updateParamCondition.getAsync()) {
                mongoUpdate.setWriteConcern(WriteConcern.NORMAL);
            } else {
                mongoUpdate.setWriteConcern(WriteConcern.ACKNOWLEDGED);
            }

            // 封装更新字段
            Map<String, Object> updateMap = updateParamCondition.getUpdateParamMap();
            UpdateParamCondition.ArrayProcessMode arrayProcessMode = updateParamCondition.getArrayProcessMode();
            if (updateMap != null && updateMap.size() > 0) {
                BasicDBObject updateObj = new BasicDBObject();
                BasicDBObject setObj = new BasicDBObject();
                BasicDBObject pushObj = new BasicDBObject();
                BasicDBObject appendObj = new BasicDBObject();
                for (Map.Entry<String, Object> entry : updateMap.entrySet()) {
                    Object value = entry.getValue();
                    if (value != null
                            && (value instanceof List || (value.getClass().isArray() && !(value instanceof byte[])))
                            && !UpdateParamCondition.ArrayProcessMode.COVER.equals(arrayProcessMode)) {
                        BasicDBObject eachObj = new BasicDBObject();
                        eachObj.put("$each", value);
                        switch (arrayProcessMode) {
                            case ADD_TO_LIST:
                                pushObj.put(entry.getKey(), eachObj);
                                break;
                            default:
                                appendObj.put(entry.getKey(), eachObj);
                                break;
                        }
                    } else {
                        setObj.put(entry.getKey(), entry.getValue());
                    }
                }
                if (!setObj.isEmpty()) {
                    updateObj.put("$set", setObj);
                }
                if (!pushObj.isEmpty()) {
                    updateObj.put("$push", pushObj);
                }
                if (!appendObj.isEmpty()) {
                    updateObj.put("$addToSet", appendObj);
                }
                mongoUpdate.setUpdate(updateObj);
            }

            // 封装查询条件
            List<Where> whereList = updateParamCondition.getWhere();
            if (CollectionUtils.isNotEmpty(whereList)) {
                BasicDBObject query = new BasicDBObject();
                mongoWhereQuery(whereList, query);
                mongoUpdate.setQuery(query);
            }

            // 设置更新选型
            mongoUpdate.setOptions(new UpdateOptions().upsert(updateParamCondition.getUpsert()));

            // 设置是否更新多个
            mongoUpdate.setMulti(updateParamCondition.getMulti());
        }
        return mongoHandle;
    }

    @Override
    public MongoHandle transitionInsertParam(AddParamCondition addParamCondition, MongoDatabaseInfo databaseInfo, MongoTableInfo tableInfo) {
        MongoHandle mongoHandle = new MongoHandle();
        MongoHandle.MongoInsert mongoInsert = new MongoHandle.MongoInsert();
        mongoHandle.setMongoInsert(mongoInsert);
        // 设置写模式
        if (addParamCondition.getAsync()) {
            mongoInsert.setWriteConcern(WriteConcern.NORMAL);
        } else {
            mongoInsert.setWriteConcern(WriteConcern.ACKNOWLEDGED);
        }
        // 封装写入数据
        List<Document> dbObjectList = new ArrayList<>();
        List<Map<String, Object>> fieldValueList = addParamCondition.getFieldValueList();
        for (int i = 0; i < fieldValueList.size(); i++) {
            Map<String, Object> fieldMap = fieldValueList.get(i);
            Document dbObject = new Document();
            for (Map.Entry<String, Object> entry : fieldMap.entrySet()) {
                if (entry.getValue() instanceof Point) {
                    Point point = (Point) entry.getValue();
                    dbObject.put(entry.getKey(), Arrays.asList(point.getLon(), point.getLat()));
                } else {
                    dbObject.put(entry.getKey(), entry.getValue());
                }
            }
            dbObjectList.add(dbObject);
        }
        mongoInsert.setInsertList(dbObjectList);
        return mongoHandle;
    }

    @Override
    public MongoHandle transitionDeleteParam(DelParamCondition delParamCondition, MongoDatabaseInfo databaseInfo, MongoTableInfo tableInfo) {
        MongoHandle mongoHandle = new MongoHandle();
        MongoHandle.MongoDel mongoDel = new MongoHandle.MongoDel();
        mongoHandle.setMongoDel(mongoDel);
        // 设置写模式
        if (delParamCondition.getAsync()) {
            mongoDel.setWriteConcern(WriteConcern.NORMAL);
        } else {
            mongoDel.setWriteConcern(WriteConcern.ACKNOWLEDGED);
        }
        // 封装查询条件
        List<Where> whereList = delParamCondition.getWhere();
        if (CollectionUtils.isNotEmpty(whereList)) {
            BasicDBObject query = new BasicDBObject();
            mongoWhereQuery(whereList, query);
            mongoDel.setQuery(query);
        }
        return mongoHandle;
    }

    @Override
    protected MongoHandle transitionDropTableParam(DropTableParamCondition dropTableParamCondition, MongoDatabaseInfo databaseInfo, MongoTableInfo tableInfo) throws Exception {
        return new MongoHandle();
    }

    @Override
    public MongoHandle transitionUpsertParam(UpsertParamCondition paramCondition, MongoDatabaseInfo databaseInfo, MongoTableInfo tableInfo) throws Exception {
        // 1.获取指定 conflict 字段值构建查询条件
        Map<String, Object> updateParamMap = paramCondition.getUpsertParamMap();
        List<String> conflictFieldList = paramCondition.getConflictFieldList();
        // 拼装查询条件
        List<Where> collect = conflictFieldList.stream().flatMap(fieldName -> {
            Object value = updateParamMap.get(fieldName);
            return Stream.of(Where.create(fieldName, value));
        }).collect(Collectors.toList());
        UpdateParamCondition updateParamCondition = new UpdateParamCondition();
        updateParamCondition.setWhere(collect);
        updateParamCondition.setUpdateParamMap(paramCondition.getUpsertParamMap());
        updateParamCondition.setUpsert(true);
        updateParamCondition.setMulti(false);
        updateParamCondition.setAsync(paramCondition.getIsAsync());
        updateParamCondition.setArrayProcessMode(paramCondition.getArrayProcessMode());
        return transitionUpdateParam(updateParamCondition, databaseInfo, tableInfo);
    }

    @Override
    public MongoHandle transitionUpsertParamBatch(UpsertParamBatchCondition paramBatchCondition, MongoDatabaseInfo databaseInfo, MongoTableInfo tableInfo) throws Exception {
        MongoHandle mongoHandle = new MongoHandle();
        MongoHandle.MongoUpsertBatch mongoUpsertBatch = new MongoHandle.MongoUpsertBatch();
        mongoHandle.setMongoUpsertBatch(mongoUpsertBatch);
        List<UpdateManyModel<Document>> updateOneModels = new ArrayList<>();
        mongoUpsertBatch.setUpdateOneModelList(updateOneModels);
        List<Map<String, Object>> upsertParamList = paramBatchCondition.getUpsertParamList();
        List<String> conflictFieldList = paramBatchCondition.getConflictFieldList();
        for (Map<String, Object> upsertParam : upsertParamList) {
            List<Where> collect = conflictFieldList.stream().flatMap(fieldName -> {
                Object value = upsertParam.get(fieldName);
                return Stream.of(Where.create(fieldName, value));
            }).collect(Collectors.toList());
            UpdateParamCondition updateParamCondition = new UpdateParamCondition();
            updateParamCondition.setWhere(collect);
            updateParamCondition.setUpdateParamMap(upsertParam);
            updateParamCondition.setUpsert(true);
            updateParamCondition.setMulti(true);
            updateParamCondition.setArrayProcessMode(paramBatchCondition.getArrayProcessMode());
            MongoHandle _mongoHandle = transitionUpdateParam(updateParamCondition, databaseInfo, tableInfo);
            MongoHandle.MongoUpdate mongoUpdate = _mongoHandle.getMongoUpdate();
            BasicDBObject query = mongoUpdate.getQuery();
            BasicDBObject update = mongoUpdate.getUpdate();
            UpdateManyModel<Document> updateManyModel = new UpdateManyModel<>(query, update, new UpdateOptions().upsert(true));
            updateOneModels.add(updateManyModel);
        }
        // 设置写模式
        if (paramBatchCondition.getIsAsync()) {
            mongoUpsertBatch.setWriteConcern(WriteConcern.NORMAL);
        } else {
            mongoUpsertBatch.setWriteConcern(WriteConcern.ACKNOWLEDGED);
        }
        return mongoHandle;
    }

    /**
     * 循环递归拼接查询条件
     *
     * @param whereList
     * @param query
     */
    private static void mongoWhereQuery(List<Where> whereList, BasicDBObject query) {
        for (Where where : whereList) {

            switch (where.getType()) {
                case AND:
                    List<Where> andParams = where.getParams();
                    if (andParams == null || andParams.isEmpty()) {
                        break;
                    }
                    List<DBObject> dbAndObjectList = new ArrayList<>(andParams.size());
                    BasicDBObject andParamObj;
                    for (Where where1 : andParams) {
                        andParamObj = new BasicDBObject();
                        dbAndObjectList.add(andParamObj);
                        List<Where> wheres = new ArrayList<>(1);
                        wheres.add(where1);
                        mongoWhereQuery(wheres, andParamObj);
                    }
                    //同级and放一起
                    List<DBObject> andObjectList = (List<DBObject>) query.get("$" + where.getType().getName());
                    if(CollectionUtil.isNotEmpty(andObjectList)) {
                        dbAndObjectList.addAll(andObjectList);
                    }
                    query.put("$" + where.getType().getName(), dbAndObjectList);
                    break;
                case NOT:
                    List<Where> notParams = where.getParams();
                    if (notParams == null || notParams.isEmpty()) {
                        break;
                    }
                    List<DBObject> dbNotObjectList = new ArrayList<>(notParams.size());
                    BasicDBObject notParamObj;
                    for (Where where1 : notParams) {
                        notParamObj = new BasicDBObject();
                        dbNotObjectList.add(notParamObj);
                        List<Where> wheres = new ArrayList<>(1);
                        wheres.add(where1);
                        mongoWhereQuery(wheres, notParamObj);
                    }
                    //同级not放一起
                    List<DBObject> notObjectList = (List<DBObject>) query.get("$nor");
                    if(CollectionUtil.isNotEmpty(notObjectList)) {
                        dbNotObjectList.addAll(notObjectList);
                    }
                    query.put("$nor", dbNotObjectList);
                    break;
                case EQ:
                case TERM:
                    if (where.getField() == null) {
                        break;
                    }
                    if (ItemFieldTypeEnum.DATE.getVal().equals(where.getParamType())) {
                        Date date = JodaTimeUtil.time(where.getParam().toString()).toDate();
                        query.put(where.getField(), date);
                    } else if (ItemFieldTypeEnum.INTEGER.getVal().equals(where.getParamType()) && where.getParam() != null) {
                        query.put(where.getField(), Integer.parseInt(String.valueOf(where.getParam())));

                    } else if (ParamTypeConstant.OBJECTID.equals(where.getParamType()) && where.getParam() != null) {
                        query.put(where.getField(), new ObjectId(String.valueOf(where.getParam())));
                    } else if ((where.getParam() instanceof Point || ItemFieldTypeEnum.POINT.getVal().equals(where.getParamType()))
                            && where.getParam() != null) {
                        Double lon;
                        Double lat;
                        if (where.getParam() instanceof Point) {
                            Point point = (Point) where.getParam();
                            lon = point.getLon();
                            lat = point.getLat();
                        } else if (where.getParam() instanceof Map) {
                            Map<String, Double> pointList = (Map<String, Double>) where.getParam();
                            lon = pointList.get("lon");
                            lat = pointList.get("lat");
                        } else {
                            throw new BusinessException("mongodb filedType point value transition fail");
                        }
                        query.put(where.getField(), Arrays.asList(lon, lat));
                    } else {
                        query.put(where.getField(), where.getParam());
                    }
                    break;
                case ONLY_LIKE:
                case MIDDLE_LIKE: {
                    String params = String.valueOf(where.getParam());
                    params = StringUtils.replaceEach(params, new String[]{"*", "%", "?"}, new String[]{".*", ".*", "."});
                    if (!StringUtils.startsWithAny(params, "*", "%", "?")) {
                        params = "^" + params;
                    }
                    if (!StringUtils.endsWithAny(params, "*", "%", "?")) {
                        params = params + "$";
                    }
                    Pattern pattern = Pattern.compile(params);
                    query.put(where.getField(), pattern);
                }
                break;
                case ILIKE:
                case LIKE:
                    String likeParam = String.valueOf(where.getParam());
                    if (StringUtils.containsIgnoreCase(likeParam, "*")) {
                        likeParam = StringUtils.replace(likeParam, "*", ".*");
                    }
                    if (StringUtils.containsIgnoreCase(likeParam, "%")) {
                        likeParam = StringUtils.replace(likeParam, "%", ".*");
                    }
                    likeParam = "^" + likeParam + "$";
                    Pattern pattern = Pattern.compile(likeParam);
                    query.put(where.getField(), pattern);
                    break;
                case FRONT_LIKE:
                    String regexFront = String.valueOf(where.getParam());
                    regexFront = regexFront + "$";
                    Pattern patternFront = Pattern.compile(regexFront);
                    query.put(where.getField(), patternFront);
                    break;
                case TAIL_LIKE:
                    String regexTail = String.valueOf(where.getParam());
                    regexTail = "^" + regexTail;
                    Pattern patternTail = Pattern.compile(regexTail);
                    query.put(where.getField(), patternTail);
                    break;
                case OR:
                    List<Where> orParams = where.getParams();
                    if (orParams == null || orParams.isEmpty()) {
                        break;
                    }
                    List<DBObject> dbObjectList = new ArrayList<>(orParams.size());
                    BasicDBObject orParamObj;
                    for (Where where1 : orParams) {
                        orParamObj = new BasicDBObject();
                        dbObjectList.add(orParamObj);
                        List<Where> wheres = new ArrayList<>(1);
                        wheres.add(where1);
                        mongoWhereQuery(wheres, orParamObj);
                    }
                    //同级or放一起
                    List<DBObject> orObjectList = (List<DBObject>) query.get("$" + where.getType().getName());
                    if(CollectionUtil.isNotEmpty(orObjectList)) {
                        dbObjectList.addAll(orObjectList);
                    }
                    query.put("$" + where.getType().getName(), dbObjectList);
                    break;
                case MATCH:
                    if (where.getField() == null) {
                        continue;
                    }
                    if (ItemFieldTypeEnum.DATE.getVal().equals(where.getParamType())) {
                        Date date = JodaTimeUtil.time(where.getParam().toString()).toDate();
                        query.put(where.getField(), date);
                    } else {
                        query.put(where.getField(), where.getParam());
                    }
                    break;
                case IN:
                case NIN:
                    inOrNin(query, where);
                    break;
                case GEO_DISTANCE:
                    /** 地理位置查询, 圆形,对应的paramValue是Map对象，至少有两个参数，
                     * distance;//eg:distance:10km 半径
                     * loc;loc:"23.04805756,113.27598616"//圆点的中心值
                     * 如果不带单位，默认是m
                     */
                    if (where.getParam() instanceof Map) {
                        Map<String, Object> paramMap = (Map) where.getParam();
                        String loc = ObjectUtils.toString(paramMap.get(CommonConstant.LOC));
                        String[] split = loc.split(CommonConstant.COMMA);
                        double x = Double.valueOf(split[0]);
                        double y = Double.valueOf(split[1]);
                        BasicDBObject geometry = new BasicDBObject();
                        geometry.put("type", "point");
                        geometry.put("coordinates", new double[]{x, y});

                        BasicDBObject near = new BasicDBObject();
                        near.put("$geometry", geometry);
                        // 上层应用传输地理数据统一为58.333km这种格式，处理为58.333
                        String distance = (String) paramMap.get(CommonConstant.DISTANCE);
                        near.put("$maxDistance", Double.valueOf(DistanceUnitEnum.kmToM(distance)));
                        query.put(where.getField(), new BasicDBObject("$near", near));
                    } else {
                        LOGGER.error("GEO_DISTANCE的参数无法解析:" + where.getParam());
                    }
                    break;
                case GEO_BOX:
                    if (where.getParam() instanceof Map) {
                        Map<String, Object> paramMap = (Map<String, Object>) where.getParam();
                        List<List<Double>> box = new LinkedList<>();
                        for (Map.Entry<String, Object> entry : paramMap.entrySet()) {
                            List<Double> minList = new ArrayList<>();
                            List<Double> maxList = new ArrayList<>();
                            Double bottomRightX = 0D;
                            Double bottomRightY = 0D;
                            Double topLeftX = 0D;
                            Double topLeftY = 0D;
                            if ("bottomRight".equals(entry.getKey())) {
                                String bottomRight = entry.getValue().toString();
                                bottomRightX = Double.valueOf(bottomRight.split(",")[0]);
                                bottomRightY = Double.valueOf(bottomRight.split(",")[1]);
                            } else {
                                String topLeft = entry.getValue().toString();
                                topLeftX = Double.valueOf(topLeft.split(",")[0]);
                                topLeftY = Double.valueOf(topLeft.split(",")[1]);
                            }
                            minList.add(bottomRightX);
                            minList.add(topLeftY);
                            maxList.add(topLeftX);
                            maxList.add(bottomRightY);
                            box.add(minList);
                            box.add(maxList);
                        }
                        BasicDBObject geoBox = new BasicDBObject();
                        geoBox.put("$box", box);
                        query.put(where.getField(), new BasicDBObject("$geoWithin", geoBox));
                    } else {
                        LOGGER.error("GEO_BOX的参数无法解析: {}" + where.getParam());
                    }
                    break;
                case GEO_POLYGON:
                    /**
                     * 多边形查询
                     */
                    if (where.getParam() instanceof List) {
                        // [{"lat":"0.151","lon":"254.00"}, {"lat":"1.555","lon":"85.55"}, {"lat":"1.555","lon":"85.55"}],
                        // 将上层应用传过来的数据转成 [[[],[]]]类型
                        List<Map<String, Object>> paramMap = (List<Map<String, Object>>) where.getParam();
                        List<List<Double>> toMongoParamList = new ArrayList<>();
                        for (int i = 0; i < paramMap.size(); i++) {
                            List<Double> xyList = new ArrayList<>();
                            Map<String, Object> map = paramMap.get(i);
                            xyList.add(Double.valueOf((String) map.get("lon")));
                            xyList.add(Double.valueOf((String) map.get("lat")));
                            toMongoParamList.add(xyList);
                        }
                        // 闭合多边形的最后一个点是第一个点，所以要再添加一次
                        List<Double> xyList = new ArrayList<>();
                        Map<String, Object> map = paramMap.get(0);
                        xyList.add(Double.valueOf((String) map.get("lon")));
                        xyList.add(Double.valueOf((String) map.get("lat")));
                        toMongoParamList.add(xyList);

                        List list = new ArrayList();
                        list.add(toMongoParamList);
                        BasicDBObject geometry = new BasicDBObject();
                        geometry.put("type", "Polygon");
                        geometry.put("coordinates", list);
                        BasicDBObject polygon = new BasicDBObject();
                        polygon.put("$geometry", geometry);
                        query.put(where.getField(), new BasicDBObject("$geoWithin", polygon));
                    } else {
                        LOGGER.error("GEO_POLYGON的参数无法解析:" + where.getParam());
                    }
                    break;
                case NOT_NULL:
                    query.put(where.getField(), new BasicDBObject("$ne", null));
                    break;
                case NULL:
                    query.put(where.getField(), new BasicDBObject("$type", 10));
                    break;
                case EXISTS:
                    query.put(where.getField(), new BasicDBObject("$exists", true));
                    break;
                default:
                    Object o = query.get(where.getField());
                    if (o != null) {
                        List<BasicDBObject> fieldParams = new ArrayList<>();
                        BasicDBObject oldBasicDBObject = (BasicDBObject) o;
                        fieldParams.add(oldBasicDBObject);
                        BasicDBObject basicDBObject = new BasicDBObject();
                        fieldParams.add(basicDBObject);
                        if (ItemFieldTypeEnum.DATE.getVal().equals(where.getParamType())) {
                            Date date = JodaTimeUtil.time(where.getParam().toString()).toDate();
                            basicDBObject.append("$" + where.getType().getName(), date);
                        } else {
                            try {
                                if (DateUtil.checkDateString(where.getParam().toString(), JodaTimeUtil.DEFAULT_YMDHMS_FORMAT)) {
                                    Date date = JodaTimeUtil.time(where.getParam().toString()).toDate();
                                    basicDBObject.append("$" + where.getType().getName(), date);
                                } else if (DateUtil.checkDateString(where.getParam().toString(), JodaTimeUtil.DEFAULT_YMD_FORMAT)) {
                                    Date date = JodaTimeUtil.time(where.getParam().toString()).toDate();
                                    basicDBObject.append("$" + where.getType().getName(), date);
                                } else if (DateUtil.checkDateString(where.getParam().toString(), JodaTimeUtil.SOLR_TDATE_RETURN_FORMATE)) {
                                    Date date = JodaTimeUtil.time(where.getParam().toString()).toDate();
                                    basicDBObject.append("$" + where.getType().getName(), date);
                                } else if (DateUtil.checkDateString(where.getParam().toString(), JodaTimeUtil.SOLR_TDATE_FORMATE)) {
                                    Date date = JodaTimeUtil.time(where.getParam().toString()).toDate();
                                    basicDBObject.append("$" + where.getType().getName(), date);
                                } else {
                                    basicDBObject.append("$" + where.getType().getName(), where.getParam());
                                }
                            } catch (Exception e) {
                                basicDBObject.append("$" + where.getType().getName(), where.getParam());
                            }

                        }
                        query.put(where.getField(), fieldParams);
                    } else {
                        BasicDBObject basicDBObject;
                        if (ItemFieldTypeEnum.DATE.getVal().equals(where.getParamType())) {
                            Date date = JodaTimeUtil.time(where.getParam().toString()).toDate();
                            basicDBObject = new BasicDBObject("$" + where.getType().getName(), date);
                        }else if(ItemFieldTypeEnum.STRING.getVal().equals(where.getParamType())) {
                            basicDBObject = new BasicDBObject("$" + where.getType().getName(), where.getParam().toString());
                        }else {
                            try {
                                if (DateUtil.checkDateString(where.getParam().toString(), JodaTimeUtil.DEFAULT_YMDHMS_FORMAT)) {
                                    Date date = DateUtil.convertToDate(where.getParam().toString(), JodaTimeUtil.DEFAULT_YMDHMS_FORMAT);
                                    basicDBObject = new BasicDBObject("$" + where.getType().getName(), date);
                                } else if (DateUtil.checkDateString(where.getParam().toString(), JodaTimeUtil.DEFAULT_YMD_FORMAT)) {
                                    Date date = DateUtil.convertToDate(where.getParam().toString(), JodaTimeUtil.DEFAULT_YMD_FORMAT);
                                    basicDBObject = new BasicDBObject("$" + where.getType().getName(), date);
                                } else if (DateUtil.checkDateString(where.getParam().toString(), JodaTimeUtil.SOLR_TDATE_FORMATE)) {
                                    Date date = DateUtil.convertToDate(where.getParam().toString(), JodaTimeUtil.SOLR_TDATE_FORMATE);
                                    basicDBObject = new BasicDBObject("$" + where.getType().getName(), date);
                                } else if (DateUtil.checkDateString(where.getParam().toString(), JodaTimeUtil.SOLR_TDATE_RETURN_FORMATE)) {
                                    Date date = DateUtil.convertToDate(where.getParam().toString(), JodaTimeUtil.SOLR_TDATE_RETURN_FORMATE);
                                    basicDBObject = new BasicDBObject("$" + where.getType().getName(), date);
                                } else {
                                    basicDBObject = new BasicDBObject("$" + where.getType().getName(), where.getParam());
                                }
                            } catch (Exception e) {
                                basicDBObject = new BasicDBObject("$" + where.getType().getName(), where.getParam());
                            }
                        }
                        query.put(where.getField(), basicDBObject);
                    }
                    break;
            }
        }
    }

    private static void inOrNin(BasicDBObject query, Where where) {
        if (where.getField() == null) {
            throw new RuntimeException("字段不能为空");
        }
        if(!(where.getParam() instanceof List)) {
            throw new RuntimeException("in操作符，参数必须是数组");
        }
        List paramList = (List) where.getParam();
        if (ItemFieldTypeEnum.DATE.getVal().equals(where.getParamType())) {
            List dateList = new ArrayList();
            for (Object param : paramList) {
                Date date = JodaTimeUtil.time(param.toString()).toDate();
                dateList.add(date);
            }
            paramList = dateList;
        }
        BasicDBObject inBdObject = new BasicDBObject();
        query.put(where.getField(), inBdObject);
        inBdObject.put("$" + where.getType().getName(), paramList);
    }

    /**
     * 聚合语法解析
     *
     * @param agg
     * @param aggregateList
     */
    private static void mongoAgg(Aggs agg, List<BasicDBObject> aggregateList) {
        String[] fields = StringUtils.split(agg.getField(), ",");
        String[] aggNames = StringUtils.split(agg.getAggName(), ",");
        AggOpType opType = agg.getType();
        List<AggFunction> aggFunctions = agg.getAggFunctions();
        BasicDBObject group = new BasicDBObject();
        switch (opType) {
            case GROUP:
            case TERMS:
                BasicDBObject aggregate = new BasicDBObject();
                BasicDBObject groupField = new BasicDBObject();
                for (int i = 0; i < fields.length; i++) {
                    String field = fields[i];
                    String aggName = aggNames[i];
                    groupField.append(aggName, "$" + field);
                }
                group.append("_id", groupField);
                group.append("doc_count", new BasicDBObject("$sum", 1));
                mongoAggFunction(aggFunctions, group);
                aggregate.append("$group", group);
                aggregateList.add(aggregate);
                AggHit aggHit = agg.getAggHit();
                if(aggHit != null) {
                    buildAggHits(agg, aggregateList, group, aggHit);
                }
                break;
            case DATE_HISTOGRAM:
                buildDateHistogram(agg, aggregateList);
                break;
            default:
                throw new BusinessException(ExceptionCode.AGG_OP_NOT_SUPPORT_EXCEPTION, opType.getOp());
        }

    }

    private static void buildAggHits(Aggs agg, List<BasicDBObject> aggregateList, BasicDBObject group, AggHit aggHit) {
        // 把文档塞入 data 中，然后再管道中获取
        List<String> select = aggHit.getSelect();
        if (CollectionUtil.isNotEmpty(select)) {
            BasicDBObject push = new BasicDBObject();
            for (String field : select) {
                push.append(field, "$" + field);
            }
            group.put("data", new BasicDBObject("$push", push));
        } else {
            group.put("data", new BasicDBObject("$push", "$$ROOT"));
        }
        // 排序
        List<Order> orderList = aggHit.getOrders();
        if (CollectionUtils.isNotEmpty(orderList)) {
            BasicDBObject orderQuery = new BasicDBObject();
            for (Order order : orderList) {
                orderQuery.append(order.getField(), order.getSort().getName().equals(Sort.ASC.getName()) ? 1 : -1);
            }
            aggregateList.add(new BasicDBObject("$sort", orderQuery));
        }
        Integer size = aggHit.getSize();
        BasicDBObject project = new BasicDBObject();
        project.put("_id", 1);
        project.put("doc_count", 1);
        BasicDBList slice = new BasicDBList();
        slice.add("$data");
        slice.add(size);
        project.put(agg.getHitName() + AGG_HITS_TAG, new BasicDBObject("$slice", slice));
        project.put(agg.getHitName() + AGG_HITS_TOTAL_TAG, new BasicDBObject("$size", "$data"));
        aggregateList.add(new BasicDBObject("$project", project));
    }

    private static void buildDateHistogram(Aggs agg, List<BasicDBObject> aggregateList) {

        AggHit aggHit = agg.getAggHit();

        String aggName = agg.getAggName();
        String field = agg.getField();
        String interval = agg.getInterval();
        String timeZone = agg.getTimeZone();
        BasicDBObject group = new BasicDBObject();
        BasicDBObject _id = new BasicDBObject();
        BasicDBObject dataToString = new BasicDBObject();
        dataToString.put("format", getFormat(interval));
        dataToString.put("date", "$" + field);
        if (StringUtils.isNotBlank(timeZone)) {
            dataToString.put("timezone", timeZone);
        }
        _id.put("$dateToString", dataToString);
        group.put("_id", new BasicDBObject(aggName, _id));
        group.put("doc_count", new BasicDBObject("$sum", 1));

        aggregateList.add(new BasicDBObject("$group", group));

        if(aggHit != null) {
            buildAggHits(agg, aggregateList, group, aggHit);
        }
    }

    private static String getFormat(String interval) {
        if ("year".equalsIgnoreCase(interval)) {
            return "%Y";
        } else if ("month".equalsIgnoreCase(interval)) {
            return "%Y-%m";
        } else if ("day".equalsIgnoreCase(interval)) {
            return "%Y-%m-%d";
        } else if ("hour".equalsIgnoreCase(interval)) {
            return "%Y-%m-%d %H";
        } else if ("minute".equalsIgnoreCase(interval)) {
            return "%Y-%m-%d %H:%M";
        } else if ("SECOND".equalsIgnoreCase(interval)) {
            return "%Y-%m-%d %H:%M:%S";
        } else if (StringUtils.endsWith(interval, "y")) {
            return "%Y";
        } else if (StringUtils.endsWith(interval, "M")) {
            return "%Y-%m";
        } else if (StringUtils.endsWith(interval, "d")) {
            return "%Y-%m-%d";
        } else if (StringUtils.endsWith(interval, "h")) {
            return "%Y-%m-%d %H";
        } else if (StringUtils.endsWith(interval, "m")) {
            return "%Y-%m-%d %H:%M";
        } else if (StringUtils.endsWith(interval, "s")) {
            return "%Y-%m-%d %H:%M:%S";
        }
        throw new RuntimeException("未知的时间间隔：" + interval);
    }

    /**
     * 解析聚合函数
     *
     * @param aggFunctions
     * @param basicDBObject
     */
    private static void mongoAggFunction(List<AggFunction> aggFunctions, BasicDBObject basicDBObject) {
        if (CollectionUtil.isNotEmpty(aggFunctions)) {
            for (int i = 0; i < aggFunctions.size(); i++) {
                AggFunction aggFunction = aggFunctions.get(i);
                if (aggFunction.getAggFunctionType().equals(AggFunctionType.COUNT)) {
                    basicDBObject.append(aggFunction.getFunctionName(), new BasicDBObject("$sum", 1));
                } else {
                    basicDBObject.append(aggFunction.getFunctionName(), new BasicDBObject("$" + aggFunction.getAggFunctionType().getType(), "$" + aggFunction.getField()));
                }
            }
        }
    }

    @Override
    public MongoHandle getCreateDatabaseParam(CreateDatabaseParamCondition paramCondition, MongoDatabaseInfo dataConf) {
        String dbName = paramCondition.getDbName();
        if(StringUtils.isBlank(dbName)) {
            if(StringUtils.isBlank(dbName)) {
                throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "数据库名称不能为空");
            }
        }
        MongoHandle.MongoCreateDatabase mongoCreateDatabase = new MongoHandle.MongoCreateDatabase();
        mongoCreateDatabase.setDbName(dbName);
        MongoHandle mongoHandle = new MongoHandle();
        mongoHandle.setMongoCreateDatabase(mongoCreateDatabase);
        return mongoHandle;
    }

    @Override
    public MongoHandle getDropDatabaseParam(DropDatabaseParamCondition paramCondition, MongoDatabaseInfo dataConf) {
        String dbName = paramCondition.getDbName();
        if(StringUtils.isBlank(dbName)) {
            if(StringUtils.isBlank(dbName)) {
                throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "数据库名称不能为空");
            }
        }
        MongoHandle.MongoDropDatabase mongoDropDatabase = new MongoHandle.MongoDropDatabase();
        mongoDropDatabase.setDbName(dbName);
        MongoHandle mongoHandle = new MongoHandle();
        mongoHandle.setMongoDropDatabase(mongoDropDatabase);
        return mongoHandle;
    }
}

