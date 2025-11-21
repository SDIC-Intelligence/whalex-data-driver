package com.meiya.whalex.util;

import cn.hutool.core.map.MapUtil;
import cn.hutool.core.util.ObjectUtil;
import com.meiya.whalex.db.entity.DbAggResultEntity;
import com.meiya.whalex.interior.db.search.in.Aggs;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author zlx
 * @date 2021/2/23
 * @project whalex-data-driver
 */
public class AggResultTranslateUtil {

    /**
     * 将数据处理成标准的输出格式
     * {
     * "requestNum": {
     * "value": 1804
     * },
     * "failNum": {
     * "value": 58
     * },
     * "successNum": {
     * "value": 1746
     * },
     * "maxCost": {
     * "value": 4688871
     * },
     * "avgTriesNum": {
     * "value": 1823
     * },
     * "avgCost": {
     * "value": 6813.920373713438
     * },
     * "minCost": {
     * "value": 964107
     * }
     * }
     *
     * @param aggResult
     * @return
     */
    public static List<Map<String, Object>> translateMongodbAggFunR(List<Map<String, Object>> aggResult) {
        if (CollectionUtils.isNotEmpty(aggResult)) {
            List<Map<String, Object>> translateResultList = new ArrayList<>();
            Iterator<Map<String, Object>> iterator = aggResult.iterator();
            while (iterator.hasNext()) {
                Map<String, Object> next = iterator.next();
                Map<String, Object> translateResult = new HashMap<>();
                translateResultList.add(translateResult);
                Map<String, DbAggResultEntity.FunVal> funR = new HashMap<>();
                translateResult.put("_id", next.get("_id"));
                translateResult.put("_result", funR);
                next.remove("_id");
                for (Map.Entry<String, Object> entry : next.entrySet()) {
                    String key = entry.getKey();
                    Object value = entry.getValue();
                    DbAggResultEntity.FunVal funVal = new DbAggResultEntity.FunVal();
                    funVal.setValue(value);
                    funR.put(key, funVal);
                }
            }
            return translateResultList;
        }
        return Collections.EMPTY_LIST;
    }

    private static void getMongodbAggResult(List<String> keyNameList, List<Map<String, Object>> aggResult, List<DbAggResultEntity.Aggregations> aggregations) {
        if (CollectionUtils.isNotEmpty(keyNameList)) {
            // 处理最外层
            DbAggResultEntity.Aggregations aggregation = new DbAggResultEntity.Aggregations();
            List<DbAggResultEntity.Bucket> bucketList = new ArrayList<>();
            aggregations.add(aggregation);
            String key = keyNameList.get(0);
            aggregation.setKey(key);
            aggregation.setBuckets(bucketList);
            // 处理里层的
            createMongodbBucket(keyNameList, aggResult, bucketList);
        }
    }

    public static void createMongodbBucket(List<String> keyNameList, List<Map<String, Object>> aggResult, List<DbAggResultEntity.Bucket> bucketList) {
        if (keyNameList.size() == 1) {
            for (Map<String, Object> map : aggResult) {
                String keyName = keyNameList.get(0);
                Object keyValue = map.get(keyName);
                map.remove(keyName);

                DbAggResultEntity.Bucket bucket = new DbAggResultEntity.Bucket();
                bucketList.add(bucket);
                bucket.setKey(keyValue);
                Map<String, DbAggResultEntity.FunVal> funValMap = (Map<String, DbAggResultEntity.FunVal>) map.get("_result");
                DbAggResultEntity.FunVal doc_count = funValMap.get("doc_count");
                if (doc_count != null) {
                    bucket.setDocCount(Long.parseLong(doc_count.getValue().toString()));
                    funValMap.remove("doc_count");
                }
                bucket.setFunR(funValMap);
            }
        } else {
            String keyName = keyNameList.remove(0);
            Set<String> keyValueList = aggResult.stream().map(each -> (String) each.get(keyName)).collect(Collectors.toSet());
            for (String keyValue : keyValueList) {
                List<Map<String, Object>> subAggResults = aggResult.stream().filter(r -> r.containsKey(keyName) && ((String) r.get(keyName)).equals(keyValue)).collect(Collectors.toList());
                DbAggResultEntity.Bucket bucket = new DbAggResultEntity.Bucket();
                List<DbAggResultEntity.Bucket> subBucketList = new ArrayList<>();
                bucketList.add(bucket);
                bucket.setKey(keyValue);
                List<String> subKeyNameList = new ArrayList<>(keyNameList);
                createMongodbBucket(subKeyNameList, subAggResults, subBucketList);
            }
        }
    }

    /**
     * mongodb 聚合结果解析
     *
     * @param aggResult
     * @return
     */
    public static DbAggResultEntity getMongodbAggResult(List<Map<String, Object>> aggResult) {
        DbAggResultEntity dbAggResultEntity = new DbAggResultEntity();
        List<DbAggResultEntity.Aggregations> aggregations = new ArrayList<>();
        dbAggResultEntity.setAggregations(aggregations);
        List<String> keyNameList = null;
        for (Map<String, Object> map : aggResult) {
            Map<String, Object> keyMap = (Map<String, Object>) map.get("_id");
            if (CollectionUtils.isEmpty(keyNameList)) {
                keyNameList = new ArrayList<>(keyMap.keySet());
            }
            map.remove("_id");
            map.putAll(keyMap);
        }
        getMongodbAggResult(keyNameList, aggResult, aggregations);
        return dbAggResultEntity;
    }

    public static List<Map<String, Object>> getMongodbTreeAggResult(List<Map<String, Object>> rows) {
        DbAggResultEntity aggResultEntity = new DbAggResultEntity();
        List<DbAggResultEntity.Aggregations> aggregations = new ArrayList<>();
        aggResultEntity.setAggregations(aggregations);
        Map<Object, Object> aggValToMap = new HashMap<>();
        int key = parserMongodbAggValToMap(rows, aggValToMap);
        // 解析分组聚合 或者 函数聚合
        List<Map<String, Object>> list = new ArrayList<>();
        if (key == 1 && aggValToMap.containsKey("__function")) {
            // 函数聚合
            Map<String, Map<String, Object>> function = (Map<String, Map<String, Object>>) aggValToMap.get("__function");
            Map<String, Object> functionMap = function.values().iterator().next();
            Map<String, Map<String, Object>> _functionMap = new HashMap<>(functionMap.size());
            for (Map.Entry<String, Object> entry : functionMap.entrySet()) {
                _functionMap.put(entry.getKey(), MapUtil.builder("value", entry.getValue()).build());
            }
            Map<String, Object> resultMap = new HashMap<>(1);
            Map<String, Object> aggregationsMap = new HashMap<>();
            resultMap.put("aggs", aggregationsMap);
            aggregationsMap.put("aggregations", _functionMap);
            list.add(resultMap);
        } else {
            // 分组聚合
            assembleRelationDataAggResult(aggregations, aggValToMap, key * 2, 1);
            list.add(aggResultEntity.toMap());
        }

        return list;
    }

    public static int parserMongodbAggValToMap(List<Map<String, Object>> rows, Map<Object, Object> aggValToMap) {
        int key = 0;
        Map<Object, Object> value;
        for (Map<String, Object> row : rows) {
            value = aggValToMap;
            Map<String, Object> aggKeyMap = (Map<String, Object>) row.remove("_id");
            key = aggKeyMap.size();

            for (Map.Entry<String, Object> entry : aggKeyMap.entrySet()) {
                Map<Object, Object> keyMap = (Map<Object, Object>) value.get(entry.getKey());
                if (keyMap == null) {
                    keyMap = new HashMap<>();
                    value.put(entry.getKey(), keyMap);
                }
                value = keyMap;
                Map<Object, Object> subMap = (Map<Object, Object>) value.get(entry.getValue());
                if (subMap == null) {
                    subMap = new HashMap<>();
                    value.put(entry.getValue(), subMap);
                }
                value = subMap;
            }
            value.putAll(row);
        }
        return key;
    }

    /**
     * 关系型数据库聚合查询结果组装
     *
     * @param rows
     * @param keys
     * @return
     */
    public static List<Map<String, Object>> getRelationDataBaseTreeAggResult(List<Map<String, Object>> rows, List<String> keys) {
        DbAggResultEntity aggResultEntity = new DbAggResultEntity();
        List<DbAggResultEntity.Aggregations> aggregations = new ArrayList<>();
        aggResultEntity.setAggregations(aggregations);
        Map<Object, Object> aggValToMap = parserRelationDataAggValToMap(keys, rows);
        assembleRelationDataAggResult(aggregations, aggValToMap, keys.size() * 2, 1);
        List<Map<String, Object>> list = new ArrayList<>();
        list.add(aggResultEntity.toMap());
        return list;
    }

    /**
     * 解析关系型数据库聚合key
     *
     * @param agg
     * @param keys
     */
    public static void parserRelationDataAggKey(Aggs agg, List<String> keys) {
        keys.add(agg.getAggName());
        List<Aggs> aggList = agg.getAggList();
        if (CollectionUtils.isNotEmpty(aggList)) {
            parserRelationDataAggKey(aggList.get(0), keys);
        }
    }

    /**
     * 组装聚合结果为层级MAP
     *
     * @param keys
     * @param rows
     * @return
     */
    public static Map<Object, Object> parserRelationDataAggValToMap(List<String> keys, List<Map<String, Object>> rows) {
        Map<Object, Object> result = new LinkedHashMap<>();
        Map<Object, Object> value;

        for (int i = 0; i < rows.size(); i++) {
            value = result;
            Map<String, Object> row = rows.get(i);
            for (int t = 0; t < keys.size(); t++) {
                String key = keys.get(t);
                Map<Object, Object> keyValue = (Map<Object, Object>) value.get(key);
                if (keyValue == null) {
                    keyValue = new LinkedHashMap<>();
                    value.put(key, keyValue);
                }
                value = keyValue;
                Object val = row.remove(key);
                Map<Object, Object> subValue = (Map<Object, Object>) value.get(val);
                if (subValue == null) {
                    subValue = new LinkedHashMap<>();
                    value.put(val, subValue);
                }
                value = subValue;
            }
            if (row.size() > 0) {
                value.putAll(row);
            }
        }
        return result;
    }

    /**
     * 解析聚合 aggName 部分
     *
     * @param aggregationList
     * @param aggValToMap
     * @param hierarchy
     * @param current
     */
    public static void assembleRelationDataAggResult(List<DbAggResultEntity.Aggregations> aggregationList, Map<Object, Object> aggValToMap, int hierarchy, int current) {
        for (Map.Entry<Object, Object> entry : aggValToMap.entrySet()) {
            DbAggResultEntity.Aggregations aggregation = new DbAggResultEntity.Aggregations();
            aggregationList.add(aggregation);
            aggregation.setKey((String) entry.getKey());
            Map<Object, Object> value = (Map<Object, Object>) entry.getValue();
            List<DbAggResultEntity.Bucket> bucketList = new ArrayList<>();
            aggregation.setBuckets(bucketList);
            assembleRelationDataBucket(bucketList, value, hierarchy, current + 1);
        }
    }

    /**
     * 解析聚合值部分
     *
     * @param bucketList
     * @param aggValToMap
     * @param hierarchy
     * @param current
     */
    public static void assembleRelationDataBucket(List<DbAggResultEntity.Bucket> bucketList, Map<Object, Object> aggValToMap, int hierarchy, int current) {
        for (Map.Entry<Object, Object> entry : aggValToMap.entrySet()) {
            DbAggResultEntity.Bucket bucket = new DbAggResultEntity.Bucket();
            bucketList.add(bucket);
            bucket.setKey(entry.getKey());
            Map<Object, Object> value = (Map<Object, Object>) entry.getValue();
            if (hierarchy == current) {
                Object docCount = value.get("doc_count");
                if (docCount != null) {
                    bucket.setDocCount(Long.parseLong(docCount.toString()));
                }
                if ((docCount == null && value.size() >= 1) || (docCount != null && value.size() > 1)) {
                    Map<String, DbAggResultEntity.FunVal> funR = new HashMap<>();
                    Map<String, DbAggResultEntity.Hits> hitsMap = new HashMap<>();
                    for (Map.Entry<Object, Object> objectEntry : value.entrySet()) {
                        Object key = objectEntry.getKey();
                        if (!ObjectUtil.equal(key, "doc_count")) {
                            Object entryValue = objectEntry.getValue();
                            if (StringUtils.contains(String.valueOf(key), "__HITS") && entryValue instanceof List) {
                                String hitsKey = StringUtils.substringBefore(String.valueOf(key), "__HITS");
                                DbAggResultEntity.Hits hitsV = hitsMap.get(hitsKey);
                                if (hitsV == null) {
                                    hitsV = new DbAggResultEntity.Hits();
                                    hitsMap.put(hitsKey, hitsV);
                                }
                                hitsV.setHits((List) entryValue);
                                continue;
                            }
                            if (StringUtils.contains(String.valueOf(key), "__HITS_TOTAL") && StringUtils.isNumeric(String.valueOf(entryValue))) {
                                String hitsKey = StringUtils.substringBefore(String.valueOf(key), "__HITS_TOTAL");
                                DbAggResultEntity.Hits hitsV = hitsMap.get(hitsKey);
                                if (hitsV == null) {
                                    hitsV = new DbAggResultEntity.Hits();
                                    hitsMap.put(hitsKey, hitsV);
                                }
                                hitsV.setTotal(Long.parseLong(String.valueOf(entryValue)));
                                continue;
                            }
                            DbAggResultEntity.FunVal funVal = new DbAggResultEntity.FunVal();
                            funVal.setValue(entryValue);
                            funR.put((String) objectEntry.getKey(), funVal);
                        }
                    }
                    bucket.setFunR(funR);
                    bucket.setHits(hitsMap);
                }
            } else {
                List<DbAggResultEntity.Aggregations> aggregationList = new ArrayList<>();
                bucket.setAggregations(aggregationList);
                assembleRelationDataAggResult(aggregationList, value, hierarchy, current + 1);
            }
        }
    }

}
