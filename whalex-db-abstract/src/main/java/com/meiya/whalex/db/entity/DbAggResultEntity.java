package com.meiya.whalex.db.entity;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.map.MapUtil;
import lombok.Data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 聚合组件返回结果
 *
 * @author 黄河森
 * @date 2021/1/14
 * @project whalex-data-driver
 */
@Data
public class DbAggResultEntity {

    private List<Aggregations> aggregations;

    public Map<String, Object> toMap() {
        Map<String, Object> resultMap = new HashMap<>(1);
        Map<String, Object> aggregationsMap = new HashMap<>();
        resultMap.put("aggs", aggregationsMap);

        Map<String, Object> map = aggregations.stream().flatMap((aggregation -> Stream.of(aggregation.toMap()))).reduce((a, b) -> {
            a.putAll(b);
            return a;
        }).orElse(new HashMap<>());

        aggregationsMap.put("aggregations", map);
        return resultMap;
    }

    /**
     * Aggregations 结果最外层
     */
    @Data
    public static class Aggregations {

        private String key;

        private List<Bucket> buckets;

        public Map<String, Object> toMap() {
            Map<String, Object> aggregationMap = new HashMap<>(1);
            Map<String, List<Map<String, Object>>> bucketMap = new HashMap<>();
            aggregationMap.put(this.key, bucketMap);
            List<Map<String, Object>> bucketList = new ArrayList<>();
            for (Bucket bucket : buckets) {
                bucketList.add(bucket.toMap());
            }
            bucketMap.put("buckets", bucketList);
            return aggregationMap;
        }
    }

    /**
     * 聚合结果集合
     */
    @Data
    public static class Bucket {
        private Object key;
        private Long docCount;
        private List<Aggregations> aggregations;
        private Map<String, FunVal> funR;
        private Map<String, Hits> hits;

        public Map<String, Object> toMap() {
            Map<String, Object> bucketMap = new HashMap<>(3);
            bucketMap.put("key", this.key);
            bucketMap.put("doc_count", this.docCount);
            if (CollectionUtil.isNotEmpty(aggregations)) {
                for (Aggregations aggregation : aggregations) {
                    bucketMap.putAll(aggregation.toMap());
                }
            }
            if (CollectionUtil.isNotEmpty(funR)) {
                Map<String, Map<String, Object>> funMap = this.funR.entrySet().stream()
                        .flatMap(entry -> {
                            Map<String, Map<String, Object>> value = MapUtil.builder(entry.getKey(), MapUtil.builder("value", entry.getValue().getValue()).build()).build();
                            return Stream.of(value);
                        }).reduce((a, b) -> {
                            a.putAll(b);
                            return a;
                        }).orElse(new HashMap<>());
                bucketMap.putAll(funMap);
            }
            if (CollectionUtil.isNotEmpty(hits)) {
                for (Map.Entry<String, Hits> entry : hits.entrySet()) {
                    Hits value = entry.getValue();
                    bucketMap.put(entry.getKey(), MapUtil.builder("hits", MapUtil.builder().put("total", value.getTotal()).put("hits", value.getHits()).build()).build());
                }
            }
            return bucketMap;
        }
    }

    /**
     * 聚合函数结果对象
     */
    @Data
    public static class FunVal {
        private Object value;
    }

    @Data
    public static class Hits {
        private long total;
        private List<Map<String, Object>> hits;
    }

}
