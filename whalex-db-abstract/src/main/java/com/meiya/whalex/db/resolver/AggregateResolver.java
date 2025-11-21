package com.meiya.whalex.db.resolver;

import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.convert.impl.BeanConverter;
import cn.hutool.core.map.MapUtil;
import com.meiya.whalex.db.entity.PageResult;
import com.meiya.whalex.exception.BusinessException;
import lombok.Builder;
import lombok.Data;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author 黄河森
 * @date 2021/6/22
 * @project whalex-data-driver-back
 */
public class AggregateResolver {

    private PageResult pageResult;

    private AggregateResolver(PageResult pageResult) {
        this.pageResult = pageResult;
    }

    public static AggregateResolver resolver(PageResult pageResult) {
        return new AggregateResolver(pageResult);
    }

    public Aggregations analysis() {
        if (!this.pageResult.getSuccess()) {
            throw new BusinessException(this.pageResult.getCode(), this.pageResult.getMessage());
        }
        Aggregations.AggregationsBuilder builder = Aggregations.builder();
        List<Map<String, Object>> rows = this.pageResult.getRows();
        if (CollectionUtil.isNotEmpty(rows)) {
            Map<String, Object> aggMap = rows.get(0);
            Map<String, Object> aggs = (Map<String, Object>) aggMap.get("aggs");
            Map<String, Object> aggregationMap = (Map<String, Object>) aggs.get("aggregations");
            List<Aggregation> aggregations = getAggregations(aggregationMap);
            return builder.aggregations(aggregations).build();
        } else {
            return builder.build();
        }
    }

    private List<Aggregation> getAggregations(Map<String, Object> aggregationMap) {
        List<Aggregation> aggregations = new ArrayList<>();
        aggregationMap.forEach((k, v) -> {
            Map<String, Object> valueMap = (Map<String, Object>) v;
            List<Map<String, Object>> buckets = (List<Map<String, Object>>) valueMap.get("buckets");
            if (buckets != null) {
                List<Bucket> bucketList = parseBucket(buckets);
                // 获取是否有对应的hit
                Object hit = aggregationMap.get(k + "_hit");
                DataRowResolver.DataRow<Map<String, Object>> dataRow = new DataRowResolver.DataRow<>();
                if (hit != null) {
                    Map<String, Object> hits = (Map<String, Object>) ((Map<String, Object>) hit).get("hits");
                    Object _total = hits.get("total");
                    Long total = null;
                    if (_total != null) {
                        if (_total instanceof Map) {
                            Map<String, Object> _totalMap = (Map<String, Object>) _total;
                            Object _value = _totalMap.get("value");
                            total = _value == null ? null : Long.parseLong(String.valueOf(_value));
                        } else {
                            total = Long.parseLong(String.valueOf(hits.get("total")));
                        }
                    }
                    List<Map<String, Object>> hitRecords = (List<Map<String, Object>>) hits.get("hits");
                    dataRow.setTotal(total);
                    dataRow.setData(hitRecords);
                }
                aggregations.add(Aggregation.builder().aggName(k).buckets(bucketList).hitRecord(dataRow).build());
            } else {
                Object value = valueMap.get("value");
                if (value != null) {
                    aggregations.add(Aggregation.builder().aggName(k).value(value).build());
                }
            }
        });
        return aggregations;
    }

    /**
     * 解析桶数据
     *
     * @param buckets
     * @return
     */
    private List<Bucket> parseBucket(List<Map<String, Object>> buckets) {
        List<Bucket> bucketList = buckets.stream().flatMap(bucketMap -> {
            Bucket.BucketBuilder bucketBuilder = Bucket.builder()
                    .key(bucketMap.remove("key"));
            Object docCount = bucketMap.remove("doc_count");
            if (docCount != null) {
                bucketBuilder.docCount(Long.parseLong(docCount.toString()));
            }
            Map<String, FunVal> funR = new LinkedHashMap<>();
            List<Aggregation> subAggregation = new ArrayList<>();
            DataRowResolver.DataRow<Map<String, Object>> dataRow = new DataRowResolver.DataRow<>();
            bucketMap.forEach((key, value) -> {
                if (value instanceof Map) {
                    Map<String, Object> valueMap = (Map<String, Object>) value;
                    if (valueMap.get("buckets") != null) {
                        subAggregation.addAll(getAggregations(MapUtil.builder(key, value).build()));
                    } else if (valueMap.get("hits") == null) {
                        funR.put(key, FunVal.builder().value(valueMap.get("value")).build());
                    } else {
                        // 当前为 topHit 记录
                        Map<String, Object> hits = (Map<String, Object>) valueMap.get("hits");
                        Object _total = hits.get("total");
                        Long total = null;
                        if (_total != null) {
                            if (_total instanceof Map) {
                                Map<String, Object> _totalMap = (Map<String, Object>) _total;
                                Object _value = _totalMap.get("value");
                                total = _value == null ? null : Long.parseLong(String.valueOf(_value));
                            } else {
                                total = Long.parseLong(String.valueOf(hits.get("total")));
                            }
                        }
                        List<Map<String, Object>> hitRecords = (List<Map<String, Object>>) hits.get("hits");
                        dataRow.setTotal(total);
                        dataRow.setData(hitRecords);
                    }
                }
            });
            return Stream.of(bucketBuilder.aggregations(subAggregation).funR(funR).hitRecord(dataRow).build());
        }).collect(Collectors.toList());
        return bucketList;
    }

    private <T> DataRowResolver.DataRow<T> analysis(List<Map<String, Object>> records, Class<T> tClass) throws Exception {
        List<T> collect = records.stream().flatMap(row -> {
            BeanConverter<T> converter = new BeanConverter<>(tClass, CopyOptions.create().setIgnoreError(false));
            T convert = converter.convert(row, null);
            return Stream.of(convert);
        }).collect(Collectors.toList());
        DataRowResolver.DataRow<T> dataRow = new DataRowResolver.DataRow<>();
        dataRow.setData(collect);
        return dataRow;
    }

    @Data
    @Builder
    public static class Aggregations {
        private List<Aggregation> aggregations;
    }

    @Data
    @Builder
    public static class Aggregation {
        private String aggName;
        private List<Bucket> buckets;
        private DataRowResolver.DataRow hitRecord;
        private Object value;
    }

    /**
     * 聚合结果集合
     */
    @Data
    @Builder
    public static class Bucket {
        private Object key;
        private Long docCount;
        private List<Aggregation> aggregations;
        private Map<String, FunVal> funR;
        private DataRowResolver.DataRow hitRecord;
    }

    /**
     * 聚合函数结果对象
     */
    @Data
    @Builder
    public static class FunVal {
        private Object value;
    }
}
