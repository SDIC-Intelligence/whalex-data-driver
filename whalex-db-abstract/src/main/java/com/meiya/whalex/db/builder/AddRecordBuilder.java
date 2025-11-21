package com.meiya.whalex.db.builder;

import cn.hutool.core.bean.BeanUtil;
import com.meiya.whalex.db.entity.AddParamCondition;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 入库对象构建
 *
 * @author 黄河森
 * @date 2021/7/22
 * @project whalex-data-driver-back
 */
public class AddRecordBuilder {

    private AddParamCondition addParamCondition;

    private AddRecordBuilder() {
        this.addParamCondition = new AddParamCondition();
    }

    public static AddRecordBuilder builder() {
        return new AddRecordBuilder();
    }

    public AddParamCondition build() {
        return this.addParamCondition;
    }

    public AddRecordBuilder add(Map<String, Object> record) {
        this.addParamCondition.addFiledMapToList(record);
        return this;
    }

    public AddRecordBuilder addBatchFromMap(List<Map<String, Object>> recordBatch) {
        this.addParamCondition.addFiledMapListToList(recordBatch);
        return this;
    }

    public <T> AddRecordBuilder add(T record) {
        return add(record, true);
    }

    public <T> AddRecordBuilder add(T record, boolean ignoreNullValue) {
        Map<String, Object> toMap = BeanUtil.beanToMap(record, false, ignoreNullValue);
        return add(toMap);
    }

    public <T> AddRecordBuilder addBatch(List<T> recordBatch) {
        return addBatch(recordBatch, true);
    }

    public <T> AddRecordBuilder addBatch(List<T> recordBatch,  boolean ignoreNullValue) {
        List<Map<String, Object>> collect = recordBatch.stream().flatMap((record) -> Stream.of(BeanUtil.beanToMap(record, false, ignoreNullValue)))
                .collect(Collectors.toList());
        return addBatchFromMap(collect);
    }

    public AddRecordBuilder captureTime(Long captureTime) {
        this.addParamCondition.setCaptureTime(captureTime);
        return this;
    }

    public AddRecordBuilder withCommitNow(Boolean commitNow) {
        this.addParamCondition.setCommitNow(commitNow);
        return this;
    }

    public AddRecordBuilder routingFieldWithElasticSearch(String routingField) {
        this.addParamCondition.setRoutingField(routingField);
        return this;
    }

    public AddRecordBuilder returnFields(List<String> returnFields) {
        this.addParamCondition.setReturnFields(returnFields);
        return this;
    }

    public AddRecordBuilder returnGeneratedKey(boolean returnGeneratedKey) {
        this.addParamCondition.setReturnGeneratedKey(returnGeneratedKey);
        return this;
    }
}
