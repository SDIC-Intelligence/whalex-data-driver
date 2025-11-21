package com.meiya.whalex.db.builder;

import cn.hutool.core.bean.BeanUtil;
import com.meiya.whalex.db.entity.UpdateParamCondition;
import com.meiya.whalex.db.entity.UpsertParamCondition;

import java.util.Arrays;
import java.util.Map;

/**
 * 更新OR插入构建
 *
 * @author 黄河森
 * @date 2021/7/22
 * @project whalex-data-driver-back
 */
public class UpsertRecordBuilder {

    private UpsertParamCondition upsertParamCondition;

    private UpsertRecordBuilder() {
        this.upsertParamCondition = new UpsertParamCondition();
    }

    public static UpsertRecordBuilder builder() {
        return new UpsertRecordBuilder();
    }

    public UpsertParamCondition build() {
        return this.upsertParamCondition;
    }

    public UpsertRecordBuilder record(Map<String, Object> record) {
        this.upsertParamCondition.setUpsertParamMap(record);
        return this;
    }

    public UpsertRecordBuilder update(Map<String, Object> record) {
        this.upsertParamCondition.setUpdateParamMap(record);
        return this;
    }

    public <T> UpsertRecordBuilder record(T record) {
        return record(record, true);
    }

    public <T> UpsertRecordBuilder update(T record) {
        return update(record, true);
    }

    public <T> UpsertRecordBuilder record(T record, boolean ignoreNullValue) {
        Map<String, Object> toMap = BeanUtil.beanToMap(record, false, ignoreNullValue);
        return record(toMap);
    }

    public <T> UpsertRecordBuilder update(T record, boolean ignoreNullValue) {
        Map<String, Object> toMap = BeanUtil.beanToMap(record, false, ignoreNullValue);
        return update(toMap);
    }

    public UpsertRecordBuilder conflictField(String... fields) {
        this.upsertParamCondition.setConflictFieldList(Arrays.asList(fields));
        return this;
    }

    public UpsertRecordBuilder captureTime(Long captureTime) {
        this.upsertParamCondition.setCaptureTime(captureTime);
        return this;
    }

    public UpsertRecordBuilder withCommitNow(Boolean commitNow) {
        this.upsertParamCondition.setCommitNow(commitNow);
        return this;
    }

    public UpsertRecordBuilder arrayProcessMode(UpdateParamCondition.ArrayProcessMode arrayProcessMode){
        this.upsertParamCondition.setArrayProcessMode(arrayProcessMode);
        return this;
    }

    public UpsertRecordBuilder withAsync(Boolean async) {
        this.upsertParamCondition.setIsAsync(async);
        return this;
    }
}
