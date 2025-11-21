package com.meiya.whalex.db.builder;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.ArrayUtil;
import com.meiya.whalex.db.entity.UpdateParamCondition;
import com.meiya.whalex.db.entity.UpsertParamBatchCondition;
import org.apache.commons.collections.ListUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * 更新OR插入构建
 *
 * @author 黄河森
 * @date 2021/7/22
 * @project whalex-data-driver-back
 */
public class UpsertRecordBatchBuilder {

    private UpsertParamBatchCondition upsertParamBatchCondition;

    private UpsertRecordBatchBuilder() {
        this.upsertParamBatchCondition = new UpsertParamBatchCondition();
    }

    public static UpsertRecordBatchBuilder builder() {
        return new UpsertRecordBatchBuilder();
    }

    public UpsertParamBatchCondition build() {
        return this.upsertParamBatchCondition;
    }

    public UpsertRecordBatchBuilder record(Map<String, Object> record) {
        List<Map<String, Object>> upsertParamList = this.upsertParamBatchCondition.getUpsertParamList();
        if (upsertParamList == null) {
            upsertParamList = new ArrayList<>();
            this.upsertParamBatchCondition.setUpsertParamList(upsertParamList);
        }
        upsertParamList.add(record);
        return this;
    }

    public UpsertRecordBatchBuilder addRecordList(List<Map<String, Object>> recordList) {
        List<Map<String, Object>> upsertParamList = this.upsertParamBatchCondition.getUpsertParamList();
        if (upsertParamList == null) {
            upsertParamList = new ArrayList<>();
            this.upsertParamBatchCondition.setUpsertParamList(upsertParamList);
        }
        upsertParamList.addAll(recordList);
        return this;
    }

    public <T> UpsertRecordBatchBuilder record(T record) {
        return record(record, true);
    }

    public <T> UpsertRecordBatchBuilder record(T record, boolean ignoreNullValue) {
        Map<String, Object> toMap = BeanUtil.beanToMap(record, false, ignoreNullValue);
        return record(toMap);
    }

    public <T> UpsertRecordBatchBuilder updateKey(String... keys) {
        this.upsertParamBatchCondition.setUpdateKeys(new ArrayList<>(Arrays.asList(keys)));
        return this;
    }

    public UpsertRecordBatchBuilder conflictField(String... fields) {
        this.upsertParamBatchCondition.setConflictFieldList(Arrays.asList(fields));
        return this;
    }

    public UpsertRecordBatchBuilder captureTime(Long captureTime) {
        this.upsertParamBatchCondition.setCaptureTime(captureTime);
        return this;
    }

    public UpsertRecordBatchBuilder withCommitNow(Boolean commitNow) {
        this.upsertParamBatchCondition.setCommitNow(commitNow);
        return this;
    }

    public UpsertRecordBatchBuilder arrayProcessMode(UpdateParamCondition.ArrayProcessMode arrayProcessMode){
        this.upsertParamBatchCondition.setArrayProcessMode(arrayProcessMode);
        return this;
    }

    public UpsertRecordBatchBuilder withAsync(Boolean async) {
        this.upsertParamBatchCondition.setIsAsync(async);
        return this;
    }
}
