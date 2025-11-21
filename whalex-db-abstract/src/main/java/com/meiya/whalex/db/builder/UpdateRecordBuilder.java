package com.meiya.whalex.db.builder;

import cn.hutool.core.bean.BeanUtil;
import com.meiya.whalex.db.entity.UpdateParamCondition;
import com.meiya.whalex.interior.db.search.in.Where;

import java.util.Map;

/**
 * 更新记录构建
 *
 * @author 黄河森
 * @date 2021/7/22
 * @project whalex-data-driver-back
 */
public class UpdateRecordBuilder {

    private UpdateParamCondition updateParamCondition;

    private UpdateRecordBuilder() {
        this.updateParamCondition = new UpdateParamCondition();
    }

    public static UpdateRecordBuilder builder() {
        return new UpdateRecordBuilder();
    }

    public UpdateParamCondition build() {
        return this.updateParamCondition;
    }

    public UpdateRecordBuilder update(Map<String, Object> record) {
        this.updateParamCondition.setUpdateParamMap(record);
        return this;
    }

    public UpdateRecordBuilder updateForMap(Map<String, Object> record) {
        this.updateParamCondition.setUpdateParamMap(record);
        return this;
    }

    public <T> UpdateRecordBuilder update(T record, boolean ignoreNullValue) {
        updateForMap(BeanUtil.beanToMap(record, false, ignoreNullValue));
        return this;
    }

    public <T> UpdateRecordBuilder update(T record) {
        update(record, true);
        return this;
    }

    public UpdateRecordBuilder where(Where where) {
        this.updateParamCondition.where(where);
        return this;
    }

    public UpdateRecordBuilder withCommitNow(Boolean commitNow) {
        this.updateParamCondition.setCommitNow(commitNow);
        return this;
    }

    public UpdateRecordBuilder arrayProcessMode(UpdateParamCondition.ArrayProcessMode arrayProcessMode) {
        if (arrayProcessMode != null) {
            this.updateParamCondition.setArrayProcessMode(arrayProcessMode);
        }
        return this;
    }

    public UpdateRecordBuilder withAsync(Boolean async) {
        this.updateParamCondition.setAsync(async);
        return this;
    }
}
