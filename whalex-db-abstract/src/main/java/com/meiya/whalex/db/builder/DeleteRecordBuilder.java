package com.meiya.whalex.db.builder;

import com.meiya.whalex.db.entity.DelParamCondition;
import com.meiya.whalex.interior.db.search.in.Where;

/**
 * 删除记录构建
 *
 * @author 黄河森
 * @date 2021/7/22
 * @project whalex-data-driver-back
 */
public class DeleteRecordBuilder {

    private DelParamCondition delParamCondition;

    private DeleteRecordBuilder() {
        this.delParamCondition = new DelParamCondition();
    }

    public static DeleteRecordBuilder builder() {
        return new DeleteRecordBuilder();
    }

    public DelParamCondition build() {
        return this.delParamCondition;
    }

    public DeleteRecordBuilder where(Where where) {
        this.delParamCondition.where(where);
        return this;
    }

    public DeleteRecordBuilder commitNow(boolean commitNow) {
        this.delParamCondition.setCommitNow(commitNow);
        return this;
    }
}
