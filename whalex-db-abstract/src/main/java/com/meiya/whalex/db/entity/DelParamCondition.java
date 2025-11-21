package com.meiya.whalex.db.entity;

import com.meiya.whalex.interior.db.search.in.Where;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import java.util.ArrayList;
import java.util.List;

/**
 * 删除操作参数实体
 *
 * @author Huanghesen
 * @date 2018/10/11
 * @project whale-common-web
 * @package com.meiya.whale.resource.db.condition
 */
@ApiModel(value = "组件删除数据参数")
public class DelParamCondition {

    /**
     * 查询条件
     */
    @ApiModelProperty(value = "删除条件")
    private List<Where> where;

    /**
     * 是否异步操作（默认实时）
     */
    @ApiModelProperty(value = "是否异步操作")
    private Boolean isAsync = Boolean.FALSE;

    @ApiModelProperty(value = "是否立即提交", notes = "默认为false，建议由客户端自主决定")
    private Boolean commitNow = Boolean.FALSE;

    public DelParamCondition where(List<Where> where) {
        this.where = where;
        return this;
    }

    public DelParamCondition where(Where where) {
        if (this.where == null) {
            this.where = new ArrayList<>();
        }
        this.where.add(where);
        return this;
    }

    public List<Where> getWhere() {
        return where;
    }

    public void setWhere(List<Where> where) {
        this.where = where;
    }

    public Boolean getAsync() {
        return isAsync;
    }

    public void setAsync(Boolean async) {
        isAsync = async;
    }

    public Boolean getCommitNow() {
        return commitNow;
    }

    public void setCommitNow(Boolean commitNow) {
        this.commitNow = commitNow;
    }
}
