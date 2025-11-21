package com.meiya.whalex.db.entity;

import com.meiya.whalex.interior.db.search.in.QueryParamCondition;
import com.meiya.whalex.util.JsonUtil;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.ToString;

/**
 * API 接口底层服务查询统一入口
 *
 * @author 黄河森
 * @date 2019/9/11
 * @project whale-cloud-platformX
 */
@ApiModel(value = "组件查询接口封装实体", description = "组件查询接口参数信息实体")
@ToString
public class DbQueryEntity extends DbBaseEntity implements Cloneable {

    /**
     * 查询实体
     */
    @ApiModelProperty(value = "查询条件")
    private QueryParamCondition queryParamCondition;

    public QueryParamCondition getQueryParamCondition() {
        if (queryParamCondition == null) {
            queryParamCondition = new QueryParamCondition();
        }
        return queryParamCondition;
    }

    public void setQueryParamCondition(QueryParamCondition queryParamCondition) {
        this.queryParamCondition = queryParamCondition;
    }

    /**
     * 重写Object.clone 实现深度克隆
     *
     * @return
     * @throws CloneNotSupportedException
     */
    @Override
    public Object clone() throws CloneNotSupportedException {
        String objectToStr = JsonUtil.objectToStr(this);
        return JsonUtil.jsonStrToObject(objectToStr, DbQueryEntity.class);
    }

    @Override
    public String toString() {
        return "DbQueryEntity{" +
                "resourceId='" + getResourceId() + '\'' +
                ", dbInfoEntityList=" + getDbInfoEntityList() +
                ", queryParamCondition=" + queryParamCondition +
                '}';
    }
}
