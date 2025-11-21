package com.meiya.whalex.db.entity;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

import java.util.List;
import java.util.Map;

/**
 * 插入 OR 更新
 *
 * @author 黄河森
 * @date 2020/6/18
 * @project whalex-data-driver
 */
@ApiModel(value = "组件插入或更新数据参数")
@Data
public class UpsertParamCondition {

    /**
     * 更新字段
     */
    @ApiModelProperty(value = "新增字段")
    private Map<String, Object> upsertParamMap;

    /**
     * 更新字段
     */
    @ApiModelProperty(value = "更新字段")
    private Map<String, Object> updateParamMap;

    @ApiModelProperty(value = "指定字段名称，当该字段值存在时，则更新，不存在则新增")
    private List<String> conflictFieldList;

    @ApiModelProperty(value = "新增数据时间段", notes = "仅以时间作为分表的组件需要使用该字段")
    private Long captureTime;

    @ApiModelProperty(value = "是否立即提交", notes = "默认为false，建议由服务端自主决定")
    private Boolean commitNow = Boolean.FALSE;

    @ApiModelProperty(value = "数组字段更新方式", notes = "默认值：覆盖")
    private UpdateParamCondition.ArrayProcessMode arrayProcessMode = UpdateParamCondition.ArrayProcessMode.COVER;

    /**
     * 是否异步操作（默认实时）
     */
    @ApiModelProperty(value = "是否异步操作")
    private Boolean isAsync = Boolean.FALSE;
}
