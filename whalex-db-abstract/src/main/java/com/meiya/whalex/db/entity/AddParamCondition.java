package com.meiya.whalex.db.entity;

import com.meiya.whalex.interior.db.search.out.FieldEntity;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import org.apache.commons.collections.CollectionUtils;

import javax.validation.constraints.NotBlank;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 统一对外插入参数封装实体
 *
 * @author Huanghesen
 * @date 2018/10/10
 * @project whale-common-web
 * @package com.meiya.whale.resource.db.condition
 */
@ApiModel(value = "组件新增数据参数")
public class AddParamCondition {

    /**
     * 新增参数
     */
    @ApiModelProperty(value = "新增数据", required = true)
    @NotBlank(message = "新增数据不能为空")
    private List<List<FieldEntity>> fieldEntityList;

    @ApiModelProperty(value = "新增数据", notes = "作为抽象层直接依赖时使用，通过HTTP方式请将参数设置在 fieldEntityList 中", hidden = true)
    private List<Map<String, Object>> fieldValueList;

    /**
     * solr 特殊参数
     */
    @ApiModelProperty(value = "新增数据时间段", notes = "仅以时间作为分表的组件需要使用该字段")
    private Long captureTime;

    /**
     * 是否异步操作（默认实时）
     */
    @ApiModelProperty(value = "操作是否异步")
    private Boolean isAsync = Boolean.FALSE;

    @ApiModelProperty(value = "是否立即提交", notes = "默认为false，建议由客户端自主决定")
    private Boolean commitNow = Boolean.FALSE;

    @ApiModelProperty(value = "是否允许插入覆盖已存在的数据", notes = "默认为true")
    private boolean overWrite = Boolean.TRUE;

    @ApiModelProperty(value = "es组件文档存储或者查询得路由字段")
    private String routingField;

    @ApiModelProperty(value = "是否返回自增主键值")
    private boolean returnGeneratedKey = Boolean.FALSE;

    @ApiModelProperty(value = "新增之后返回的字段")
    private List<String> returnFields;

    public List<Map<String, Object>> getFieldValueList() {
        return fieldValueList;
    }

    public void setFieldValueList(List<Map<String, Object>> fieldValueList) {
        this.fieldValueList = fieldValueList;
    }

    public String getRoutingField() {
        return routingField;
    }

    public void setRoutingField(String routingField) {
        this.routingField = routingField;
    }

    /**
     * 构造一条插入记录key - value
     *
     * @param fieldCount
     * @return
     */
    public Map<String, Object> createFiledMap(int fieldCount) {
        if (fieldCount == 0) {
            fieldCount = 16;
        }
        Map<String, Object> filedMap = new LinkedHashMap<>(fieldCount);
        return filedMap;
    }

    /**
     * 将一行添加记录插入了批量集合中
     * @param fieldMap
     */
    public void addFiledMapToList(Map<String, Object> fieldMap) {
        if (CollectionUtils.isEmpty(fieldValueList)) {
            fieldValueList = new ArrayList<>();
        }
        fieldValueList.add(fieldMap);
    }
    
    /**
     * 将一行添加记录插入了批量集合中
     * @param fieldMapList
     */
    public void addFiledMapListToList(List<Map<String, Object>> fieldMapList) {
        if (CollectionUtils.isNotEmpty(fieldMapList)) {
            if (CollectionUtils.isEmpty(fieldValueList)) {
                fieldValueList = new ArrayList<>();
            }
            fieldValueList.addAll(fieldMapList);
        }
    }

    public List<List<FieldEntity>> getFieldEntityList() {
        return fieldEntityList;
    }

    public void setFieldEntityList(List<List<FieldEntity>> fieldEntityList) {
        this.fieldEntityList = fieldEntityList;
    }

    public Long getCaptureTime() {
        return captureTime;
    }

    public void setCaptureTime(Long captureTime) {
        this.captureTime = captureTime;
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

    public boolean isOverWrite() {
        return overWrite;
    }

    public void setOverWrite(boolean overWrite) {
        this.overWrite = overWrite;
    }

    public boolean isReturnGeneratedKey() {
        return returnGeneratedKey;
    }

    public void setReturnGeneratedKey(boolean returnGeneratedKey) {
        this.returnGeneratedKey = returnGeneratedKey;
    }

    public List<String> getReturnFields() {
        return returnFields;
    }

    public void setReturnFields(List<String> returnFields) {
        this.returnFields = returnFields;
    }

    public AddParamCondition() {
    }

    public AddParamCondition(@NotBlank(message = "新增数据不能为空") List<List<FieldEntity>> fieldEntityList, Long captureTime, Boolean isAsync, Boolean commitNow, boolean overWrite) {
        this.fieldEntityList = fieldEntityList;
        this.captureTime = captureTime;
        this.isAsync = isAsync;
        this.commitNow = commitNow;
        this.overWrite = overWrite;
    }
}
