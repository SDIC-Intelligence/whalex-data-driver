package com.meiya.whalex.db.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.meiya.whalex.interior.db.search.in.Where;
import com.meiya.whalex.interior.db.search.out.FieldEntity;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import javax.validation.constraints.NotBlank;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 更新操作参数实体
 *
 * @author Huanghesen
 * @date 2018/10/11
 * @project whale-common-web
 * @package com.meiya.whale.resource.db.condition
 */
@ApiModel(value = "组件更新数据参数")
public class UpdateParamCondition {

    /**
     * 更新字段
     */
    @ApiModelProperty(value = "更新数据", required = true)
    @NotBlank(message = "更新数据不能为空")
    private List<FieldEntity> fieldEntityList;

    /**
     * 更新字段
     */
    @ApiModelProperty(value = "更新字段", notes = "作为抽象层直接依赖时使用，通过HTTP方式请将参数设置在 fieldEntityList 中", hidden = true)
    private Map<String, Object> updateParamMap;

    /**
     * 查询条件
     */
    @ApiModelProperty(value = "更新条件")
    private List<Where> where;

    /**
     * 当更新记录不存在时，是否插入
     * true 为 不存在时就插入
     * false 为 不存在时不插入（默认）
     */
    @ApiModelProperty(value = "更新操作", notes = "当更新记录不存在时，是否插入")
    @Deprecated
    private Boolean upsert = Boolean.FALSE;

    /**
     * 当有多条匹配的更新行时，是否只更新第一条
     * true 为 全部更新
     * false 为只更新第一天（默认）
     */
    @ApiModelProperty(value = "更新操作", notes = "当有多条匹配的更新行时，是否全部更新")
    private Boolean multi = Boolean.TRUE;

    /**
     * 是否异步操作（默认实时）
     */
    @ApiModelProperty(value = "是否异步操作")
    private Boolean isAsync = Boolean.FALSE;

    @ApiModelProperty(value = "是否立即提交", notes = "默认为false，建议由客户端自主决定")
    private Boolean commitNow = Boolean.FALSE;

    @ApiModelProperty(value = "数组字段更新方式", notes = "默认值：覆盖")
    private ArrayProcessMode arrayProcessMode = ArrayProcessMode.COVER;

    public UpdateParamCondition() {
    }

    public UpdateParamCondition(@NotBlank(message = "更新数据不能为空") List<FieldEntity> fieldEntityList, List<Where> where, Boolean upsert, Boolean multi, Boolean isAsync) {
        this.fieldEntityList = fieldEntityList;
        this.where = where;
        this.upsert = upsert;
        this.multi = multi;
        this.isAsync = isAsync;
    }

    public UpdateParamCondition where(List<Where> where) {
        this.where = where;
        return this;
    }

    public UpdateParamCondition where(Where where) {
        if (this.where == null) {
            this.where = new ArrayList<>();
        }
        this.where.add(where);
        return this;
    }

    public Map<String, Object> getUpdateParamMap() {
        return updateParamMap;
    }

    public void setUpdateParamMap(Map<String, Object> updateParamMap) {
        this.updateParamMap = updateParamMap;
    }

    public List<Where> getWhere() {
        return where;
    }

    public void setWhere(List<Where> where) {
        this.where = where;
    }
    @Deprecated
    public Boolean getUpsert() {
        return upsert;
    }
    @Deprecated
    public void setUpsert(Boolean upsert) {
        this.upsert = upsert;
    }

    public Boolean getMulti() {
        return multi;
    }

    public void setMulti(Boolean multi) {
        this.multi = multi;
    }

    public Boolean getAsync() {
        return isAsync;
    }

    public void setAsync(Boolean async) {
        isAsync = async;
    }

    public List<FieldEntity> getFieldEntityList() {
        return fieldEntityList;
    }

    public void setFieldEntityList(List<FieldEntity> fieldEntityList) {
        this.fieldEntityList = fieldEntityList;
    }

    public Boolean getCommitNow() {
        return commitNow;
    }

    public void setCommitNow(Boolean commitNow) {
        this.commitNow = commitNow;
    }

    public ArrayProcessMode getArrayProcessMode() {
        return arrayProcessMode;
    }

    public void setArrayProcessMode(ArrayProcessMode arrayProcessMode) {
        this.arrayProcessMode = arrayProcessMode;
    }

    /**
     * 更新情况下，数组字段处理方式
     */
    public enum ArrayProcessMode {
        /**
         * 覆盖
         */
        COVER("cover"),
        /**
         * 排重追加
         */
        ADD_TO_SET("addToSet"),
        /**
         * 不排重追加
         */
        ADD_TO_LIST("addToList")
        ;
        private String mode;

        ArrayProcessMode(String mode) {
            this.mode = mode;
        }

        @JsonValue
        public String getMode() {
            return mode;
        }

        @JsonCreator
        public static ArrayProcessMode parse(String mode) {
            if (mode == null) {
                return ArrayProcessMode.COVER;
            }
            for (ArrayProcessMode arrayProcessMode : ArrayProcessMode.values()) {
                if (arrayProcessMode.mode.equalsIgnoreCase(mode)) {
                    return arrayProcessMode;
                }
            }
            return ArrayProcessMode.COVER;
        }

        @Override
        public String toString() {
            return getMode();
        }
    }
}
