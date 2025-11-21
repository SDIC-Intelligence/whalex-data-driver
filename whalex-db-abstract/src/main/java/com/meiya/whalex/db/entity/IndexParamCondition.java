package com.meiya.whalex.db.entity;

import com.meiya.whalex.db.constant.IndexType;
import com.meiya.whalex.interior.db.search.condition.Sort;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

/**
 * 建立索引
 *
 * @author Huanghesen
 * @date 2019/3/5
 */
@Data
@NoArgsConstructor
@ApiModel(value = "组件创建索引参数")
public class IndexParamCondition {

    @ApiModelProperty(value = "索引名称")
    private String indexName;
    @ApiModelProperty(value = "索引类型")
    private IndexType indexType;
    @ApiModelProperty(value = "索引字段名")
    @Deprecated
    private String column;
    @ApiModelProperty(value = "索引字段名集合")
    private List<IndexColumn> columns;
    @ApiModelProperty(value = "索引顺序", allowableValues = "1,-1", notes = "正序索引: 1, 倒排索引: -1")
    @Deprecated
    private String sort;
    @ApiModelProperty(value = "是否异步操作")
    private Boolean async = true;
    @ApiModelProperty(value = "不存在才执行")
    private Boolean isNotExists;
    @ApiModelProperty(value = "属性")
    private Map<String, String> properties;

    @Deprecated
    public IndexParamCondition(String column, String sort, Boolean async) {
        this.column = column;
        this.sort = sort;
        this.async = async;
    }

    @Builder
    public IndexParamCondition(String indexName, IndexType indexType, List<IndexColumn> columns, Boolean async, Boolean isNotExists, Map<String, String> properties) {
        this.indexName = indexName;
        this.indexType = indexType;
        this.columns = columns;
        this.async = async;
        this.isNotExists = isNotExists;
        this.properties = properties;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class IndexColumn {
        private String column;
        private Sort sort;
    }
}
