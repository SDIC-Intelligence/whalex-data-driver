package com.meiya.whalex.interior.db.operation.in;

import com.meiya.whalex.interior.base.BaseQuery;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import java.util.List;
import java.util.Map;

/**
 * 索引创建请求报文
 *
 * @author 黄河森
 * @date 2019/11/28
 * @project whale-cloud-platformX
 */
@ApiModel("索引创建请求报文")
public class CreateIndexOperation extends BaseQuery {

    @ApiModelProperty(value = "资源标识符")
    private String resourceId;

    @ApiModelProperty(value = "组件类型")
    private String dbType;

    @ApiModelProperty(value = "数据库表标识符")
    private String dbId;

    @ApiModelProperty(value = "索引字段名")
    @Deprecated
    private String column;

    @ApiModelProperty(value = "索引字段名集合")
    private List<String> columns;

    @ApiModelProperty(value = "索引顺序集合", allowableValues = "1,-1", notes = "正序索引: 1, 倒排索引: -1")
    private List<String> sorts;

    @ApiModelProperty(value = "索引名称")
    private String indexName;

    @ApiModelProperty(value = "索引类型")
    private String indexType;

    @ApiModelProperty(value = "索引顺序", allowableValues = "1,-1", notes = "正序索引: 1, 倒排索引: -1")
    @Deprecated
    private String sort;

    @ApiModelProperty(value = "是否异步操作", allowableValues = "true, false")
    private Boolean async = true;

    @ApiModelProperty(value = "属性")
    private Map<String, String> properties;

    public String getResourceId() {
        return resourceId;
    }

    public void setResourceId(String resourceId) {
        this.resourceId = resourceId;
    }

    public String getDbType() {
        return dbType;
    }

    public void setDbType(String dbType) {
        this.dbType = dbType;
    }

    public String getDbId() {
        return dbId;
    }

    public void setDbId(String dbId) {
        this.dbId = dbId;
    }

    public String getColumn() {
        return column;
    }

    public void setColumn(String column) {
        this.column = column;
    }

    public String getSort() {
        return sort;
    }

    public void setSort(String sort) {
        this.sort = sort;
    }

    public Boolean getAsync() {
        return async;
    }

    public void setAsync(Boolean async) {
        this.async = async;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public List<String> getColumns() {
        return columns;
    }

    public void setColumns(List<String> columns) {
        this.columns = columns;
    }

    public List<String> getSorts() {
        return sorts;
    }

    public void setSorts(List<String> sorts) {
        this.sorts = sorts;
    }

    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public String getIndexType() {
        return indexType;
    }

    public void setIndexType(String indexType) {
        this.indexType = indexType;
    }
}
