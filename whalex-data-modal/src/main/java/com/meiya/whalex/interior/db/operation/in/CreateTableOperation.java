package com.meiya.whalex.interior.db.operation.in;

import com.meiya.whalex.interior.base.BaseQuery;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * 创建表请求报文
 *
 * @author 黄河森
 * @date 2019/12/3
 * @project whale-cloud-platformX
 */
@ApiModel("数据库表创建请求报文")
public class CreateTableOperation extends BaseQuery {

    @ApiModelProperty(value = "资源标识符")
    private String resourceId;

    @ApiModelProperty(value = "组件类型")
    private String dbType;

    @ApiModelProperty(value = "数据库表标识符")
    private String dbId;

    @ApiModelProperty(value = "表描述")
    private String tableComment;

    @ApiModelProperty(value = "建表开始时间", notes = "周期性表可设置创建周期", access = "yyyy-MM-dd")
    private String startTime;

    @ApiModelProperty(value = "建表结束时间", notes = "周期性表可设置创建周期", access = "yyyy-MM-dd")
    private String endTime;

    @ApiModelProperty(value = "表字段信息")
    private List<CreateTableParamCondition.CreateTableFieldParam> createTableParamList = new LinkedList<>();

    @ApiModelProperty(value = "es分词器定义", notes = "es分词器定义")
    private Map<String, Object> analyzerDefine;

    @ApiModelProperty(value = "es分词器包括（analyzer, tokenizer）", notes = "es分词器包括（analyzer, tokenizer）")
    private Map<String, Object> analysis;

    @ApiModelProperty(value = "表模型信息", notes = "表模型信息")
    private CreateTableParamCondition.TableModelParam tableModelParam;

    @ApiModelProperty(value = "分布键信息", notes = "分布键信息")
    private CreateTableParamCondition.DistributedParam distributedParam;

    public Map<String, Object> getAnalysis() {
        return analysis;
    }

    public void setAnalysis(Map<String, Object> analysis) {
        this.analysis = analysis;
    }

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

    public String getTableComment() {
        return tableComment;
    }

    public void setTableComment(String tableComment) {
        this.tableComment = tableComment;
    }

    public List<CreateTableParamCondition.CreateTableFieldParam> getCreateTableParamList() {
        return createTableParamList;
    }

    public void setCreateTableParamList(List<CreateTableParamCondition.CreateTableFieldParam> createTableParamList) {
        this.createTableParamList = createTableParamList;
    }

    public String getStartTime() {
        return startTime;
    }

    public void setStartTime(String startTime) {
        this.startTime = startTime;
    }

    public String getEndTime() {
        return endTime;
    }

    public void setEndTime(String endTime) {
        this.endTime = endTime;
    }

    public Map<String, Object> getAnalyzerDefine() {
        return analyzerDefine;
    }

    public void setAnalyzerDefine(Map<String, Object> analyzerDefine) {
        this.analyzerDefine = analyzerDefine;
    }

    public CreateTableParamCondition.TableModelParam getTableModelParam() {
        return tableModelParam;
    }

    public void setTableModelParam(CreateTableParamCondition.TableModelParam tableModelParam) {
        this.tableModelParam = tableModelParam;
    }

    public CreateTableParamCondition.DistributedParam getDistributedParam() {
        return distributedParam;
    }

    public void setDistributedParam(CreateTableParamCondition.DistributedParam distributedParam) {
        this.distributedParam = distributedParam;
    }
}
