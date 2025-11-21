package com.meiya.whalex.interior.db.operation.in;

import com.meiya.whalex.interior.base.BaseQuery;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

import javax.validation.constraints.NotBlank;
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
@ApiModel("根据连接信息创建数据库表请求报文")
@Data
public class DbCreateTableOperation extends BaseQuery {

    @NotBlank(message = "数据库表名不能为空")
    @ApiModelProperty(value = "数据库表名", required = true)
    private String tableName;

    @NotBlank(message = "数据库类型不能为空")
    @ApiModelProperty(value = "数据库类型")
    private String dbType;

    @NotBlank(message = "数据库配置信息不能为空")
    @ApiModelProperty(value = "数据库配置信息")
    private String connSetting;

    @ApiModelProperty(value = "表配置信息，默认单表")
    private String tableJson = "{\"periodType\":\"only_one\"}";

    @ApiModelProperty(value = "数据库版本")
    private String version;

    @NotBlank(message = "云厂商配置信息不能为空")
    @ApiModelProperty(value = "云厂商")
    private String cloudCode;

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

    @ApiModelProperty(value = "标签")
    private String tag;

    @ApiModelProperty(value = "表模型信息", notes = "表模型信息")
    private CreateTableParamCondition.TableModelParam tableModelParam;

    @ApiModelProperty(value = "分布键信息", notes = "分布键信息")
    private CreateTableParamCondition.DistributedParam distributedParam;
}
