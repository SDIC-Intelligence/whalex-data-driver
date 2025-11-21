package com.meiya.whalex.interior.db.operation.in;

import com.meiya.whalex.interior.base.BaseQuery;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

import javax.validation.constraints.NotBlank;
import java.util.List;
import java.util.Map;

/**
 * 索引创建请求报文
 *
 * @author 黄河森
 * @date 2019/11/28
 * @project whale-cloud-platformX
 */
@Data
@ApiModel("根据数据库配置创建索引请求报文")
public class DbCreateIndexOperation extends BaseQuery {

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

    @ApiModelProperty(value = "标签")
    private String tag;

    @ApiModelProperty(value = "索引字段名集合")
    private List<String> columns;

    @ApiModelProperty(value = "索引顺序集合", allowableValues = "1,-1", notes = "正序索引: 1, 倒排索引: -1")
    private List<String> sorts;

    @ApiModelProperty(value = "是否异步操作", allowableValues = "true, false")
    private Boolean async = true;

    @ApiModelProperty(value = "索引名称")
    private String indexName;

    @ApiModelProperty(value = "索引类型")
    private String indexType;

    @ApiModelProperty(value = "属性")
    private Map<String, String> properties;
}
