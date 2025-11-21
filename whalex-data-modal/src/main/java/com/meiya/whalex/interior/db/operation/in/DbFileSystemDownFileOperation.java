package com.meiya.whalex.interior.db.operation.in;

import com.meiya.whalex.interior.base.BaseQuery;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

import javax.validation.constraints.NotBlank;

/**
 * 创建表请求报文
 *
 * @author 黄河森
 * @date 2019/12/3
 * @project whale-cloud-platformX
 */
@Data
@ApiModel("根据数据库配置下載文件")
public class DbFileSystemDownFileOperation extends BaseQuery {

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
    private String tableJson;

    @ApiModelProperty(value = "数据库版本")
    private String version;

    @NotBlank(message = "云厂商配置信息不能为空")
    @ApiModelProperty(value = "云厂商")
    private String cloudCode;

    @ApiModelProperty(value = "标签")
    private String tag;

    @ApiModelProperty(value = "文件路径")
    private String path;

}
