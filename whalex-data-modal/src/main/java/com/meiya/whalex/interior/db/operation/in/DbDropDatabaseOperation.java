package com.meiya.whalex.interior.db.operation.in;

import com.meiya.whalex.interior.base.BaseQuery;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

import javax.validation.constraints.NotBlank;

/**
 * 数据库删除请求报文
 *
 * @author 黄河森
 * @date 2019/11/28
 * @project whale-cloud-platformX
 */
@Data
@ApiModel("根据数据库配置删除数据库请求报文")
public class DbDropDatabaseOperation extends BaseQuery {

    @NotBlank(message = "数据库名不能为空")
    @ApiModelProperty(value = "数据库名", required = true)
    private String dbName;

    @NotBlank(message = "数据库类型不能为空")
    @ApiModelProperty(value = "数据库类型")
    private String dbType;

    @NotBlank(message = "数据库配置信息不能为空")
    @ApiModelProperty(value = "数据库配置信息")
    private String connSetting;

    @ApiModelProperty(value = "数据库版本")
    private String version;

    @NotBlank(message = "云厂商配置信息不能为空")
    @ApiModelProperty(value = "云厂商")
    private String cloudCode;

    @ApiModelProperty(value = "标签")
    private String tag;


}
