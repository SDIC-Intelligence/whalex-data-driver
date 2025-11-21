package com.meiya.whalex.interior.db.operation.in;

import com.meiya.whalex.interior.base.BaseQuery;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import javax.validation.constraints.NotBlank;

/**
 * 数据库连接测试参数实体
 *
 * @author 黄河森
 * @date 2019/11/28
 * @project whale-cloud-platformX
 */
@ApiModel(value = "数据库连接测试请求报文")
public class DatabaseQuery extends BaseQuery {

    @ApiModelProperty(value = "数据库标识符", notes = "bigdataResourceId 与 connSetting 必须传递其中一个")
    private String bigdataResourceId;

    @NotBlank(message = "组件类型不能为空")
    @ApiModelProperty(value = "组件类型", notes = "设置 connSetting 必须传递该参数")
    private String resourceName;

    @ApiModelProperty(value = "数据库配置信息", notes = "bigdataResourceId 与 connSetting 必须传递其中一个")
    private String connSetting;

    @ApiModelProperty(value = "数据库连接类型")
    private String connType;

    @ApiModelProperty(value = "组件版本")
    private String version;

    @ApiModelProperty(value = "云厂商")
    private String cloudCode;

    @ApiModelProperty(value = "标签")
    private String tag;

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public String getCloudCode() {
        return cloudCode;
    }

    public void setCloudCode(String cloudCode) {
        this.cloudCode = cloudCode;
    }

    public String getBigdataResourceId() {
        return bigdataResourceId;
    }

    public void setBigdataResourceId(String bigdataResourceId) {
        this.bigdataResourceId = bigdataResourceId;
    }

    public void setResourceName(String resourceName) {
        this.resourceName = resourceName;
    }

    public String getConnSetting() {
        return connSetting;
    }

    public void setConnSetting(String connSetting) {
        this.connSetting = connSetting;
    }

    public String getConnType() {
        return connType;
    }

    public void setConnType(String connType) {
        this.connType = connType;
    }

    public String getResourceName() {
        return resourceName;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }
}
