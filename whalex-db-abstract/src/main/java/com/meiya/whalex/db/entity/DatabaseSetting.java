package com.meiya.whalex.db.entity;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Builder;
import lombok.Data;

import java.util.List;

/**
 * @author 黄河森
 * @date 2021/8/2
 * @project whalex-data-driver-back
 */
@ApiModel(value = "组件连接信息")
@Data
@Builder
public class DatabaseSetting {

    @ApiModelProperty(value = "云组件库信息编码")
    private String bigDataResourceId;
    @ApiModelProperty(value = "云组件组件类型")
    private String dbType;
    @ApiModelProperty(value = "云组件连接类型")
    private String connTypeId;
    @ApiModelProperty(value = "云组件库信息")
    private String connSetting;
    @ApiModelProperty(value = "云厂商代码")
    private String cloudCode;
    @ApiModelProperty(value = "云组件版本")
    private String version;
    @ApiModelProperty(value = "组件标签")
    private String tag;

    public DatabaseSetting() {
    }

    public DatabaseSetting(String bigDataResourceId, String dbType, String connTypeId, String connSetting, String cloudCode, String version, String tag) {
        this.bigDataResourceId = bigDataResourceId;
        this.dbType = dbType;
        this.connTypeId = connTypeId;
        this.connSetting = connSetting;
        this.cloudCode = cloudCode;
        this.version = version;
        this.tag = tag;
    }
}
