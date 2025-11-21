package com.meiya.whalex.db.entity;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Builder;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;

/**
 * @author 黄河森
 * @date 2021/8/2
 * @project whalex-data-driver-back
 */
@ApiModel(value = "组件表配置信息参数")
@Data
@Builder
public class TableSetting {

    @ApiModelProperty(value = "云组件表信息编码")
    private String dbId;
    @ApiModelProperty(value = "云组件表名")
    private String tableName;
    @ApiModelProperty(value = "云组件表信息")
    private String tableJson;

    /**
     * 获取当前的 表标识符 或者 表名
     * @return
     */
    public String getDbIdOrTableName() {
        return StringUtils.isBlank(this.getDbId()) ? this.getTableName() : this.getDbId();
    }

    public TableSetting() {
    }

    public TableSetting(String dbId, String tableName, String tableJson) {
        this.dbId = dbId;
        this.tableName = tableName;
        this.tableJson = tableJson;
    }
}
