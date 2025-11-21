package com.meiya.whalex.db.entity;

import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

/**
 * 组件修复表分区接口报文
 *
 * @author 黄河森
 * @date 2021/10/4
 * @package com.meiya.whalex.db.entity
 * @project whalex-data-driver
 */
@Data
public class DbRepairTableEntity {

    /**
     * 组件信息
     */
    @ApiModelProperty(value = "云组件信息")
    private DbHandleEntity dbHandleEntity;

}
