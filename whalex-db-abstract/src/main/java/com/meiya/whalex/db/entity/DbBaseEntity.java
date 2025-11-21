package com.meiya.whalex.db.entity;

import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

import java.util.List;

@Data
public class DbBaseEntity {

    /**
     * 资源标识符
     */
    @ApiModelProperty(value = "资源标识符")
    private String resourceId;

    /**
     * 组件信息
     */
    @ApiModelProperty(value = "云组件信息")
    private List<DbHandleEntity> dbInfoEntityList;


    @ApiModelProperty(value = "事务id")
    private String transactionId;

    @ApiModelProperty(value = "是否开启事务")
    private boolean openTransaction;

    @ApiModelProperty(value = "提交事务")
    private boolean commitTransaction;
}
