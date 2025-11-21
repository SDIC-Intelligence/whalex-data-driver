package com.meiya.whalex.db.entity;

import com.meiya.whalex.util.JsonUtil;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;


/**
 * API 接口底层服务数据插入统一入口
 *
 * @author 黄河森
 * @date 2019/9/11
 * @project whale-cloud-platformX
 */
@ApiModel(value = "SQL操作参数")
@Data
public class DbSqlEntity extends DbBaseEntity implements Cloneable  {

    /**
     * sql语句
     */
    @ApiModelProperty(value = "sql语句")
    private String sql;

    /**
     * sql参数
     */
    @ApiModelProperty(value = "sql参数")
    private Object[] params;


    @Override
    public Object clone() throws CloneNotSupportedException {
        String objectToStr = JsonUtil.objectToStr(this);
        return JsonUtil.jsonStrToObject(objectToStr, DbSqlEntity.class);
    }
}
