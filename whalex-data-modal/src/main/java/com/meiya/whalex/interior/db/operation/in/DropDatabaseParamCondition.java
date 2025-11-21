package com.meiya.whalex.interior.db.operation.in;

import io.swagger.annotations.ApiModel;
import lombok.Data;

@ApiModel(value = "删除数据库条件")
@Data
public class DropDatabaseParamCondition {

    private String dbName;

}
