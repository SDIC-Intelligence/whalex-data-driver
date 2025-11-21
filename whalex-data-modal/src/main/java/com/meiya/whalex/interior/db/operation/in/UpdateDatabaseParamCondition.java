package com.meiya.whalex.interior.db.operation.in;

import io.swagger.annotations.ApiModel;
import lombok.Data;

@ApiModel(value = "更新数据库条件")
@Data
public class UpdateDatabaseParamCondition {

    private String dbName;

    private String newDbName;

}
