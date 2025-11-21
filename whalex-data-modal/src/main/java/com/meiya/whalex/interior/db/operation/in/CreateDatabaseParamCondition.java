package com.meiya.whalex.interior.db.operation.in;

import io.swagger.annotations.ApiModel;
import lombok.Data;

@ApiModel(value = "创建数据库条件")
@Data
public class CreateDatabaseParamCondition {

    private String dbName;

    private String character;

    private String collate;

}
