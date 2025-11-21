package com.meiya.whalex.interior.db.operation.in;

import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * CREATE TABLE LIKE 语法
 *
 * @author 黄河森
 * @date 2024/5/22
 * @package com.meiya.whalex.interior.db.operation.in
 * @project whalex-data-driver
 * @description CreateTableLike
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class CreateTableLikeParam {

    @ApiModelProperty(value = "复制目标表")
    private String copyTableName;

}
