package com.meiya.whalex.interior.db.search.in;

import com.meiya.whalex.interior.db.search.condition.AssociatedType;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

import java.io.Serializable;
import java.util.List;

/**
 * 关联查询定义
 *
 * @author 黄河森
 * @date 2022/2/21
 * @package com.meiya.whalex.interior.db.search.in
 * @project whalex-data-driver
 */
@ApiModel(value = "关联查询定义")
@Data
public class AssociatedQuery implements Serializable {

    @ApiModelProperty(value = "关联子表名称")
    private String tableName;

    @ApiModelProperty(value = "源表字段名称")
    private String localField;

    @ApiModelProperty(value = "关联子表字段名称")
    private String foreignField;

    @ApiModelProperty(value = "别名")
    private String as;

    @ApiModelProperty(value = "连接方式")
    private AssociatedType type;

    @ApiModelProperty("关联子表返回字段")
    private List<String> select;

    @ApiModelProperty(value = "关联子表查询条件")
    private Where where;

}
