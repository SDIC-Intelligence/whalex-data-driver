package com.meiya.whalex.interior.db.search.in;

import com.meiya.whalex.interior.base.BaseQuery;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

import javax.validation.constraints.NotBlank;

/**
 * 数据朔源查询
 *
 * @author 黄河森
 * @date 2020/5/28
 * @project whale-cloud-platformX
 */
@ApiModel(value = "数据朔源查询请求参数")
@Data
public class DataSourceQuery extends BaseQuery {

    @ApiModelProperty(value = "朔源资源标识符", required = true)
    @NotBlank(message = "朔源资源标识符")
    private String resourceId;

    @ApiModelProperty(value = "朔源ID", required = true)
    @NotBlank(message = "朔源ID不能为空")
    private String id;

}
