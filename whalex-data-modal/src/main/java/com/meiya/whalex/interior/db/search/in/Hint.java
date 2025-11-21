package com.meiya.whalex.interior.db.search.in;

import com.meiya.whalex.interior.db.search.condition.Sort;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

import java.io.Serializable;

/**
 * @author 黄河森
 * @date 2024/1/11
 * @package com.meiya.whalex.interior.db.search.in
 * @project whalex-data-driver
 * @description Index
 */
@ApiModel(value = "指定查询命中特定索引执行对象")
@Data
public class Hint implements Serializable {

    @ApiModelProperty(value = "查询提示索引名称")
    private String indexName;

    @ApiModelProperty(value = "查询提示索引信息")
    private HintIndex index;

    @ApiModel(value = "查询提示索引描述")
    @Data
    public static class HintIndex implements Serializable {
        @ApiModelProperty(value = "索引名称")
        private String column;
        @ApiModelProperty(value = "索引排序")
        private Sort sort = Sort.ASC;
    }

}
