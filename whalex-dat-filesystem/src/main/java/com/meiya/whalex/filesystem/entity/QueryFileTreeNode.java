package com.meiya.whalex.filesystem.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 查看文件树
 *
 * @author 黄河森
 * @date 2023/9/1
 * @package com.meiya.whalex.filesystem.entity
 * @project whalex-data-driver
 * @description QueryFileTreeNode
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class QueryFileTreeNode {

    /**
     * 递归层级，默认只查询当前层级
     */
    private Integer hierarchy = 0;

    private FilterFileCondition filter;

}
