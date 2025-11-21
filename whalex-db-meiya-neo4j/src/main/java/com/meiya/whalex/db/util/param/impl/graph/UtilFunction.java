package com.meiya.whalex.db.util.param.impl.graph;

import lombok.Builder;
import lombok.Data;

/**
 * Neo4J 提供的工具函数
 *
 * @author 黄河森
 * @date 2023/4/4
 * @package com.meiya.whalex.db.util.param.impl.graph
 * @project whalex-data-driver
 */
@Data
@Builder
public class UtilFunction {

    private UtilType type;

    private String key;

    private String as;

    public enum UtilType {
        /**
         * 遍历集合，输出为一个个元素
         */
        UNWIND
    }

}
