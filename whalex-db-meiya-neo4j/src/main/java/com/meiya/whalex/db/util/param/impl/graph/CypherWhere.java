package com.meiya.whalex.db.util.param.impl.graph;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Cypher where 构造器
 *
 * @author 黄河森
 * @date 2023/3/22
 * @package com.meiya.whalex.db.util.param.impl.graph
 * @project whalex-data-driver
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CypherWhere {

    private String key;

    private CypherRel op;

    private Object value;

}
