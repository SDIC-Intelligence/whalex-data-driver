package com.meiya.whalex.db.util.param.impl.graph;

import lombok.Builder;
import lombok.Data;

/**
 * 分页
 *
 * @author 黄河森
 * @date 2023/4/6
 * @package com.meiya.whalex.db.util.param.impl.graph
 * @project whalex-data-driver
 */
@Data
@Builder
public class CypherPage {

    private Long skip;

    private Long limit;

}
