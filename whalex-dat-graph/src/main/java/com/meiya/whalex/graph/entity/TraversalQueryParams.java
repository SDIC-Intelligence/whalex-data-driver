package com.meiya.whalex.graph.entity;

import lombok.Builder;
import lombok.Data;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;

/**
 * @author 黄河森
 * @date 2023/4/19
 * @package com.meiya.whalex.graph.entity
 * @project whalex-data-driver
 */
@Builder
@Data
public class TraversalQueryParams {
    private Traversal traversal;
}
