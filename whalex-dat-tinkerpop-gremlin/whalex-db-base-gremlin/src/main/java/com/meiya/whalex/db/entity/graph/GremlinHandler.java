package com.meiya.whalex.db.entity.graph;

import com.meiya.whalex.db.entity.AbstractDbHandler;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;

import java.util.List;
import java.util.Map;

/**
 * @author chenjp
 * @date 2021/2/23
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class GremlinHandler extends AbstractDbHandler {

    private GremlinQuery GremlinQuery;

    private GremlinUpdate gremlinUpdate;

    private GremlinDelete gremlinDelete;

    @Data
    public static class GremlinQuery {
        private String gremlin;
        private Map<String, Object> params;
        private Traversal queryTraversal;
        private Traversal countTraversal;
        private String returnType;
    }

    @Data
    public static class GremlinInsert {
        private String gremlin;
    }

    @Data
    public static class GremlinUpdate {
        private GraphTraversal traversal;
    }

    @Data
    public static class GremlinDelete {
        private GraphTraversal traversal;
        private Long total;
    }

}
