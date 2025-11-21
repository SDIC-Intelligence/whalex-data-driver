package com.meiya.whalex.db.entity.graph;

import com.meiya.whalex.db.entity.AbstractDbHandler;
import com.meiya.whalex.db.util.param.impl.graph.ReturnType;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

/**
 * Neo4j 组件操作实体
 *
 * @author chenjp
 * @date 2020/9/27
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class Neo4jHandler extends AbstractDbHandler {

    private Neo4jQuery neo4jQuery;
    private Neo4jInsert neo4jInsert;
    private Neo4jUpdate neo4jUpdate;
    private Neo4jDelete neo4jDelete;
    private Noe4jGremlinQuery gremlinQuery;

    @Data
    public static class Neo4jQuery {
        private String cql;
    }

    @Data
    public static class Neo4jInsert {
        private String vertexCql;
        private List<String> edgeCql;
        private List<String> idIndex;
    }

    @Data
    public static class Neo4jUpdate {
        private String cql;
    }

    @Data
    public static class Neo4jDelete {
        private String cql;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Noe4jGremlinQuery {
        private String cql;
        private Map<String, Object> params;
        private ReturnType type;
    }

}
