package com.meiya.whalex.db.entity.graph;

import com.meiya.whalex.db.util.NebulaGraphValueWrapperUtil;
import com.meiya.whalex.graph.entity.InternalQlRelationship;
import com.meiya.whalex.graph.entity.QlValue;
import com.vesoft.nebula.client.graph.data.Node;
import com.vesoft.nebula.client.graph.data.Relationship;
import com.vesoft.nebula.client.graph.data.ValueWrapper;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author 黄河森
 * @date 2024/3/8
 * @package com.meiya.whalex.db.entity.graph
 * @project whalex-data-driver
 * @description NebulaGraphQlRelationship
 */
public class NebulaGraphQlRelationship extends InternalQlRelationship {
    public NebulaGraphQlRelationship(long id, long start, long end, String type) {
        super(id, start, end, type);
    }

    public NebulaGraphQlRelationship(long id, long start, long end, String type, Map<String, QlValue> properties) {
        super(id, start, end, type, properties);
    }

    public static class Builder {
        public static NebulaGraphQlRelationship build(Relationship relationship) throws UnsupportedEncodingException {
            HashMap<String, ValueWrapper> _properties = relationship.properties();
            Map<String, QlValue> properties = new HashMap<>();
            for (Map.Entry<String, ValueWrapper> entry : _properties.entrySet()) {
                ValueWrapper value = entry.getValue();
                String key = entry.getKey();
                properties.put(key, NebulaGraphValueWrapperUtil.wrapper(value));
            }
            return new NebulaGraphQlRelationship(relationship.ranking(), relationship.srcId().asLong(), relationship.dstId().asLong(), relationship.edgeName(), properties);
        }
    }
}
