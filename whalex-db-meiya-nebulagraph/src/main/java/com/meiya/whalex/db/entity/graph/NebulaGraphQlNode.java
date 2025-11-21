package com.meiya.whalex.db.entity.graph;

import com.meiya.whalex.db.util.NebulaGraphValueWrapperUtil;
import com.meiya.whalex.graph.entity.InternalQlNode;
import com.meiya.whalex.graph.entity.QlValue;
import com.vesoft.nebula.client.graph.data.Node;
import com.vesoft.nebula.client.graph.data.ValueWrapper;

import java.io.UnsupportedEncodingException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author 黄河森
 * @date 2024/3/8
 * @package com.meiya.whalex.db.entity.graph
 * @project whalex-data-driver
 * @description NebulaGraphQlNode
 */
public class NebulaGraphQlNode extends InternalQlNode {

    public NebulaGraphQlNode(long id) {
        super(id);
    }

    public NebulaGraphQlNode(long id, Collection<String> labels, Map<String, QlValue> properties) {
        super(id, labels, properties);
    }

    public static class Builder {
        public static NebulaGraphQlNode build(Node node) throws UnsupportedEncodingException {
            List<String> labels = node.labels();
            Map<String, QlValue> properties = new HashMap<>();
            for (String label : labels) {
                HashMap<String, ValueWrapper> _properties = node.properties(label);
                for (Map.Entry<String, ValueWrapper> entry : _properties.entrySet()) {
                    String key = entry.getKey();
                    ValueWrapper value = entry.getValue();
                    QlValue wrapper = NebulaGraphValueWrapperUtil.wrapper(value);
                    properties.put(label + "." + key, wrapper);
                }
            }
            return new NebulaGraphQlNode(Long.parseLong(node.getId().asString()), node.labels(), properties);
        }
    }
}
