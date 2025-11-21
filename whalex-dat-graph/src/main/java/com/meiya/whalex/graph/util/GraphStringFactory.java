package com.meiya.whalex.graph.util;

import com.meiya.whalex.graph.entity.GraphEdge;
import com.meiya.whalex.graph.entity.GraphPath;
import com.meiya.whalex.graph.entity.GraphProperty;
import com.meiya.whalex.graph.entity.GraphVertex;
import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

/**
 * 对象 toString 工具类
 *
 * @author 黄河森
 * @date 2022/12/28
 * @package com.meiya.whalex.graph.util.helper
 * @project whalex-data-driver
 */
public class GraphStringFactory {
    private static final String V = "v";
    private static final String E = "e";
    private static final String P = "p";
    private static final String VP = "vp";
    private static final String PATH = "path";
    private static final String L_BRACKET = "[";
    private static final String R_BRACKET = "]";
    private static final String COMMA_SPACE = ", ";
    private static final String DASH = "-";
    private static final String ARROW = "->";
    private static final String EMPTY_PROPERTY = "p[empty]";
    private static final String EMPTY_VERTEX_PROPERTY = "vp[empty]";
    private static final String LINE_SEPARATOR = System.getProperty("line.separator");
    private static final String STORAGE = "storage";
    private static final String featuresStartWith = "supports";
    private static final int prefixLength = "supports".length();

    private GraphStringFactory() {
    }

    public static String vertexString(final GraphVertex vertex) {
        return "v[" + vertex.id() + "]";
    }

    public static String edgeString(final GraphEdge edge) {
        return "e[" + edge.id() + "]" + "[" + edge.outVertex().id() + "-" + edge.label() + "->" + edge.inVertex().id() + "]";
    }

    public static String propertyString(final GraphProperty property) {
        String valueString;
        if (property instanceof VertexProperty) {
            if (!property.isPresent()) {
                return "vp[empty]";
            } else {
                valueString = String.valueOf(property.value());
                return "vp[" + property.key() + "->" + StringUtils.abbreviate(valueString, 20) + "]";
            }
        } else if (!property.isPresent()) {
            return "p[empty]";
        } else {
            valueString = String.valueOf(property.value());
            return "p[" + property.key() + "->" + StringUtils.abbreviate(valueString, 20) + "]";
        }
    }

    public static String pathString(GraphPath path) {
        return "path[" + String.join(", ", IteratorUtils.map(path, Object::toString)) + "]";
    }
}
