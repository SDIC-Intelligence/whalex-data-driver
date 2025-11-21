package com.meiya.whalex.db.entity.graph;

import com.meiya.whalex.graph.entity.GraphPath;
import com.meiya.whalex.graph.util.GraphStringFactory;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * @author 黄河森
 * @date 2022/12/28
 * @package com.meiya.whalex.db.entity.graph
 * @project whalex-data-driver
 */
public class GremlinPath implements GraphPath {

    private Path path;

    public GremlinPath(Path path) {
        this.path = path;
    }

    @Override
    public List<Object> objects() {
        List<Object> objects = this.path.objects();
        List<Object> results = new ArrayList<>(objects.size());
        for (Object object : objects) {
            if (object instanceof Vertex) {
                results.add(new GremlinVertex((Vertex) object));
            } else if (object instanceof Edge) {
                results.add(new GremlinEdge((Edge) object));
            } else {
                results.add(object);
            }
        }
        return results;
    }

    @Override
    public List<Set<String>> labels() {
        return this.path.labels();
    }

    @Override
    public String toString() {
        return GraphStringFactory.pathString(this);
    }

    @Override
    public int hashCode() {
        return objects().hashCode();
    }

    @Override
    public boolean equals(final Object other) {
        if (!(other instanceof Path)) {
            return false;
        } else {
            Path otherPath = (Path)other;
            if (otherPath.size() != this.objects().size()) {
                return false;
            } else {
                List<Object> otherPathObjects = otherPath.objects();
                List<Set<String>> otherPathLabels = otherPath.labels();

                for(int i = this.objects().size() - 1; i >= 0; --i) {
                    if (!this.objects().get(i).equals(otherPathObjects.get(i))) {
                        return false;
                    }

                    if (!((Set)this.labels().get(i)).equals(otherPathLabels.get(i))) {
                        return false;
                    }
                }

                return true;
            }
        }
    }
}
