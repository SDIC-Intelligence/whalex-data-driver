package com.meiya.whalex.db.entity.graph;

import com.meiya.whalex.graph.entity.GraphResult;
import com.meiya.whalex.graph.entity.GraphResultSet;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * @author 黄河森
 * @date 2023/5/8
 * @package com.meiya.whalex.db.entity.graph
 * @project whalex-data-driver
 */
public class JanusGraphResultSet implements GraphResultSet {

    private Traversal defaultGraphTraversal;

    public JanusGraphResultSet(Traversal defaultGraphTraversal) {
        this.defaultGraphTraversal = defaultGraphTraversal;
    }

    @Override
    public GraphResult one() {
        if (iterator().hasNext()) {
            GraphResult next = iterator().next();
            return next;
        } else {
            return null;
        }
    }

    @Override
    public CompletableFuture<List<GraphResult>> all() {
        return CompletableFuture.supplyAsync(new Supplier<List<GraphResult>>() {
            @Override
            public List<GraphResult> get() {
                List<GraphResult> GraphResults = new ArrayList<>();
                while (iterator().hasNext()) {
                    GraphResults.add(iterator().next());
                }
                return GraphResults;
            }
        });
    }

    @Override
    public Stream<GraphResult> stream() {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(this.iterator(), 1088), false);
    }

    @Override
    public Iterator<GraphResult> iterator() {
        return new Iterator<GraphResult>() {
            private GraphResult nextOne = null;

            @Override
            public boolean hasNext() {
                return defaultGraphTraversal.hasNext();
            }

            @Override
            public GraphResult next() {
                Object next = defaultGraphTraversal.next();
                return new GremlinResult(new Result(next));
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }
}
