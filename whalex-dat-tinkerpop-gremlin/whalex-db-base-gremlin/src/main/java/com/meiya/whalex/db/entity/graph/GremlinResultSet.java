package com.meiya.whalex.db.entity.graph;

import com.meiya.whalex.graph.entity.GraphResult;
import com.meiya.whalex.graph.entity.GraphResultSet;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultSet;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Spliterators;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * @author 黄河森
 * @date 2022/12/27
 * @package com.meiya.whalex.db.entity.graph
 * @project whalex-data-driver
 */
public class GremlinResultSet implements GraphResultSet {

    private ResultSet results;

    public GremlinResultSet(ResultSet results) {
        this.results = results;
    }

    @Override
    public GraphResult one() {
        Result one = results.one();
        if (one != null) {
            return new GremlinResult(one);
        } else {
            return null;
        }
    }

    @Override
    public CompletableFuture<List<GraphResult>> all() {
        CompletableFuture<List<Result>> future = results.all();
        return future.thenApply(new Function<List<Result>, List<GraphResult>>() {
            @Override
            public List<GraphResult> apply(List<Result> results) {
                return results.stream().flatMap(result -> {
                    return Stream.of(new GremlinResult(result));
                }).collect(Collectors.toList());
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
                if (null == this.nextOne) {
                    this.nextOne = GremlinResultSet.this.one();
                }

                return this.nextOne != null;
            }

            @Override
            public GraphResult next() {
                if (null == this.nextOne && !this.hasNext()) {
                    throw new NoSuchElementException();
                } else {
                    GraphResult r = this.nextOne;
                    this.nextOne = null;
                    return r;
                }
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }
}
