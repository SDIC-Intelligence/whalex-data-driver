package com.meiya.whalex.db.entity.graph;

import com.meiya.whalex.db.util.param.impl.graph.ReturnType;
import com.meiya.whalex.graph.entity.GraphResult;
import com.meiya.whalex.graph.entity.GraphResultSet;
import org.neo4j.driver.Record;
import org.neo4j.driver.async.AsyncSession;
import org.neo4j.driver.async.ResultCursor;
import org.neo4j.driver.internal.util.Futures;

import java.util.*;
import java.util.concurrent.CompletionStage;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * @author 黄河森
 * @date 2023/1/3
 * @package com.meiya.whalex.db.entity.graph
 * @project whalex-data-driver
 */
public class Neo4jCursorResultSet extends Neo4jBaseResultSet<ResultCursor, AsyncSession> implements GraphResultSet {

    public Neo4jCursorResultSet(ResultCursor result, AsyncSession session, ReturnType returnType) {
        super(result, session, returnType);
    }

    @Override
    protected Record resultNext() {
        CompletionStage<Record> recordCompletionStage = this.result.peekAsync();
        Record record = Futures.blockingGet(recordCompletionStage);
        if (record != null) {
            CompletionStage<Record> stage = this.result.nextAsync();
            record = Futures.blockingGet(stage);
            return record;
        } else {
            close();
            return null;
        }
    }

    @Override
    protected CompletionStage<List<Record>> resultAll() {
        return result.listAsync();
    }

    @Override
    protected void close() {
        if (session != null) {
            session.closeAsync();
        }
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
                    this.nextOne = Neo4jCursorResultSet.this.one();
                }

                boolean b = this.nextOne != null;

                if (!b) {
                    if (session != null) {
                        session.closeAsync();
                    }
                }

                return b;
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
