package com.meiya.whalex.db.entity.graph;

import com.meiya.whalex.db.util.param.impl.graph.ReturnType;
import com.meiya.whalex.graph.entity.GraphResult;
import com.meiya.whalex.graph.entity.GraphResultSet;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.async.ResultCursor;
import org.neo4j.driver.internal.InternalResult;

import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.CompletionStage;

/**
 * @author 黄河森
 * @date 2023/1/3
 * @package com.meiya.whalex.db.entity.graph
 * @project whalex-data-driver
 */
public class Neo4jResultSet extends Neo4jBaseResultSet<Result, Session> implements GraphResultSet {

    public Neo4jResultSet(Result result, Session session, ReturnType returnType) {
        super(result, session, returnType);
    }

    @Override
    protected Record resultNext() {
        if (result.hasNext()) {
            Record next = result.next();
            return next;
        } else {
            close();
            return null;
        }
    }

    @Override
    protected CompletionStage<List<Record>> resultAll() {
        try {
            Class<InternalResult> internalStatementResultClass = InternalResult.class;
            Field cursor = internalStatementResultClass.getDeclaredField("cursor");
            cursor.setAccessible(true);
            ResultCursor statementResultCursor = (ResultCursor) cursor.get(result);
            return statementResultCursor.listAsync();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void close() {
        if (session != null) {
            session.close();
        }
    }
}
