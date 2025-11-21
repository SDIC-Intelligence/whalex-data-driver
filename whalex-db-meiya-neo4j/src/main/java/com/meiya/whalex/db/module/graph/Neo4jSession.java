package com.meiya.whalex.db.module.graph;

import org.neo4j.driver.Session;
import org.neo4j.driver.async.AsyncSession;

/**
 * @author 黄河森
 * @date 2023/3/16
 * @package com.meiya.whalex.db.module.graph
 * @project whalex-data-driver
 */
public class Neo4jSession implements AutoCloseable {

    private Session session;

    private AsyncSession asyncSession;

    public Neo4jSession(Session session) {
        this.session = session;
    }

    public Neo4jSession(AsyncSession asyncSession) {
        this.asyncSession = asyncSession;
    }

    @Override
    public void close() throws Exception {
        if (session != null) {
            session.close();
        } else {
            asyncSession.closeAsync();
        }
    }
}
