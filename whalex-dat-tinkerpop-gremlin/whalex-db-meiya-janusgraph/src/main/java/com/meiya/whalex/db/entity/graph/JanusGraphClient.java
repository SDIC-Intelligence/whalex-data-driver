package com.meiya.whalex.db.entity.graph;

import com.meiya.whalex.db.kerberos.KerberosUniformAuth;
import com.meiya.whalex.graph.entity.GraphResultSet;
import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.language.grammar.GremlinQueryParser;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.function.Supplier;

/**
 * @author 黄河森
 * @date 2023/5/8
 * @package com.meiya.whalex.db.entity.graph
 * @project whalex-data-driver
 */
public class JanusGraphClient extends GremlinClient {

    private String mode;

    private JanusGraph janusGraph;

    public JanusGraph getJanusGraph() {
        return janusGraph;
    }

    public JanusGraphClient(String mode, String storage, String storageHost, String storageTable, Integer storagePort, String index, String indexHost) {
        this.mode = mode;
        JanusGraphFactory.Builder builder = JanusGraphFactory.build()
                .set("storage.backend", storage)
                .set("storage.hostname", storageHost)
                .set("storage.hbase.table", storageTable);

        if (storagePort != null) {
            builder.set("storage.port", storagePort);
        }

        if (StringUtils.isNotBlank(index) && StringUtils.isNotBlank(indexHost)) {
            builder.set("index.search.backend", index)
                    .set("index.search.hostname", indexHost);
        }
        this.janusGraph = builder.open();
    }

    public JanusGraphClient(Cluster cluster, Client client, KerberosUniformAuth kerberosUniformLogin, String mode) {
        super(cluster, client, kerberosUniformLogin);
        this.mode = mode;
    }

    @Override
    public void close() throws IOException {
        super.close();
        if (janusGraph != null) {
            janusGraph.close();
        }
    }

    @Override
    public Future<GraphResultSet> submitAsync(String gremlin) {
        if (StringUtils.equalsIgnoreCase(mode, JanusGraphDatabaseInfo.LOCAL_MODE)) {
            return CompletableFuture.supplyAsync(new Supplier<GraphResultSet>() {
                @Override
                public GraphResultSet get() {
                    return submit(gremlin);
                }
            });
        } else {
            return super.submitAsync(gremlin);
        }
    }

    @Override
    public Future<GraphResultSet> submitAsync(String gremlin, Map<String, Object> parameters) {
        if (StringUtils.equalsIgnoreCase(mode, JanusGraphDatabaseInfo.LOCAL_MODE)) {
            gremlin = conversionPlaceholder(gremlin, parameters);
            return submitAsync(gremlin);
        } else {
            return super.submitAsync(gremlin, parameters);
        }
    }

    /**
     * 替换占位符
     *
     * @param gremlin
     * @param parameters
     * @return
     */
    private String conversionPlaceholder(String gremlin, Map<String, Object> parameters) {
        List<String> values = new ArrayList<>();
        for (Object value : parameters.values()) {
            values.add(String.valueOf(value));
        }
        gremlin = StringUtils.replaceEach(gremlin, parameters.keySet().toArray(new String[parameters.size()]), values.toArray(new String[values.size()]));
        return gremlin;
    }

    @Override
    public GraphResultSet submit(String gremlin) {
        if (StringUtils.equalsIgnoreCase(mode, JanusGraphDatabaseInfo.LOCAL_MODE)) {
            DefaultGraphTraversal parse = (DefaultGraphTraversal) GremlinQueryParser.parse(gremlin);
            List<Step> steps = parse.getSteps();
            return getGraphStep(steps);
        } else {
            return super.submit(gremlin);
        }
    }

    @Override
    public GraphResultSet submit(String gremlin, Map<String, Object> parameters) {
        if (StringUtils.equalsIgnoreCase(mode, JanusGraphDatabaseInfo.LOCAL_MODE)) {
            gremlin = conversionPlaceholder(gremlin, parameters);
            return submit(gremlin);
        } else {
            return super.submit(gremlin, parameters);
        }
    }

    @Override
    public GraphResultSet submit(Traversal traversal) {
        if (StringUtils.equalsIgnoreCase(mode, JanusGraphDatabaseInfo.LOCAL_MODE)) {
            List<Step> steps = traversal.asAdmin().getSteps();
            return getGraphStep(steps);
        } else {
            return super.submit(traversal);
        }
    }

    private GraphResultSet getGraphStep(List<Step> steps) {
        GraphTraversalSource g = janusGraph.traversal();
        DefaultGraphTraversal graphTraversal = new DefaultGraphTraversal(g.clone());
        for (int i = 0; i < steps.size(); i++) {
            graphTraversal.addStep(i, steps.get(i));
        }
        return new JanusGraphResultSet(graphTraversal);
    }

    @Override
    public Future<GraphResultSet> submitAsync(Traversal traversal) {
        if (StringUtils.equalsIgnoreCase(mode, JanusGraphDatabaseInfo.LOCAL_MODE)) {
            return CompletableFuture.supplyAsync(new Supplier<GraphResultSet>() {
                @Override
                public GraphResultSet get() {
                    return submit(traversal);
                }
            });
        } else {
            return super.submitAsync(traversal);
        }
    }
}
