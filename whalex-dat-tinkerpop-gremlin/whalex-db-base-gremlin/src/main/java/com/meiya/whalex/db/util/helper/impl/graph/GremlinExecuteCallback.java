package com.meiya.whalex.db.util.helper.impl.graph;

import org.apache.tinkerpop.gremlin.driver.Client;

/**
 * @author 黄河森
 * @date 2022/12/1
 * @package com.meiya.whalex.db.util.helper.impl.graph
 * @project whalex-data-driver
 */
public interface GremlinExecuteCallback<V> {

    /**
     * Gremlin 提交
     *
     * @param client
     * @return
     * @throws Exception
     */
    V doWithAlias(Client client) throws Exception;

}
