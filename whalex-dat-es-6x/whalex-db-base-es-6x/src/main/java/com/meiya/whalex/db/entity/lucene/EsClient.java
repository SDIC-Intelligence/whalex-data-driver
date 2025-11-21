package com.meiya.whalex.db.entity.lucene;

import org.elasticsearch.client.RestClient;

/**
 * Es 连接对象实体
 * 适配读写分离
 *
 * @author 黄河森
 * @date 2019/12/21
 * @project whale-cloud-platformX
 */
public class EsClient {

    /**
     * 读连接
     */
    private RestClient queryRestClient;

    /**
     * 默认连接，没有读连接时，读操作也使用该对象
     */
    private RestClient restClient;

    /**
     * 前置负载均衡
     */
    private boolean preLoadBalancing;

    public RestClient getQueryRestClient() {
        return queryRestClient;
    }

    public void setQueryRestClient(RestClient queryRestClient) {
        this.queryRestClient = queryRestClient;
    }

    public RestClient getRestClient() {
        return restClient;
    }

    public void setRestClient(RestClient restClient) {
        this.restClient = restClient;
    }

    public RestClient getQueryClient() {
        if (queryRestClient != null) {
            return getQueryRestClient();
        }
        return getRestClient();
    }

    public boolean isPreLoadBalancing() {
        return preLoadBalancing;
    }

    public void setPreLoadBalancing(boolean preLoadBalancing) {
        this.preLoadBalancing = preLoadBalancing;
    }
}
