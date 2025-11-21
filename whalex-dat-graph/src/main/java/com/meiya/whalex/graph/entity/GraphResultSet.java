package com.meiya.whalex.graph.entity;


import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

/**
 * 图数据库检索结果集合
 *
 * @author 黄河森
 * @date 2022/12/27
 * @package com.meiya.whalex.graph.gremlin.driver
 * @project whalex-data-driver
 */
public interface GraphResultSet extends Iterable<GraphResult> {

    /**
     * 获取单个结果对象
     *
     * @return
     */
    GraphResult one();

    /**
     * 一次获取所有结果
     *
     * @return
     */
    CompletableFuture<List<GraphResult>> all();

    /**
     * 返回流对象
     *
     * @return
     */
    Stream<GraphResult> stream();
}
