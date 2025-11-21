package com.meiya.whalex.graph.entity;

/**
 * 图数据库单个结果对象
 *
 * @author 黄河森
 * @date 2022/12/27
 * @package com.meiya.whalex.graph.gremlin.driver
 * @project whalex-data-driver
 */
public interface GraphResult {

    /**
     * v[id]
     *
     * @return
     */
    String getString();

    /**
     * 获取 int 值
     *
     * @return
     */
    int getInt();

    /**
     * 获取 byte 值
     *
     * @return
     */
    byte getByte();

    /**
     * 获取 short 值
     *
     * @return
     */
    short getShort();

    /**
     * 获取 long 值
     *
     * @return
     */
    long getLong();

    /**
     * 获取 float 值
     *
     * @return
     */
    float getFloat();

    /**
     * 获取 double 值
     *
     * @return
     */
    double getDouble();

    /**
     * 获取 boolean 值
     *
     * @return
     */
    boolean getBoolean();

    /**
     * 判断是否有值
     *
     * @return
     */
    boolean isNull();

    /**
     * 获取节点
     *
     * @return
     */
    GraphVertex getVertex();

    /**
     * 获取边
     *
     * @return
     */
    GraphEdge getEdge();

    /**
     * 获取节点/边
     *
     * @return
     */
    GraphElement getElement();

    /**
     * 获取边
     *
     * @return
     */
    GraphPath getPath();

    /**
     * 获取属性
     *
     * @param <V>
     * @return
     */
    <V> GraphProperty<V> getProperty();

    /**
     * 获取节点属性
     *
     * @param <V>
     * @return
     */
    <V> GraphVertexProperty<V> getVertexProperty();

    /**
     * 获取值
     *
     * @return
     */
    Object getObject();

    /**
     * 获取值转为对象
     *
     * @param clazz
     * @param <T>
     * @return
     */
    <T> T get(final Class<? extends T> clazz);

}
