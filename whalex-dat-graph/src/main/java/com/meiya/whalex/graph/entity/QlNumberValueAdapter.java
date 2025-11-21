package com.meiya.whalex.graph.entity;

/**
 * @author 黄河森
 * @date 2024/3/8
 * @package com.meiya.whalex.graph.entity
 * @project whalex-data-driver
 * @description QlNumberValueAdapter
 */
public abstract class QlNumberValueAdapter<V extends Number> extends QlValueAdapter {

    public QlNumberValueAdapter() {
    }

    public final V asObject() {
        return this.asNumber();
    }

    public abstract V asNumber();

}
