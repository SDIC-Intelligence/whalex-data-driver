package com.meiya.whalex.graph.exception;

/**
 * @author 黄河森
 * @date 2024/3/8
 * @package com.meiya.whalex.graph.entity
 * @project whalex-data-driver
 * @description QlNotMultiValued
 */
public class QlNotMultiValued extends QlValueException {

    private static final long serialVersionUID = -7380569883011364095L;

    public QlNotMultiValued(String message) {
        super(message);
    }
}
