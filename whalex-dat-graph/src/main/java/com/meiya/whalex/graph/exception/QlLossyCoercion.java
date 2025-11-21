package com.meiya.whalex.graph.exception;

/**
 * @author 黄河森
 * @date 2024/3/8
 * @package com.meiya.whalex.graph.exception
 * @project whalex-data-driver
 * @description QlLossyCoercion
 */
public class QlLossyCoercion extends QlValueException {

    private static final long serialVersionUID = -6259981390929065202L;

    public QlLossyCoercion(String sourceTypeName, String destinationTypeName) {
        super(String.format("Cannot coerce %s to %s without losing precision", sourceTypeName, destinationTypeName));
    }

}
