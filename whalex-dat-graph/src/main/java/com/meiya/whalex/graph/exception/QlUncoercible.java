package com.meiya.whalex.graph.exception;

/**
 * @author 黄河森
 * @date 2024/3/11
 * @package com.meiya.whalex.graph.exception
 * @project whalex-data-driver
 * @description QlUncoercible
 */
public class QlUncoercible extends QlValueException {

    private static final long serialVersionUID = -6259981390929065202L;

    public QlUncoercible(String sourceTypeName, String destinationTypeName) {
        super(String.format("Cannot coerce %s to %s", sourceTypeName, destinationTypeName));
    }

}
