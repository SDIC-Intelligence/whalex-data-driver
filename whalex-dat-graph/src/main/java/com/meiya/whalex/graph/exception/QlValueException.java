package com.meiya.whalex.graph.exception;

/**
 * @author 黄河森
 * @date 2024/3/8
 * @package com.meiya.whalex.graph.exception
 * @project whalex-data-driver
 * @description QlValueException
 */
public class QlValueException extends QlClientException {

    private static final long serialVersionUID = -1269336313727174998L;

    public QlValueException(String message) {
        super(message);
    }
}
