package com.meiya.whalex.graph.exception;

/**
 * @author 黄河森
 * @date 2024/3/8
 * @package com.meiya.whalex.graph.exception
 * @project whalex-data-driver
 * @description QlValueException
 */
public class QlUnsupportedEncodingException extends QlClientException {

    private static final long serialVersionUID = -1269336313727174992L;

    public QlUnsupportedEncodingException(String message) {
        super(message);
    }
}
