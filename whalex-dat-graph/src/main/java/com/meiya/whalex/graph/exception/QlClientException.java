package com.meiya.whalex.graph.exception;

/**
 * @author 黄河森
 * @date 2024/3/8
 * @package com.meiya.whalex.graph.exception
 * @project whalex-data-driver
 * @description ClientException
 */
public class QlClientException extends QlGraphException {
    public QlClientException(String message) {
        super(message);
    }

    public QlClientException(String message, Throwable cause) {
        super(message, cause);
    }

    public QlClientException(String code, String message) {
        super(code, message);
    }
}
