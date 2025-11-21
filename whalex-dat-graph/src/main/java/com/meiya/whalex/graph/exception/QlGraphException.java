package com.meiya.whalex.graph.exception;

/**
 * @author 黄河森
 * @date 2024/3/8
 * @package com.meiya.whalex.graph.exception
 * @project whalex-data-driver
 * @description QlGraphException
 */
public abstract class QlGraphException extends RuntimeException {

    private static final long serialVersionUID = -80579062276712566L;
    private final String code;

    public QlGraphException(String message) {
        this("N/A", message);
    }

    public QlGraphException(String message, Throwable cause) {
        this("N/A", message, cause);
    }

    public QlGraphException(String code, String message) {
        this(code, message, (Throwable)null);
    }

    public QlGraphException(String code, String message, Throwable cause) {
        super(message, cause);
        this.code = code;
    }

    public String code() {
        return this.code;
    }

}
