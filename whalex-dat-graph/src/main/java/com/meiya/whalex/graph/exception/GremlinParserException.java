package com.meiya.whalex.graph.exception;

/**
 * Gremlin 解析异常
 *
 * @author 黄河森
 * @date 2023/3/22
 * @package com.meiya.whalex.graph.exception
 * @project whalex-data-driver
 */
public class GremlinParserException extends RuntimeException {

    public GremlinParserException(String message) {
        super(message);
    }

    public GremlinParserException(String message, Throwable cause) {
        super(message, cause);
    }

    public GremlinParserException(Throwable cause) {
        super(cause);
    }
}
