package com.meiya.whalex.graph.exception;

/**
 * Gremlin 未实现异常
 *
 * @author 黄河森
 * @date 2023/3/27
 * @package com.meiya.whalex.graph.exception
 * @project whalex-data-driver
 */
public class GremlinNotImplementedException extends RuntimeException {

    public GremlinNotImplementedException(String message) {
        super(message);
    }

}
