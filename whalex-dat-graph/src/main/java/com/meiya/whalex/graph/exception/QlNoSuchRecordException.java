package com.meiya.whalex.graph.exception;

import java.util.NoSuchElementException;

/**
 * @author 黄河森
 * @date 2024/3/7
 * @package com.meiya.whalex.graph.exception
 * @project whalex-data-driver
 * @description QlNoSuchRecordException
 */
public class QlNoSuchRecordException extends NoSuchElementException {

    private static final long serialVersionUID = 9091962868264042492L;

    public QlNoSuchRecordException(String message) {
        super(message);
    }

}
