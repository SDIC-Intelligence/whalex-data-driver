package com.meiya.whalex.db.kerberos.exception;

public class KerberosRuntimeException extends RuntimeException {
    public KerberosRuntimeException() {
    }

    public KerberosRuntimeException(String message) {
        super(message);
    }

    public KerberosRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }

    public KerberosRuntimeException(Throwable cause) {
        super(cause);
    }

    public KerberosRuntimeException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
