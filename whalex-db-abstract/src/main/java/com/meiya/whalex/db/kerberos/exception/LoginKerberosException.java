package com.meiya.whalex.db.kerberos.exception;

public class LoginKerberosException extends Exception {
    public LoginKerberosException() {
    }

    public LoginKerberosException(String message) {
        super(message);
    }

    public LoginKerberosException(String message, Throwable cause) {
        super(message, cause);
    }

    public LoginKerberosException(Throwable cause) {
        super(cause);
    }

    public LoginKerberosException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
