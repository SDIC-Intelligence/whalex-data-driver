package com.meiya.whalex.db.kerberos.exception;

/**
 * 证书异常
 *
 * @author 黄河森
 * @date 2019/12/16
 * @project whale-cloud-platformX
 */
public class GetKerberosException extends LoginKerberosException {

    public GetKerberosException() {
    }

    public GetKerberosException(String message) {
        super(message);
    }

    public GetKerberosException(String message, Throwable cause) {
        super(message, cause);
    }

    public GetKerberosException(Throwable cause) {
        super(cause);
    }

    public GetKerberosException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
