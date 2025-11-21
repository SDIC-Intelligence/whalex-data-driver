package com.meiya.whalex.db.util.helper.impl.lucene;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.X509ExtendedTrustManager;
import java.net.Socket;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

/**
 * @author 黄河森
 * @date 2022/5/26
 * @package com.meiya.whalex.db.util.helper.impl.lucene
 * @project whalex-data-driver
 */
public class UnsafeX509ExtendedTrustManager extends X509ExtendedTrustManager {

    private static final X509ExtendedTrustManager INSTANCE = new UnsafeX509ExtendedTrustManager();

    private static final X509Certificate[] EMPTY_CERTIFICATES = new X509Certificate[0];

    private UnsafeX509ExtendedTrustManager() {
    }

    public static X509ExtendedTrustManager getInstance() {
        return INSTANCE;
    }

    @Override
    public void checkClientTrusted(X509Certificate[] x509Certificates, String s, Socket socket) throws CertificateException {

    }

    @Override
    public void checkServerTrusted(X509Certificate[] x509Certificates, String s, Socket socket) throws CertificateException {

    }

    @Override
    public void checkClientTrusted(X509Certificate[] x509Certificates, String s, SSLEngine sslEngine) throws CertificateException {

    }

    @Override
    public void checkServerTrusted(X509Certificate[] x509Certificates, String s, SSLEngine sslEngine) throws CertificateException {

    }

    @Override
    public void checkClientTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {

    }

    @Override
    public void checkServerTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {

    }

    @Override
    public X509Certificate[] getAcceptedIssuers() {
        return EMPTY_CERTIFICATES;
    }
}
