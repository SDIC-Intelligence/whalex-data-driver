package com.meiya.whalex.db.kerberos;

import cn.hutool.core.io.FileUtil;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;

/**
 * 设置 kerberos 认证环境参数
 *
 * @author 黄河森
 * @date 2023/5/29
 * @package com.meiya.whalex.db.kerberos
 * @project whalex-data-driver
 */
@Slf4j
public class KerberosEnvConfigurationUtil {

    /**
     * line operator string
     */
    private static final String LINE_SEPARATOR = System.getProperty("line.separator");

    /**
     * jaas file postfix
     */
    private static final String JAAS_POSTFIX = ".jaas.conf";

    /**
     * IBM jdk login module
     */
    private static final String IBM_LOGIN_MODULE = "com.ibm.security.auth.module.Krb5LoginModule required";

    /**
     * oracle jdk login module
     */
    private static final String SUN_LOGIN_MODULE = "com.sun.security.auth.module.Krb5LoginModule required";

    /**
     * java security login file path
     */
    public static final String JAVA_SECURITY_LOGIN_CONF_KEY = "java.security.auth.login.config";

    public static final String JAVA_SECURITY_KRB5_CONF_KEY = "java.security.krb5.conf";

    private static final String ZOOKEEPER_SERVER_PRINCIPAL_KEY = "zookeeper.server.principal";

    /**
     * 登录异常提示
     */
    private static final String logIN_FAILED_CAUSE_PASSWORD_WRONG =
            "(wrong password) keytab file and user not match, you can kinit -k -t keytab user in client server to check";

    private static final String logIN_FAILED_CAUSE_TIME_WRONG =
            "(clock skew) time of local server and remote server not match, please check ntp to remote server";

    private static final String logIN_FAILED_CAUSE_AES256_WRONG =
            "(aes256 not support) aes256 not support by default jdk/jre, need copy local_policy.jar and US_export_policy.jar from remote server in path /opt/huawei/Bigdata/jdk/jre/lib/security";

    private static final String logIN_FAILED_CAUSE_PRINCIPAL_WRONG =
            "(no rule) principal format not support by default, need add property hadoop.security.auth_to_local(in core-site.xml) value RULE:[1:$1] RULE:[2:$1]";

    private static final String logIN_FAILED_CAUSE_TIME_OUT =
            "(time out) can not connect to kdc server or there is fire wall in the network";

    /**
     * JDK 版本
     */
    private static final boolean IS_IBM_JDK = System.getProperty("java.vendor").contains("IBM");

    /**
     * 设置 krb5 认证文件
     *
     * @param krb5ConfPath
     * @param krb5Conf
     * @throws IOException
     */
    public static void setKrb5Config(String krb5ConfPath, byte[] krb5Conf) throws IOException {
        // check file
        File krb5ConfFile = new File(krb5ConfPath);
        if (!krb5ConfFile.exists()) {
            if (krb5Conf != null) {
                FileUtil.mkParentDirs(krb5ConfPath);
                FileUtil.writeBytes(krb5Conf, krb5ConfFile);
                krb5ConfFile = new File(krb5ConfPath);
                log.error("krb5ConfFile(" + krb5ConfFile.getAbsolutePath() + ") does not exists and anew file!");
            } else {
                log.error("krb5ConfFile(" + krb5ConfFile.getAbsolutePath() + ") does not exists.");
                throw new IOException("krb5ConfFile(" + krb5ConfFile.getAbsolutePath() + ") does not exists.");
            }
        }
        if (!krb5ConfFile.isFile()) {
            log.error("krb5ConfFile(" + krb5ConfFile.getAbsolutePath() + ") is not a file.");
            throw new IOException("krb5ConfFile(" + krb5ConfFile.getAbsolutePath() + ") is not a file.");
        }
        // set property
        System.setProperty(JAVA_SECURITY_KRB5_CONF_KEY, krb5ConfPath);
        String ret = System.getProperty(JAVA_SECURITY_KRB5_CONF_KEY);
        if (ret == null) {
            log.error(JAVA_SECURITY_KRB5_CONF_KEY + " is null.");
            throw new IOException(JAVA_SECURITY_KRB5_CONF_KEY + " is null.");
        }
        if (!ret.equals(krb5ConfPath)) {
            log.error(JAVA_SECURITY_KRB5_CONF_KEY + " is " + ret + " is not " + krb5ConfPath + ".");
            throw new IOException(JAVA_SECURITY_KRB5_CONF_KEY + " is " + ret + " is not " + krb5ConfPath + ".");
        }
    }

    /**
     * 设置 zk principal
     *
     * @param zkServerPrincipal
     * @throws IOException
     */
    public static void setZookeeperServerPrincipal(String zkServerPrincipal) throws IOException {
        System.setProperty(ZOOKEEPER_SERVER_PRINCIPAL_KEY, zkServerPrincipal);
        String ret = System.getProperty(ZOOKEEPER_SERVER_PRINCIPAL_KEY);
        if (ret == null)
        {
            log.error(ZOOKEEPER_SERVER_PRINCIPAL_KEY + " is null.");
            throw new IOException(ZOOKEEPER_SERVER_PRINCIPAL_KEY + " is null.");
        }
        if (!ret.equals(zkServerPrincipal))
        {
            log.error(ZOOKEEPER_SERVER_PRINCIPAL_KEY + " is " + ret + " is not " + zkServerPrincipal
                    + ".");
            throw new IOException(ZOOKEEPER_SERVER_PRINCIPAL_KEY + " is " + ret + " is not "
                    + zkServerPrincipal + ".");
        }
    }

}
