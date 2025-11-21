/*
package com.meiya.whalex.db.util.helper.impl.graph;

import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

*/
/**
 * @author 黄河森
 * @date 2020/9/24
 * @project whalex-data-driver
 *//*

@Slf4j
public class KerberosLoginUtil {

    */
/**
     * line operator string
     *//*

    private static final String LINE_SEPARATOR = System.getProperty("line.separator");

    */
/**
     * jaas file postfix
     *//*

    private static final String JAAS_POSTFIX = ".jaas.conf";

    */
/**
     * IBM jdk login module
     *//*

    private static final String IBM_LOGIN_MODULE = "com.ibm.security.auth.module.Krb5LoginModule required";

    */
/**
     * oracle jdk login module
     *//*

    private static final String SUN_LOGIN_MODULE = "com.sun.security.auth.module.Krb5LoginModule required";

    */
/**
     * java security login file path
     *//*

    public static final String JAVA_SECURITY_LOGIN_CONF_KEY = "java.security.auth.login.config";

    private static final String JAVA_SECURITY_KRB5_CONF_KEY = "java.security.krb5.conf";

    private static final String ZOOKEEPER_SERVER_PRINCIPAL_KEY = "zookeeper.server.principal";

    */
/**
     * 登录异常提示
     *//*

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

    */
/**
     * JDK 版本
     *//*

    private static final boolean IS_IBM_JDK = System.getProperty("java.vendor").contains("IBM");

    */
/**
     * 设置 krb5 认证文件
     *
     * @param krb5ConfFile
     * @throws IOException
     *//*

    public static void setKrb5Config(String krb5ConfFile) throws IOException {
        System.setProperty(JAVA_SECURITY_KRB5_CONF_KEY, krb5ConfFile);
        String ret = System.getProperty(JAVA_SECURITY_KRB5_CONF_KEY);
        if (ret == null) {
            log.error(JAVA_SECURITY_KRB5_CONF_KEY + " is null.");
            throw new IOException(JAVA_SECURITY_KRB5_CONF_KEY + " is null.");
        }
        if (!ret.equals(krb5ConfFile)) {
            log.error(JAVA_SECURITY_KRB5_CONF_KEY + " is " + ret + " is not " + krb5ConfFile + ".");
            throw new IOException(JAVA_SECURITY_KRB5_CONF_KEY + " is " + ret + " is not " + krb5ConfFile + ".");
        }
    }

    */
/**
     * 设置 zk principal
     *
     * @param zkServerPrincipal
     * @throws IOException
     *//*

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

    */
/**
     * 初始化 jaas 文件
     *
     * @param principal
     * @param keytabPath
     *//*

    public static void setJaasFile(String principal, String keytabPath)
            throws IOException {
        String jaasPath = new File(System.getProperty("java.io.tmpdir")) + File.separator + System.getProperty("user.name") + JAAS_POSTFIX;
        log.info("GraphBase Client jaasPath: [{}]", jaasPath);
        // windows路径下分隔符替换
        jaasPath = jaasPath.replace("\\", "\\\\");
        keytabPath = keytabPath.replace("\\", "\\\\");
        // 删除jaas文件
        deleteJaasFile(jaasPath);
        writeJaasFile(jaasPath, principal, keytabPath);
        System.setProperty(JAVA_SECURITY_LOGIN_CONF_KEY, jaasPath);
    }

    */
/**
     * 删除路径下的 jaas 文件
     * @param jaasPath
     * @throws IOException
     *//*

    private static void deleteJaasFile(String jaasPath)
            throws IOException {
        File jaasFile = new File(jaasPath);
        if (jaasFile.exists()) {
            if (!jaasFile.delete()) {
                throw new IOException("Failed to delete exists jaas file.");
            }
        }
    }

    */
/**
     * 创建写入 jaas 文件
     *
     * @param jaasPath
     * @param principal
     * @param keytabPath
     * @throws IOException
     *//*

    private static void writeJaasFile(String jaasPath, String principal, String keytabPath)
            throws IOException {
        FileWriter writer = new FileWriter(new File(jaasPath));
        try {
            String jaasConfContext = getJaasConfContext(principal, keytabPath);
            log.info("GraphBase Client jaas content: [{}]", jaasConfContext);
            writer.write(jaasConfContext);
            writer.flush();
        } catch (IOException e) {
            throw new IOException("Failed to create jaas.conf File");
        } finally {
            writer.close();
        }
    }

    */
/**
     * 构建 jaas 文件内容
     *
     * @param principal
     * @param keytabPath
     * @return
     *//*

    private static String getJaasConfContext(String principal, String keytabPath) {
        Module[] allModule = Module.values();
        StringBuilder builder = new StringBuilder();
        for (Module module : allModule) {
            builder.append(getModuleContext(principal, keytabPath, module));
        }
        return builder.toString();
    }

    */
/**
     * 获取 jaas 文件 模块内容串
     *
     * @param userPrincipal
     * @param keyTabPath
     * @param module
     * @return
     *//*

    private static String getModuleContext(String userPrincipal, String keyTabPath, Module module) {
        StringBuilder builder = new StringBuilder();
        if (IS_IBM_JDK) {
            builder.append(module.getName()).append(" {").append(LINE_SEPARATOR);
            builder.append(IBM_LOGIN_MODULE).append(LINE_SEPARATOR);
            builder.append("credsType=both").append(LINE_SEPARATOR);
            builder.append("principal=\"" + userPrincipal + "\"").append(LINE_SEPARATOR);
            builder.append("useKeytab=\"" + keyTabPath + "\"").append(LINE_SEPARATOR);
            builder.append("debug=true;").append(LINE_SEPARATOR);
            builder.append("};").append(LINE_SEPARATOR);
        } else {
            builder.append(module.getName()).append(" {").append(LINE_SEPARATOR);
            builder.append(SUN_LOGIN_MODULE).append(LINE_SEPARATOR);
            builder.append("useKeyTab=true").append(LINE_SEPARATOR);
            builder.append("keyTab=\"" + keyTabPath + "\"").append(LINE_SEPARATOR);
            builder.append("principal=\"" + userPrincipal + "\"").append(LINE_SEPARATOR);
            builder.append("useTicketCache=false").append(LINE_SEPARATOR);
            builder.append("storeKey=true").append(LINE_SEPARATOR);
            builder.append("debug=true;").append(LINE_SEPARATOR);
            builder.append("};").append(LINE_SEPARATOR);
        }

        return builder.toString();
    }

    */
/**
     * Jaas 文件模块
     *//*

    public static enum Module {
        GraphBase("gremlinclient");

        private String name;

        private Module(String name) {
            this.name = name;
        }

        public String getName() {
            return this.name;
        }
    }
}
*/
