package com.meiya.whalex.db.entity.document;

import com.meiya.whalex.db.entity.AbstractDatabaseInfo;
import com.mongodb.AuthenticationMechanism;
import com.mongodb.connection.ClusterType;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

/**
 * MONGO 组件数据库配置信息
 *
 * @author 黄河森
 * @date 2019/9/19
 * @project whale-cloud-platformX
 */
public class MongoDatabaseInfo extends AbstractDatabaseInfo {

    private List<String> serverUrl;

    private String userName;

    private String password;

    private String authDatabase;

    private String databaseName;

    /**
     * 密码是否加密
     */
    private boolean isEncrypt = true;

    /**
     * 是否伪集群
     */
    private boolean isPseudoCluster = Boolean.FALSE;

    /**
     * 集群模式
     */
    private ClusterType clusterType;

    /**
     * 认证方式
     */
    private AuthenticationMechanism authenticationMechanism;

    /**
     * 是否开启 SSL 连接
     */
    private boolean enableSSL = Boolean.FALSE;

    /**
     * 是否不校验域名
     */
    private boolean sslInvalidHostNameAllowed = Boolean.FALSE;

    /**
     * 信任库路径
     */
    private String trustStorePath;

    /**
     * 信任库秘钥
     */
    private String trustStorePassword;

    /**
     * 信任证书
     */
    private String keyStorePath;

    /**
     * 信任证书秘钥
     */
    private String keyStorePassword;

    /**
     *  是否使用 MongoClientURI
     */
    private boolean isURIConnect = Boolean.FALSE;

    public List<String> getServerUrl() {
        return serverUrl;
    }

    public void setServerUrl(List<String> serverUrl) {
        this.serverUrl = serverUrl;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getAuthDatabase() {
        return authDatabase;
    }

    public void setAuthDatabase(String authDatabase) {
        this.authDatabase = authDatabase;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    @Override
    public String getServerAddr() {
        if (CollectionUtils.isNotEmpty(serverUrl)) {
            return StringUtils.replaceEach(serverUrl.toString(), new String[]{"[","]"}, new String[]{"",""});
        } else {
            return null;
        }
    }

    @Override
    public String getDbName() {
        return databaseName;
    }

    public boolean isEncrypt() {
        return isEncrypt;
    }

    public void setEncrypt(boolean encrypt) {
        isEncrypt = encrypt;
    }

    public boolean isPseudoCluster() {
        return isPseudoCluster;
    }

    public void setPseudoCluster(boolean pseudoCluster) {
        isPseudoCluster = pseudoCluster;
    }

    public AuthenticationMechanism getAuthenticationMechanism() {
        return authenticationMechanism;
    }

    public void setAuthenticationMechanism(AuthenticationMechanism authenticationMechanism) {
        this.authenticationMechanism = authenticationMechanism;
    }

    public boolean isEnableSSL() {
        return enableSSL;
    }

    public void setEnableSSL(boolean enableSSL) {
        this.enableSSL = enableSSL;
    }

    public String getTrustStorePath() {
        return trustStorePath;
    }

    public void setTrustStorePath(String trustStorePath) {
        this.trustStorePath = trustStorePath;
    }

    public String getTrustStorePassword() {
        return trustStorePassword;
    }

    public void setTrustStorePassword(String trustStorePassword) {
        this.trustStorePassword = trustStorePassword;
    }

    public String getKeyStorePath() {
        return keyStorePath;
    }

    public void setKeyStorePath(String keyStorePath) {
        this.keyStorePath = keyStorePath;
    }

    public String getKeyStorePassword() {
        return keyStorePassword;
    }

    public void setKeyStorePassword(String keyStorePassword) {
        this.keyStorePassword = keyStorePassword;
    }

    public boolean isSslInvalidHostNameAllowed() {
        return sslInvalidHostNameAllowed;
    }

    public void setSslInvalidHostNameAllowed(boolean sslInvalidHostNameAllowed) {
        this.sslInvalidHostNameAllowed = sslInvalidHostNameAllowed;
    }

    public boolean isURIConnect() {
        return isURIConnect;
    }

    public void setURIConnect(boolean URIConnect) {
        isURIConnect = URIConnect;
    }

    public ClusterType getClusterType() {
        return clusterType;
    }

    public void setClusterType(ClusterType clusterType) {
        this.clusterType = clusterType;
    }
}
