package com.meiya.whalex.db.entity.bigtable;

import com.meiya.whalex.db.entity.AbstractDatabaseInfo;

/**
 * HBase 数据库配置
 *
 * @author 黄河森
 * @date 2019/12/16
 * @project whale-cloud-platformX
 */
public class HBaseDatabaseInfo extends AbstractDatabaseInfo {

    public static final String REUSABLE_IPC_POOL_TYPE = "Reusable";
    public static final String ROUND_ROBIN_IPC_POOL_TYPE = "RoundRobin";
    public static final String THREAD_LOCAL_IPC_POOL_TYPE = "ThreadLocal";

    /**
     * zookeeper地址
     */
    private String zookeeperAddr;

    /**
     * 认证类型
     */
    private String authType;

    /**
     * 认证证书
     */
    private String certificateId;

    /**
     * 用户名
     */
    private String userName;
    /**
     * krb5 地址
     */
    private String krb5Path;
    /**
     * userKeytab 地址
     */
    private String userKeytabPath;

    /**
     * kerberos.principal
     */
    private String kerberosPrincipal;

    /**
     * zk 目录配置
     */
    private String zkNodeParent;

    /**
     * 安全SASL连接保护
     */
    private String rpcProtection;

    /**
     * 从数据库下载证书时，保存的父级目录
     */
    private String parentDir;

    /**
     * 命名空间
     */
    private String namespace;

    /**
     * zk Principal
     */
    private String zookeeperPrincipal = "zookeeper/hadoop.hadoop.com";

    private String certificateBase64Str;

    /**
     * 资源池类型
     * 可选值：Reusable、RoundRobin、ThreadLocal
     */
    private String ipcPollType;

    public String getCertificateBase64Str() {
        return certificateBase64Str;
    }

    public void setCertificateBase64Str(String certificateBase64Str) {
        this.certificateBase64Str = certificateBase64Str;
    }

    public String getZookeeperAddr() {
        return zookeeperAddr;
    }

    public void setZookeeperAddr(String zookeeperAddr) {
        this.zookeeperAddr = zookeeperAddr;
    }

    public String getAuthType() {
        return authType;
    }

    public void setAuthType(String authType) {
        this.authType = authType;
    }

    public String getCertificateId() {
        return certificateId;
    }

    public void setCertificateId(String certificateId) {
        this.certificateId = certificateId;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getKrb5Path() {
        return krb5Path;
    }

    public void setKrb5Path(String krb5Path) {
        this.krb5Path = krb5Path;
    }

    public String getUserKeytabPath() {
        return userKeytabPath;
    }

    public void setUserKeytabPath(String userKeytabPath) {
        this.userKeytabPath = userKeytabPath;
    }

    @Override
    public String getServerAddr() {
        return zookeeperAddr;
    }

    @Override
    public String getDbName() {
        return null;
    }

    public String getKerberosPrincipal() {
        return kerberosPrincipal;
    }

    public void setKerberosPrincipal(String kerberosPrincipal) {
        this.kerberosPrincipal = kerberosPrincipal;
    }

    public String getZkNodeParent() {
        return zkNodeParent;
    }

    public void setZkNodeParent(String zkNodeParent) {
        this.zkNodeParent = zkNodeParent;
    }

    public String getRpcProtection() {
        return rpcProtection;
    }

    public void setRpcProtection(String rpcProtection) {
        this.rpcProtection = rpcProtection;
    }

    public String getParentDir() {
        return parentDir;
    }

    public void setParentDir(String parentDir) {
        this.parentDir = parentDir;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public String getZookeeperPrincipal() {
        return zookeeperPrincipal;
    }

    public void setZookeeperPrincipal(String zookeeperPrincipal) {
        this.zookeeperPrincipal = zookeeperPrincipal;
    }

    public String getIpcPollType() {
        return ipcPollType;
    }

    public void setIpcPollType(String ipcPollType) {
        this.ipcPollType = ipcPollType;
    }
}
