package com.meiya.whalex.db.entity.lucene;

import com.meiya.whalex.db.entity.AbstractDatabaseInfo;
import org.apache.commons.lang3.StringUtils;

/**
 * 数据库配置
 *
 * @author 黄河森
 * @date 2019/12/19
 * @project whale-cloud-platformX
 */
public class EsDatabaseInfo extends AbstractDatabaseInfo {

    public static final String KERBEROS_AUTH = "cer";
    public static final String X_PACK_AUTH = "xpack";
    public static final String CUSTOM_PROXY_AUTH = "custom-proxy-username";
    public static final String SEARCH_GUARD_AUTH = "search-guard";

    /**
     * es 节点地址
     */
    private String serverAddress;

    /**
     * 查询节点
     */
    private String queryServerAddress;

    /**
     * 认证类型
     */
    private String authType;

    /**
     * 证书编号
     */
    private String certificateId;

    /**
     * 请求协议
     */
    private String requestType;

    /**
     * 前置负载均衡
     */
    private boolean preLoadBalancing;

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
     * 证书保存的目录
     */
    private String parentDir;

    /**
     * xpack 认证密码
     */
    private String password;

    private String certificateBase64Str;

    private boolean openParallel = true;

    public boolean isOpenParallel() {
        return openParallel;
    }

    public void setOpenParallel(boolean openParallel) {
        this.openParallel = openParallel;
    }

    public String getCertificateBase64Str() {
        return certificateBase64Str;
    }

    public void setCertificateBase64Str(String certificateBase64Str) {
        this.certificateBase64Str = certificateBase64Str;
    }

    public String getServerAddress() {
        return serverAddress;
    }

    public void setServerAddress(String serverAddress) {
        this.serverAddress = serverAddress;
    }

    public String getQueryServerAddress() {
        return queryServerAddress;
    }

    public void setQueryServerAddress(String queryServerAddress) {
        this.queryServerAddress = queryServerAddress;
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

    public String getRequestType() {
        return requestType;
    }

    public void setRequestType(String requestType) {
        this.requestType = requestType;
    }

    public boolean isPreLoadBalancing() {
        return preLoadBalancing;
    }

    public void setPreLoadBalancing(boolean preLoadBalancing) {
        this.preLoadBalancing = preLoadBalancing;
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

    public String getKerberosPrincipal() {
        return kerberosPrincipal;
    }

    public void setKerberosPrincipal(String kerberosPrincipal) {
        this.kerberosPrincipal = kerberosPrincipal;
    }

    @Override
    public String getServerAddr() {
        if (StringUtils.isNotBlank(queryServerAddress)) {
            return queryServerAddress;
        } else {
            return serverAddress;
        }
    }

    @Override
    public String getDbName() {
        return null;
    }

    public String getParentDir() {
        return parentDir;
    }

    public void setParentDir(String parentDir) {
        this.parentDir = parentDir;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }
}
