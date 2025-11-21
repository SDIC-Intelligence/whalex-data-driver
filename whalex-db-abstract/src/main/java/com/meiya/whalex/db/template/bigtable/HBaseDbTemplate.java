package com.meiya.whalex.db.template.bigtable;

import com.meiya.whalex.annotation.*;
import com.meiya.whalex.db.template.BaseDbConfTemplate;
import com.meiya.whalex.interior.db.constant.DbResourceEnum;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

/**
 * @author 黄河森
 * @date 2020/7/1
 * @project whalex-data-driver
 */
@Data
@Builder
@AllArgsConstructor
@DbType(value = {DbResourceEnum.hbase, DbResourceEnum.hbase})
public class HBaseDbTemplate extends BaseDbConfTemplate {

    public static final String REUSABLE_IPC_POOL_TYPE = "Reusable";
    public static final String ROUND_ROBIN_IPC_POOL_TYPE = "RoundRobin";
    public static final String THREAD_LOCAL_IPC_POOL_TYPE = "ThreadLocal";

    @Url
    private String serviceUrl;

    /**
     * 认证类型
     */
    @ExtendField(value = "authType")
    private String authType;

    /**
     * 认证证书
     */
    @ExtendField(value = "certificateId")
    private String certificateId;

    /**
     * 用户名
     */
    @UserName
    private String userName;
    /**
     * krb5 地址
     */
    @ExtendField(value = "krb5Path")
    private String krb5Path;
    /**
     * userKeytab 地址
     */
    @ExtendField(value = "userKeytabPath")
    private String userKeytabPath;

    @ExtendField(value = "zkNodeParent")
    private String zkNodeParent;
    /**
     * 腾讯认证-secureid
     */
    @ExtendField(value = "secureid")
    private String secureid;
    /**
     * 腾讯认证-securekey
     */
    @ExtendField(value = "securekey")
    private String securekey;

    /**
     * 命名空间
     */
    @DatabaseName
    private String namespace;

    /**
     * 顶层目录
     */
    @ExtendField(value = "parentDir")
    private String parentDir;
    @ExtendField(value = "rpcProtection")
    private String rpcProtection;
    @ExtendField(value = "kerberosPrincipal")
    private String kerberosPrincipal;

    /**
     * zookeeperPrincipal
     */
    @ExtendField(value = "zookeeperPrincipal")
    private String zookeeperPrincipal;

    @ExtendField(value = "certificateBase64Str")
    private String certificateBase64Str;

    /**
     * 资源池类型
     * 可选值：Reusable、RoundRobin、ThreadLocal
     */
    @ExtendField(value = "ipcPollType")
    private String ipcPollType;

    public HBaseDbTemplate() {
    }

    public HBaseDbTemplate(String serviceUrl, String certificateId, String authType, String krb5Path, String userKeytabPath) {
        this.serviceUrl = serviceUrl;
        this.certificateId = certificateId;
        this.authType = authType;
        this.krb5Path = krb5Path;
        this.userKeytabPath = userKeytabPath;
    }

    public HBaseDbTemplate(String serviceUrl, String certificateId, String authType, String krb5Path, String userKeytabPath, String userName) {
        this.serviceUrl = serviceUrl;
        this.certificateId = certificateId;
        this.authType = authType;
        this.krb5Path = krb5Path;
        this.userKeytabPath = userKeytabPath;
        this.userName = userName;
    }

    public HBaseDbTemplate(String serviceUrl, String certificateId, String authType, String krb5Path, String userKeytabPath, String userName, String zkNodeParent, String secureid, String securekey) {
        this.serviceUrl = serviceUrl;
        this.certificateId = certificateId;
        this.authType = authType;
        this.krb5Path = krb5Path;
        this.userKeytabPath = userKeytabPath;
        this.userName = userName;
        this.zkNodeParent = zkNodeParent;
        this.secureid = secureid;
        this.securekey = securekey;
    }

    public HBaseDbTemplate(String serviceUrl, String authType, String certificateId, String userName, String krb5Path, String userKeytabPath, String zkNodeParent, String secureid, String securekey, String namespace) {
        this.serviceUrl = serviceUrl;
        this.authType = authType;
        this.certificateId = certificateId;
        this.userName = userName;
        this.krb5Path = krb5Path;
        this.userKeytabPath = userKeytabPath;
        this.zkNodeParent = zkNodeParent;
        this.secureid = secureid;
        this.securekey = securekey;
        this.namespace = namespace;
    }
}
