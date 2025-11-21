package com.meiya.whalex.db.template.dwh;

import com.fasterxml.jackson.annotation.JsonValue;
import com.meiya.whalex.annotation.*;
import com.meiya.whalex.db.template.BaseDbConfTemplate;
import com.meiya.whalex.interior.db.constant.DbResourceEnum;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;

/**
 * @author 黄河森
 * @date 2020/7/6
 * @project whalex-data-driver
 */
@Data
@Builder
@AllArgsConstructor
@DbType(value = {DbResourceEnum.hive})
public class HiveDbTemplate extends BaseDbConfTemplate {
    /**
     * ip:port
     */
    @Url
    private String serviceUrl;
    /**
     * 库名
     */
    @DatabaseName
    private String database;
    /**
     * 认证证书
     */
    @ExtendField(value = "certificateId")
    private String certificateId;
    /**
     * 认证类型(如果是认证文件认证,填写cer)
     */
    @ExtendField(value = "authType")
    private String authType;
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

    /**
     * principal
     */
    @ExtendField(value = "kerberosPrincipal")
    private String kerberosPrincipal;

    /**
     * zk 命名空间
     */
    @ExtendField(value = "zooKeeperNamespace")
    private String zooKeeperNamespace;

    /**
     * 用户名
     */
    @UserName
    private String userName;

    /**
     * 密码
     */
    @Password
    private String password;

    /**
     * 连接方式
     */
    @ExtendField(value = "linkType")
    private LinkType linkType;

    /**
     * 任务引擎
     */
    @ExtendField(value = "jobEngine")
    private JobEngine jobEngine;

    /**
     * 任务队列名称
     */
    @ExtendField(value = "queueName")
    private String queueName;

    @ExtendField(value = "certificateBase64Str")
    private String certificateBase64Str;

    @ExtendField(value = "metaStoreUrl")
    private String metaStoreUrl;

    @ExtendField(value = "metaStoreMaxConnectionCount")
    private String metaStoreMaxConnectionCount;

    @ExtendField(value = "metaStoreExecuteSetugi")
    private String metaStoreExecuteSetugi;

    public HiveDbTemplate() {
    }

    public HiveDbTemplate(String serviceUrl, String database, String certificateId, String authType, String krb5Path, String userKeytabPath, String userName, String password) {
        this.serviceUrl = serviceUrl;
        this.database = database;
        this.certificateId = certificateId;
        this.authType = authType;
        this.krb5Path = krb5Path;
        this.userKeytabPath = userKeytabPath;
        this.userName = userName;
        this.password = password;
    }

    /**
     * 认证文件认证模式
     * @param serviceUrl
     * @param database
     * @param certificateId
     * @param authType
     * @param krb5Path
     * @param userKeytabPath
     * @param userName
     */
    public HiveDbTemplate(String serviceUrl, String database, String certificateId, String authType, String krb5Path, String userKeytabPath, String userName) {
        this.serviceUrl = serviceUrl;
        this.database = database;;
        this.certificateId = certificateId;
        this.authType = authType;
        this.krb5Path = krb5Path;
        this.userKeytabPath = userKeytabPath;
        this.userName = userName;
    }

    /**
     * 账号密码认证模式
     * @param serviceUrl
     * @param database
     * @param userName
     * @param password
     */
    public HiveDbTemplate(String serviceUrl, String database, String userName, String password) {
        this.serviceUrl = serviceUrl;
        this.database = database;
        this.userName = userName;
        this.password = password;
    }

    public HiveDbTemplate(String serviceUrl, String database, String certificateId, String authType, String krb5Path, String userKeytabPath, String kerberosPrincipal, String zooKeeperNamespace, String userName, String password) {
        this.serviceUrl = serviceUrl;
        this.database = database;
        this.certificateId = certificateId;
        this.authType = authType;
        this.krb5Path = krb5Path;
        this.userKeytabPath = userKeytabPath;
        this.kerberosPrincipal = kerberosPrincipal;
        this.zooKeeperNamespace = zooKeeperNamespace;
        this.userName = userName;
        this.password = password;
    }

    public HiveDbTemplate(String serviceUrl, String database, String certificateId, String authType, String krb5Path, String userKeytabPath, String kerberosPrincipal, String zooKeeperNamespace, String userName, String password, LinkType linkType) {
        this.serviceUrl = serviceUrl;
        this.database = database;
        this.certificateId = certificateId;
        this.authType = authType;
        this.krb5Path = krb5Path;
        this.userKeytabPath = userKeytabPath;
        this.kerberosPrincipal = kerberosPrincipal;
        this.zooKeeperNamespace = zooKeeperNamespace;
        this.userName = userName;
        this.password = password;
        this.linkType = linkType;
    }

    /**
     * 连接方式
     */
    public enum LinkType {
        HIVE_SERVER("hiveserver"),
        ZK_LINK("zk")
        ;
        private String linkType;

        LinkType(String linkType) {
            this.linkType = linkType;
        }

        public static LinkType getEnumByValue(String value) {

            if(StringUtils.isBlank(value)) {
                return null;
            }

            LinkType[] values = LinkType.values();
            for (LinkType lt : values) {
                if(lt.linkType.equals(value)) {
                    return lt;
                }
            }
            throw new RuntimeException("未知的连接方式" + value);
        }

        @JsonValue
        public String getLinkType() {
            return linkType;
        }
    }

    public enum JobEngine {
        TEZ_JOB("tez"),
        YARN_JOB("yarn");
        private String name;

        JobEngine(String name) {
            this.name = name;
        }

        public static JobEngine getEnumByValue(String value) {

            if(StringUtils.isBlank(value)) {
                return null;
            }

            JobEngine[] values = JobEngine.values();
            for (JobEngine jobEngine : values) {
                if(jobEngine.name.equals(value)) {
                    return jobEngine;
                }
            }
            throw new RuntimeException("未知的工作引擎" + value);
        }

        @JsonValue
        public String getName() {
            return name;
        }
    }
}
