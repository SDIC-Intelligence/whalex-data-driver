package com.meiya.whalex.db.template.document;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.meiya.whalex.annotation.DatabaseName;
import com.meiya.whalex.annotation.DbType;
import com.meiya.whalex.annotation.ExtendField;
import com.meiya.whalex.annotation.Host;
import com.meiya.whalex.annotation.Password;
import com.meiya.whalex.annotation.Port;
import com.meiya.whalex.annotation.Url;
import com.meiya.whalex.annotation.UserName;
import com.meiya.whalex.db.template.BaseDbConfTemplate;
import com.meiya.whalex.interior.db.constant.DbResourceEnum;
import lombok.*;
import org.apache.commons.lang3.StringUtils;

/**
 * Mongo 数据库配置模板
 *
 * @author 黄河森
 * @date 2020/1/15
 * @project whale-cloud-platformX
 */
@Getter
@Setter
@Builder
@AllArgsConstructor
@DbType(value = DbResourceEnum.mongodb)
public class MongoDbTemplate extends BaseDbConfTemplate {

    @Url
    private String url;

    /**
     * 服务地址
     */
    @Host
    private String serviceUrl;

    /**
     * 服务端口
     */
    @Port
    private String port;

    /**
     * 连接数据库
     */
    @DatabaseName
    private String database;

    /**
     * 认证数据库
     */
    @ExtendField(value = "authDatabase")
    private String authDatabase;

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
     * 密码是否加密
     */
    @ExtendField(value = "encrypt")
    private boolean encrypt = true;

    /**
     * 认证类型
     */
    @ExtendField(value = "authType")
    private AuthType authType;

    /**
     * 是否开启 SSL 连接
     */
    @ExtendField(value = "enableSSL")
    private boolean enableSSL;

    /**
     * 是否校验域名
     */
    @ExtendField(value = "sslInvalidHostNameAllowed")
    private boolean sslInvalidHostNameAllowed;

    /**
     * 信任库路径
     */
    @ExtendField(value = "trustStorePath")
    private String trustStorePath;

    /**
     * 信任库秘钥
     */
    @ExtendField(value = "trustStorePassword")
    private String trustStorePassword;

    /**
     * 信任证书
     */
    @ExtendField(value = "keyStorePath")
    private String keyStorePath;

    /**
     * 信任证书秘钥
     */
    @ExtendField(value = "keyStorePassword")
    private String keyStorePassword;

    @ExtendField(value = "isURIConnect")
    private Boolean isURIConnect;

    @ExtendField(value = "clusterType")
    private ClusterType clusterType = ClusterType.STANDALONE;

    public MongoDbTemplate() {
    }

    public MongoDbTemplate(String serviceUrl, String port, String database, String authDatabase, String userName, String password) {
        this.serviceUrl = serviceUrl;
        this.port = port;
        this.database = database;
        this.authDatabase = authDatabase;
        this.userName = userName;
        this.password = password;
    }

    public MongoDbTemplate(String serviceUrl, String port, String database, String authDatabase, String userName, String password, boolean encrypt) {
        this.serviceUrl = serviceUrl;
        this.port = port;
        this.database = database;
        this.authDatabase = authDatabase;
        this.userName = userName;
        this.password = password;
        this.encrypt = encrypt;
    }

    public MongoDbTemplate(String serviceUrl, String port, String database, String authDatabase, String userName, String password, boolean encrypt, AuthType authType) {
        this.serviceUrl = serviceUrl;
        this.port = port;
        this.database = database;
        this.authDatabase = authDatabase;
        this.userName = userName;
        this.password = password;
        this.encrypt = encrypt;
        this.authType = authType;
    }

    public static enum AuthType {
        MONGODB_CR("MONGODB-CR"),
        SCRAM_SHA_1("SCRAM-SHA-1"),
        NORMAL(null),
        ;

        private final String mechanismName;

        AuthType(String mechanismName) {
            this.mechanismName = mechanismName;
        }

        @JsonValue
        public String getMechanismName() {
            return mechanismName;
        }
    }

    public enum ClusterType {
        STANDALONE,
        REPLICA_SET,
        SHARDED;

        public static ClusterType getEnumByValue(String value) {

            if(StringUtils.isBlank(value)) {
                return null;
            }

            ClusterType[] values = ClusterType.values();
            for (ClusterType ps : values) {
                if(ps.getName().equals(value)) {
                    return ps;
                }
            }
            throw new RuntimeException("未知的集群模式" + value);
        }

        @JsonCreator
        public static ClusterType getEnum(String name) {
            for (ClusterType item : values()) {
                if (item.name().equals(name)) {
                    return item;
                }
            }
            return null;
        }

        @JsonValue
        public String getName() {
            return name();
        }
    }
}
