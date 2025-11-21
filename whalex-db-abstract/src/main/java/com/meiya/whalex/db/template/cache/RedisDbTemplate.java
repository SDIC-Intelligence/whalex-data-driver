package com.meiya.whalex.db.template.cache;

import com.meiya.whalex.annotation.DatabaseName;
import com.meiya.whalex.annotation.DbType;
import com.meiya.whalex.annotation.ExtendField;
import com.meiya.whalex.annotation.Password;
import com.meiya.whalex.annotation.Url;
import com.meiya.whalex.db.template.BaseDbConfTemplate;
import com.meiya.whalex.interior.db.constant.DbResourceEnum;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Redis 数据库配置模板
 * @author chenjp
 * @date 2020/9/9
 */
@Data
@NoArgsConstructor
@Builder
@AllArgsConstructor
@DbType(value = {DbResourceEnum.redis})
public class RedisDbTemplate extends BaseDbConfTemplate {

    /**
     * 服务地址
     */
    @Url
    private String serviceUrl;

    @ExtendField(value = "userName")
    private String userName;

    /**
     * 密码
     */
    @Password
    private String password;

    /**
     * 数据库索引
     */
    @DatabaseName
    private Integer dbIndex;

    /**
     * 认证证书
     */
    @ExtendField(value = "certificateId")
    private String certificateId;

    @ExtendField(value = "authType")
    private String authType;

    @ExtendField(value = "userKeytabPath")
    private String userKeytabPath;


    @ExtendField(value = "parentPath")
    private String parentPath;

    @ExtendField(value = "certificateBase64Str")
    private String certificateBase64Str;

    @ExtendField(value = "krb5Path")
    private String krb5Path;

    @ExtendField(value = "useLock")
    private boolean useLock;

    public RedisDbTemplate(String serviceUrl){
        this.serviceUrl = serviceUrl;
    }

    public RedisDbTemplate(String serviceUrl, String password, String certificateId) {
        this.serviceUrl = serviceUrl;
        this.password = password;
        this.certificateId = certificateId;
    }

    public RedisDbTemplate(String serviceUrl, String userName, String password, String certificateId, String authType, String userKeytabPath, String parentPath, String certificateBase64Str, String krb5Path) {
        this.serviceUrl = serviceUrl;
        this.userName = userName;
        this.password = password;
        this.certificateId = certificateId;
        this.authType = authType;
        this.userKeytabPath = userKeytabPath;
        this.parentPath = parentPath;
        this.certificateBase64Str = certificateBase64Str;
        this.krb5Path = krb5Path;
    }

    public RedisDbTemplate(String serviceUrl, String password) {
        this.serviceUrl = serviceUrl;
        this.password = password;
    }
}
