package com.meiya.whalex.db.template.lucene;

import com.meiya.whalex.annotation.DbType;
import com.meiya.whalex.annotation.ExtendField;
import com.meiya.whalex.annotation.Password;
import com.meiya.whalex.annotation.Url;
import com.meiya.whalex.annotation.UserName;
import com.meiya.whalex.db.template.BaseDbConfTemplate;
import com.meiya.whalex.interior.db.constant.DbResourceEnum;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

/**
 * es 数据配置模板
 *
 * @author 黄河森
 * @date 2020/1/15
 * @project whale-cloud-platformX
 */
@Getter
@Setter
@Builder
@AllArgsConstructor
@DbType(value = DbResourceEnum.es)
public class EsDbTemplate extends BaseDbConfTemplate {

    public static final String KERBEROS_AUTH = "cer";
    public static final String X_PACK_AUTH = "xpack";
    public static final String SEARCH_GUARD_AUTH = "search-guard";
    public static final String CUSTOM_PROXY_AUTH = "custom-proxy-username";

    /**
     * 请求方式
     */
    public static final String DEFAULT_REQUEST_TYPE = "http/https";
    public static final String HTTP_REQUEST_TYPE = "http";
    public static final String HTTPS_REQUEST_TYPE = "https";

    @Url
    private String serviceUrl;

    @ExtendField(value = "queryServiceUrl")
    private String queryServiceUrl;

    @ExtendField(value = "authType")
    private String authType;

    @ExtendField(value = "certificateId")
    private String certificateId;

    @ExtendField(value = "isFrontBalancing")
    private String isFrontBalancing;

    @ExtendField(value = "serviceType")
    private String serviceType;

    @ExtendField(value = "krb5Path")
    private String krb5Path;

    @ExtendField(value = "userKeytabPath")
    private String userKeytabPath;

    @UserName
    private String username;

    @Password
    private String password;

    @ExtendField(value = "parentPath")
    private String parentPath;

    @ExtendField(value = "certificateBase64Str")
    private String certificateBase64Str;

    /**
     * 华为云滚动索引是否配置 ism_template
     */
    @ExtendField(value = "openIsmTemplate")
    private String openIsmTemplate;

    @ExtendField(value = "openParallel")
    private String openParallel;

    public EsDbTemplate() {
    }

    public EsDbTemplate(String serviceUrl, String queryServiceUrl, String authType, String certificateId, String isFrontBalancing, String serviceType, String krb5Path, String userKeytabPath) {
        this.serviceUrl = serviceUrl;
        this.queryServiceUrl = queryServiceUrl;
        this.authType = authType;
        this.certificateId = certificateId;
        this.isFrontBalancing = isFrontBalancing;
        this.serviceType = serviceType;
        this.krb5Path = krb5Path;
        this.userKeytabPath = userKeytabPath;
    }

    public EsDbTemplate(String serviceUrl, String queryServiceUrl, String authType, String certificateId, String isFrontBalancing, String serviceType, String krb5Path, String userKeytabPath, String username, String password) {
        this.serviceUrl = serviceUrl;
        this.queryServiceUrl = queryServiceUrl;
        this.authType = authType;
        this.certificateId = certificateId;
        this.isFrontBalancing = isFrontBalancing;
        this.serviceType = serviceType;
        this.krb5Path = krb5Path;
        this.userKeytabPath = userKeytabPath;
        this.username = username;
        this.password = password;
    }
}
