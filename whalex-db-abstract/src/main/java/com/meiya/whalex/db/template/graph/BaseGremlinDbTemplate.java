package com.meiya.whalex.db.template.graph;

import com.meiya.whalex.annotation.*;
import com.meiya.whalex.db.template.BaseDbConfTemplate;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author 黄河森
 * @date 2023/4/20
 * @package com.meiya.whalex.db.template.graph
 * @project whalex-data-driver
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class BaseGremlinDbTemplate extends BaseDbConfTemplate {

    @Host
    private String serviceUrl;

    @Port
    private String port;

    @UserName
    private String username;

    @Password
    private String password;

    @DatabaseName
    private String alias;

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
     * 认证类型(如果是认证文件认证,填写cer)
     */
    @ExtendField(value = "authType")
    private String authType;

    /**
     * 证书保存的目录
     */
    @ExtendField(value = "parentDir")
    private String parentDir;

    @ExtendField(value = "kerberosPrincipal")
    private String kerberosPrincipal;

    @ExtendField(value = "serializer")
    private String serializer;

    @ExtendField(value = "mapperBuilder")
    private String mapperBuilder;

    @ExtendField(value = "ioRegistries")
    private String ioRegistries;

    @ExtendField(value = "protocol")
    private String protocol;

    @ExtendField(value = "enableSSL")
    private String enableSSL;

    @ExtendField(value = "sslSkipCertValidation")
    private String sslSkipCertValidation;

    @ExtendField(value = "maxContentLength")
    private String maxContentLength;

    @ExtendField(value = "resultIterationBatchSize")
    private String resultIterationBatchSize;

    @ExtendField(value = "certificateBase64Str")
    private String certificateBase64Str;
}
