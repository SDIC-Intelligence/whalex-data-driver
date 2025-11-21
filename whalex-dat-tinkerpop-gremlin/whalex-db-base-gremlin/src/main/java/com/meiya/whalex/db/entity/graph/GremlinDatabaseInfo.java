package com.meiya.whalex.db.entity.graph;

import cn.hutool.core.collection.CollectionUtil;
import com.meiya.whalex.db.entity.AbstractDatabaseInfo;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

/**
 * @author 黄河森
 * @date 2023/4/18
 * @package com.meiya.whalex.db.entity.graph
 * @project whalex-data-driver
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class GremlinDatabaseInfo extends AbstractDatabaseInfo {

    public static final String KERBEROS = "cer";

    public static final String NORMAL = "normal";

    /**
     * GraphBase图库地址
     */
    private List<String> serverUrl;

    /**
     * 端口
     */
    private Integer port;

    /**
     * 库名 alias
     */
    private String alias;

    /**
     * 用户名
     */
    private String username;

    /**
     * 密码
     */
    private String password;

    /**
     * 认证类型
     */
    private String authType;

    /**
     * 认证证书
     */
    private String certificateId;

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
     * 序列化器
     */
    private String serializer;

    /**
     * 序列化映射器
     */
    private String mapperBuilder;

    /**
     * io注册器
     */
    private String ioRegistries;

    /**
     * 协议
     */
    private String protocol;

    /**
     * 是否开启SSL
     */
    private Boolean enableSSL;

    /**
     * 是否跳过SSL校验
     */
    private Boolean sslSkipCertValidation;

    /**
     * 最大报文长度
     */
    private Integer maxContentLength = 65536000;

    /**
     * 结果批量获取代销
     */
    private Integer resultIterationBatchSize = 960;

    /**
     * 证书保存的目录
     */
    private String parentDir;

    /**
     * base64 证书串
     */
    private String certificateBase64Str;

    @Override
    public String getServerAddr() {
        if (CollectionUtils.isNotEmpty(serverUrl)) {
            return StringUtils.replaceEach(CollectionUtil.join(serverUrl.iterator(), ","), new String[]{"[","]"}, new String[]{"",""});
        } else {
            return null;
        }
    }

    @Override
    public String getDbName() {
        return alias;
    }

    @Builder
    public GremlinDatabaseInfo(List<String> serverUrl, Integer port, String alias, String username, String password, String authType, String certificateId, String krb5Path, String userKeytabPath, String parentDir, String kerberosPrincipal, String serializer, String mapperBuilder, String ioRegistries, String protocol, Boolean enableSSL, Boolean sslSkipCertValidation, Integer maxContentLength, Integer resultIterationBatchSize, String certificateBase64Str) {
        this.serverUrl = serverUrl;
        this.port = port;
        this.alias = alias;
        this.username = username;
        this.password = password;
        this.authType = authType;
        this.certificateId = certificateId;
        this.krb5Path = krb5Path;
        this.userKeytabPath = userKeytabPath;
        this.parentDir = parentDir;
        this.kerberosPrincipal = kerberosPrincipal;
        if (StringUtils.isNotBlank(serializer)) {
            this.serializer = serializer;
        }
        if (StringUtils.isNotBlank(mapperBuilder)) {
            this.mapperBuilder = mapperBuilder;
        }
        this.ioRegistries = ioRegistries;
        this.protocol = protocol;
        this.enableSSL = enableSSL;
        this.sslSkipCertValidation = sslSkipCertValidation;
        if (maxContentLength != null) {
            this.maxContentLength = maxContentLength;
        }
        if (resultIterationBatchSize != null) {
            this.resultIterationBatchSize = resultIterationBatchSize;
        }
        this.certificateBase64Str = certificateBase64Str;
    }

    public GremlinDatabaseInfo() {
    }
}
