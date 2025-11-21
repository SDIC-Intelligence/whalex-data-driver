package com.meiya.whalex.db.template.graph;

import com.meiya.whalex.annotation.DbType;
import com.meiya.whalex.annotation.ExtendField;
import com.meiya.whalex.interior.db.constant.DbResourceEnum;
import lombok.Builder;
import lombok.Data;

/**
 * @author 黄河森
 * @date 2023/4/20
 * @package com.meiya.whalex.db.template.graph
 * @project whalex-data-driver
 */
@Data
@DbType(DbResourceEnum.janusgraph)
public class JanusGraphDbTemplate extends BaseGremlinDbTemplate {

    public static final String REMOTE_MODE = "remote";

    public static final String LOCAL_MODE = "local";

    @ExtendField(value = "mode")
    private String mode;

    @ExtendField(value = "storageBackend")
    private String storageBackend = "hbase";

    @ExtendField(value = "indexBackend")
    private String indexBackend;

    @ExtendField(value = "indexServerUrl")
    private String indexServerUrl;

    @Builder(builderMethodName = "remoteBuilder", buildMethodName = "remoteBuild")
    public JanusGraphDbTemplate(String serviceUrl, String port, String username, String password, String alias, String krb5Path, String userKeytabPath, String authType, String parentDir, String kerberosPrincipal, String serializer, String mapperBuilder, String ioRegistries, String protocol, String enableSSL, String sslSkipCertValidation, String maxContentLength, String resultIterationBatchSize, String certificateBase64Str) {
        super(serviceUrl, port, username, password, alias, krb5Path, userKeytabPath, authType, parentDir, kerberosPrincipal, serializer, mapperBuilder, ioRegistries, protocol, enableSSL, sslSkipCertValidation, maxContentLength, resultIterationBatchSize, certificateBase64Str);
        this.mode = REMOTE_MODE;
    }

    @Builder(builderMethodName = "localBuilder", buildMethodName = "localBuild")
    public JanusGraphDbTemplate(String alias, String serviceUrl, String port, String storageBackend, String indexBackend, String indexServerUrl) {
        super();
        this.setServiceUrl(serviceUrl);
        this.setPort(port);
        this.setAlias(alias);
        this.storageBackend = storageBackend;
        this.indexBackend = indexBackend;
        this.indexServerUrl = indexServerUrl;
        this.mode = LOCAL_MODE;
    }
}
