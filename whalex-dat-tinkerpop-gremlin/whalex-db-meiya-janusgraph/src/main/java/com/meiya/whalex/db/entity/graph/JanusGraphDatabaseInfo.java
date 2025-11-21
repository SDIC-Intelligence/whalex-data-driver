package com.meiya.whalex.db.entity.graph;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.List;

/**
 * @author 黄河森
 * @date 2023/5/8
 * @package com.meiya.whalex.db.entity.graph
 * @project whalex-data-driver
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class JanusGraphDatabaseInfo extends GremlinDatabaseInfo {

    public static final String REMOTE_MODE = "remote";

    public static final String LOCAL_MODE = "local";

    /**
     * 连接模式
     */
    private String mode = REMOTE_MODE;

    private String storageBackend = "hbase";

    private String indexBackend;

    private String indexServerUrl;

    public JanusGraphDatabaseInfo() {
    }

    public JanusGraphDatabaseInfo(List<String> serverUrl, Integer port, String alias, String username, String password, String authType, String certificateId, String krb5Path, String userKeytabPath, String parentDir, String kerberosPrincipal, String serializer, String mapperBuilder, String ioRegistries, String protocol, Boolean enableSSL, Boolean sslSkipCertValidation, Integer maxContentLength, Integer resultIterationBatchSize, String storageBackend, String indexBackend, String indexServerUrl, String certificateBase64Str) {
        super(serverUrl, port, alias, username, password, authType, certificateId, krb5Path, userKeytabPath, parentDir, kerberosPrincipal, serializer, mapperBuilder, ioRegistries, protocol, enableSSL, sslSkipCertValidation, maxContentLength, resultIterationBatchSize, certificateBase64Str);
        this.storageBackend = storageBackend;
        this.indexBackend = indexBackend;
        this.indexServerUrl = indexServerUrl;
    }

    public JanusGraphDatabaseInfo(GremlinDatabaseInfo gremlinDatabaseInfo, String storageBackend, String indexBackend, String indexServerUrl, String mode) {
        super(gremlinDatabaseInfo.getServerUrl()
                , gremlinDatabaseInfo.getPort()
                , gremlinDatabaseInfo.getAlias()
                , gremlinDatabaseInfo.getUsername()
                , gremlinDatabaseInfo.getPassword()
                , gremlinDatabaseInfo.getAuthType()
                , gremlinDatabaseInfo.getCertificateId()
                , gremlinDatabaseInfo.getKrb5Path()
                , gremlinDatabaseInfo.getUserKeytabPath()
                , gremlinDatabaseInfo.getParentDir()
                , gremlinDatabaseInfo.getKerberosPrincipal()
                , gremlinDatabaseInfo.getSerializer()
                , gremlinDatabaseInfo.getMapperBuilder()
                , gremlinDatabaseInfo.getIoRegistries()
                , gremlinDatabaseInfo.getProtocol()
                , gremlinDatabaseInfo.getEnableSSL()
                , gremlinDatabaseInfo.getSslSkipCertValidation()
                , gremlinDatabaseInfo.getMaxContentLength()
                , gremlinDatabaseInfo.getResultIterationBatchSize()
                , gremlinDatabaseInfo.getCertificateBase64Str());
        this.storageBackend = storageBackend;
        this.indexBackend = indexBackend;
        this.indexServerUrl = indexServerUrl;
        this.mode = mode;
    }
}
