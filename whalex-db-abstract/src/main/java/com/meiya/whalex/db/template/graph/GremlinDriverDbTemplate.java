package com.meiya.whalex.db.template.graph;

import com.meiya.whalex.annotation.DbType;
import com.meiya.whalex.interior.db.constant.DbResourceEnum;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author 黄河森
 * @date 2023/4/20
 * @package com.meiya.whalex.db.template.graph
 * @project whalex-data-driver
 */
@Data
@NoArgsConstructor
@DbType(DbResourceEnum.gremlin)
public class GremlinDriverDbTemplate extends BaseGremlinDbTemplate {

    @Builder
    public GremlinDriverDbTemplate(String serviceUrl, String port, String username, String password, String alias, String krb5Path, String userKeytabPath, String authType, String parentDir, String kerberosPrincipal, String serializer, String mapperBuilder, String ioRegistries, String protocol, String enableSSL, String sslSkipCertValidation, String maxContentLength, String resultIterationBatchSize, String certificateBase64Str) {
        super(serviceUrl, port, username, password, alias, krb5Path, userKeytabPath, authType, parentDir, kerberosPrincipal, serializer, mapperBuilder, ioRegistries, protocol, enableSSL, sslSkipCertValidation, maxContentLength, resultIterationBatchSize, certificateBase64Str);
    }
}
