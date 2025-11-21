package com.meiya.whalex.db.util.helper.impl.graph;

import com.meiya.whalex.annotation.DbHelper;
import com.meiya.whalex.db.entity.AbstractCursorCache;
import com.meiya.whalex.db.entity.graph.GremlinDatabaseInfo;
import com.meiya.whalex.db.entity.graph.GremlinDriverClient;
import com.meiya.whalex.db.entity.graph.GremlinTableInfo;
import com.meiya.whalex.db.kerberos.KerberosJaasConfigurationUtil;
import com.meiya.whalex.db.kerberos.KerberosUniformAuth;
import com.meiya.whalex.interior.db.constant.CloudVendorsEnum;
import com.meiya.whalex.interior.db.constant.DbResourceEnum;
import com.meiya.whalex.interior.db.constant.DbVersionEnum;
import org.apache.commons.lang3.StringUtils;

/**
 * @author 黄河森
 * @date 2025/7/2
 * @package com.meiya.whalex.db.util.helper.impl.graph
 * @project whalex-data-driver-2.4.1
 * @description GremlinDriverConfigHelper
 */
@DbHelper(dbType = DbResourceEnum.gremlin, version = DbVersionEnum.GREMLIN_3_7_3, cloudVendors = CloudVendorsEnum.OPEN)
public class GremlinDriverConfigHelper extends BaseGremlinConfigHelper<GremlinDriverClient, GremlinDatabaseInfo, GremlinTableInfo, AbstractCursorCache> {

    @Override
    protected GremlinDriverClient getS(GremlinDatabaseInfo databaseConf, KerberosUniformAuth kerberosUniformLogin) {
        return GremlinDriverClient.builder().hosts(databaseConf.getServerAddr())
                .port(Integer.valueOf(String.valueOf(databaseConf.getPort())))
                .alias(databaseConf.getAlias())
                .maxConnectionPoolSize(this.threadConfig.getMaximumPoolSize())
                .minConnectionPoolSize(this.threadConfig.getCorePoolSize())
                .enableSSL(databaseConf.getEnableSSL())
                .ioRegistries(databaseConf.getIoRegistries())
                .mapperBuilderClass(databaseConf.getMapperBuilder())
                .maxContentLength(databaseConf.getMaxContentLength())
                .username(databaseConf.getUsername())
                .password(databaseConf.getPassword())
                .protocol(databaseConf.getProtocol())
                .resultIterationBatchSize(databaseConf.getResultIterationBatchSize())
                .serializerClass(databaseConf.getSerializer())
                .sslSkipCertValidation(databaseConf.getSslSkipCertValidation())
                .jaasEntry(
                        StringUtils.equalsIgnoreCase(GremlinDatabaseInfo.KERBEROS, databaseConf.getAuthType()) ? KerberosJaasConfigurationUtil.Module.GREMLIN.getName() : null
                )
                .kerberosUniformLogin(kerberosUniformLogin)
                .build();
    }
}
