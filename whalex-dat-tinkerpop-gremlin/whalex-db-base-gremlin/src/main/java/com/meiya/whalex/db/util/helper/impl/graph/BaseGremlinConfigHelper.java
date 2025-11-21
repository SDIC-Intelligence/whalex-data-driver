package com.meiya.whalex.db.util.helper.impl.graph;

import cn.hutool.core.codec.Base64;
import com.meiya.whalex.annotation.DbHelper;
import com.meiya.whalex.business.entity.CertificateConf;
import com.meiya.whalex.business.entity.DatabaseConf;
import com.meiya.whalex.business.entity.TableConf;
import com.meiya.whalex.business.service.DatabaseConfService;
import com.meiya.whalex.db.entity.AbstractCursorCache;
import com.meiya.whalex.db.entity.graph.GremlinClient;
import com.meiya.whalex.db.entity.graph.GremlinDatabaseInfo;
import com.meiya.whalex.db.entity.graph.GremlinTableInfo;
import com.meiya.whalex.db.kerberos.KerberosJaasConfigurationUtil;
import com.meiya.whalex.db.kerberos.KerberosLoginCallBack;
import com.meiya.whalex.db.kerberos.KerberosUniformAuth;
import com.meiya.whalex.db.kerberos.KerberosUniformAuthFactory;
import com.meiya.whalex.db.kerberos.exception.GetKerberosException;
import com.meiya.whalex.db.util.helper.AbstractDbModuleConfigHelper;
import com.meiya.whalex.exception.BusinessException;
import com.meiya.whalex.exception.ExceptionCode;
import com.meiya.whalex.util.JsonUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.apache.commons.collections.CollectionUtils.isNotEmpty;
import static org.apache.commons.lang3.StringUtils.*;

/**
 * @author chenjp
 * @date 2021/2/23
 */
@Slf4j
public class BaseGremlinConfigHelper<
        S extends GremlinClient, D extends GremlinDatabaseInfo, T extends GremlinTableInfo, C extends AbstractCursorCache
        > extends AbstractDbModuleConfigHelper<S, D, T, C> {

    @Override
    public boolean checkDataSourceStatus(S cluster) throws Exception {
        return cluster.connect();
    }

    @Override
    public D initDbModuleConfig(DatabaseConf conf) {
        try {
            String connSetting = conf.getConnSetting();
            if (isBlank(connSetting)) {
                log.warn("获取GraphBase组件数据库部署定义为空，bigResourceId：{}", conf.getBigdataResourceId());
                throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "数据库配置必填参数缺失，请校验参数配置是否正确!");
            }
            Map<String, Object> dbMap = JsonUtil.jsonStrToObject(connSetting, Map.class);
            List<String> serverUrls = new ArrayList<>();
            String serverUrl = (String) dbMap.get("serviceUrl");
            Object portObj = dbMap.get("port");
            String port = portObj != null ? String.valueOf(portObj) : null;
            if (isBlank(serverUrl) || (isBlank(port) || !isNumeric(port))) {
                log.warn("获取GraphBase组件数据库部署定义地址或端口为空，bigResourceId：{}", conf.getBigdataResourceId());
                throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "数据库配置必填参数缺失，请校验参数配置是否正确!");
            }
            List<String> serverSet = convertServer(serverUrl);
            if (isNotEmpty(serverSet)) {
                serverUrls.addAll(serverSet);
            }
            return getDatabaseInfo(dbMap, serverUrls);
        } catch (Exception e) {
            log.error("GraphBase 数据库配置加载失败!", e);
            throw new BusinessException(e, ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "数据库配置解析异常，请校验参数配置是否正确!");
        }
    }

    @Override
    public T initTableConfig(TableConf conf) {
        GremlinTableInfo graphBaseTableInfo = new GremlinTableInfo();
        graphBaseTableInfo.setTableName(conf.getTableName());
        return (T) graphBaseTableInfo;
    }

    @Override
    public S initDbConnect(D databaseConf, T tableConf) {
        String authType = databaseConf.getAuthType();
        String certificateId = databaseConf.getCertificateId();
        String certificateBase64Str = databaseConf.getCertificateBase64Str();
        KerberosUniformAuth kerberosUniformLogin = null;
        //  获取证书
        if (equalsIgnoreCase(authType, GremlinDatabaseInfo.KERBEROS) || isNotBlank(certificateId) || isNotBlank(certificateBase64Str)) {
            try {
                DbHelper annotation = this.getClass().getAnnotation(DbHelper.class);
                DatabaseConfService databaseConfService = getDatabaseConfService();
                // certificateBase64Str存在
                if(StringUtils.isNotBlank(databaseConf.getCertificateBase64Str())) {
                    byte[] content = Base64.decode(databaseConf.getCertificateBase64Str());
                    kerberosUniformLogin = KerberosUniformAuthFactory.getKerberosUniformLoginByZip(annotation.dbType(), annotation.version(), annotation.cloudVendors()
                            , content, ".zip", databaseConf.getUsername(), databaseConf.getKerberosPrincipal(), databaseConf.getParentDir(), null);
                } else if (databaseConfService == null) {
                    //  本地获取
                    kerberosUniformLogin = KerberosUniformAuthFactory.getKerberosUniformLoginByLocal(annotation.dbType(), annotation.version(), annotation.cloudVendors()
                            , databaseConf.getUsername(), databaseConf.getKerberosPrincipal(), databaseConf.getKrb5Path(), databaseConf.getUserKeytabPath()
                            , databaseConf.getParentDir()
                            , null);
                } else {
                    //  从数据库获取
                    CertificateConf certificateConf = databaseConfService.queryCertificateConfById(certificateId);
                    if (certificateConf == null) {
                        throw new BusinessException(ExceptionCode.NOT_FOUND_CERTIFICATE, certificateId);
                    }
                    kerberosUniformLogin = KerberosUniformAuthFactory.getKerberosUniformLoginByZip(annotation.dbType(), annotation.version(), annotation.cloudVendors()
                            , certificateConf.getContent(), certificateConf.getFileName(), certificateConf.getUserName(), databaseConf.getKerberosPrincipal(), databaseConf.getParentDir()
                            , null);
                }
            } catch (GetKerberosException e) {
                throw new BusinessException(e, ExceptionCode.GET_CERTIFICATE_EXCEPTION, certificateId);
            }
            try {
                log.info("Kerberos Gremlin 进行证书登录！");
                kerberosUniformLogin.login(KerberosJaasConfigurationUtil.Module.GREMLIN, new KerberosLoginCallBack() {
                    @Override
                    public void loginExtend(KerberosUniformAuth kerberosUniformLogin) throws Exception {
                        System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
                    }
                });
            } catch (Exception e) {
                throw new BusinessException(e, ExceptionCode.CERTIFICATE_LOGIN_EXCEPTION, certificateId);
            }
        }

        return getS(databaseConf, kerberosUniformLogin);
    }

    protected S getS(D databaseConf, KerberosUniformAuth kerberosUniformLogin) {
        GremlinClient gremlinClient = GremlinClient.baseBuilder().hosts(databaseConf.getServerAddr())
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

        return (S) gremlinClient;
    }

    @Override
    public void destroyDbConnect(String cacheKey, S cluster) throws Exception {
        cluster.close();
    }

    private D getDatabaseInfo(Map<String, Object> dbMap, List<String> serverList) {
        // 数据库名称
        String database = (String) dbMap.get("alias");
        // 认证类型
        String authType = (String) dbMap.get("authType");
        // 证书信息
        String certificateId = (String) dbMap.get("certficateId");
        if (StringUtils.isBlank(certificateId)) {
            certificateId = (String) dbMap.get("certificateId");
        }

        // 证书Base64
        //证书，base64字符串
        String certificateBase64Str = (String) dbMap.get("certificateBase64Str");

        // krb5 地址
        String krb5Path = (String) dbMap.get("krb5Path");
        // userKeytab 地址
        String userKeytabPath = (String) dbMap.get("userKeytabPath");
        // kerberos 认证主题名称
        String kerberosPrincipal = (String) dbMap.get("kerberosPrincipal");
        GremlinDatabaseInfo gremlinDatabaseInfo = new GremlinDatabaseInfo();
        gremlinDatabaseInfo.setServerUrl(serverList);
        gremlinDatabaseInfo.setPort(Integer.valueOf(String.valueOf(dbMap.get("port"))));
        if (StringUtils.isNotBlank(database)) {
            gremlinDatabaseInfo.setAlias(database);
        }
        gremlinDatabaseInfo.setUsername((String) dbMap.get("username"));
        gremlinDatabaseInfo.setPassword((String) dbMap.get("password"));
        gremlinDatabaseInfo.setKrb5Path(krb5Path);
        gremlinDatabaseInfo.setUserKeytabPath(userKeytabPath);
        gremlinDatabaseInfo.setKerberosPrincipal(kerberosPrincipal);
        if (isNotBlank(certificateId)) {
            gremlinDatabaseInfo.setCertificateId(certificateId);
        }
        gremlinDatabaseInfo.setCertificateBase64Str(certificateBase64Str);
        if (isNotBlank(authType)) {
            gremlinDatabaseInfo.setAuthType(authType);
        } else {
            if (isNotBlank(certificateId) || (isNotBlank(krb5Path) && isNotBlank(userKeytabPath)) || isNotBlank(certificateBase64Str)) {
                gremlinDatabaseInfo.setAuthType(GremlinDatabaseInfo.KERBEROS);
            } else {
                gremlinDatabaseInfo.setAuthType(GremlinDatabaseInfo.NORMAL);
            }
        }
        String serializer = (String) dbMap.get("serializer");
        String mapperBuilder = (String) dbMap.get("mapperBuilder");
        String ioRegistries = (String) dbMap.get("ioRegistries");
        String protocol = (String) dbMap.get("protocol");
        Object enableSSL = dbMap.get("enableSSL");
        Object sslSkipCertValidation = dbMap.get("sslSkipCertValidation");
        Object maxContentLength = dbMap.get("maxContentLength");
        Object resultIterationBatchSize = dbMap.get("resultIterationBatchSize");
        if (isNotBlank(serializer)) {
            gremlinDatabaseInfo.setSerializer(serializer);
        }
        if (isNotBlank(mapperBuilder)) {
            gremlinDatabaseInfo.setMapperBuilder(mapperBuilder);
        }
        if (isNotBlank(ioRegistries)) {
            gremlinDatabaseInfo.setIoRegistries(ioRegistries);
        }
        if (isNotBlank(protocol)) {
            gremlinDatabaseInfo.setProtocol(protocol);
        }
        if (enableSSL != null) {
            gremlinDatabaseInfo.setEnableSSL(Boolean.parseBoolean(String.valueOf(enableSSL)));
        }
        if (sslSkipCertValidation != null) {
            gremlinDatabaseInfo.setSslSkipCertValidation(Boolean.parseBoolean(String.valueOf(sslSkipCertValidation)));
        }
        if (maxContentLength != null) {
            gremlinDatabaseInfo.setMaxContentLength(Integer.parseInt(String.valueOf(maxContentLength)));
        }
        if (resultIterationBatchSize != null) {
            gremlinDatabaseInfo.setResultIterationBatchSize(Integer.valueOf(String.valueOf(resultIterationBatchSize)));
        }
        return (D) gremlinDatabaseInfo;
    }

    /**
     * 转换serviceUrl
     */
    private List<String> convertServer(String serviceUrl) {
        List<String> serverSet = new ArrayList<>();
        String isRange = substringBetween(serviceUrl, "(", ")");
        if (isBlank(isRange)) {
            isRange = substringBetween(serviceUrl, "[", "]");
        }
        if (isBlank(isRange)) {
            isRange = serviceUrl;
        }
        if (isNotBlank(isRange) || contains(serviceUrl, ",")) {
            String[] servers = split(isRange, ",");
            serverSet.addAll(Arrays.asList(servers));
        } else {
            serverSet.add(serviceUrl);
        }
        return serverSet;
    }

}
