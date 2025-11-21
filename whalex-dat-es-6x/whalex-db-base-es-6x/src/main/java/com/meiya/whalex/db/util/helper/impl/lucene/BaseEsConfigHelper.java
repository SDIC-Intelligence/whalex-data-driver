package com.meiya.whalex.db.util.helper.impl.lucene;
import cn.hutool.core.codec.Base64;
import com.meiya.whalex.annotation.DbHelper;
import com.meiya.whalex.business.entity.CertificateConf;
import com.meiya.whalex.business.entity.DatabaseConf;
import com.meiya.whalex.business.entity.TableConf;
import com.meiya.whalex.business.service.DatabaseConfService;
import com.meiya.whalex.db.entity.lucene.*;
import com.meiya.whalex.db.kerberos.KerberosJaasConfigurationUtil;
import com.meiya.whalex.db.kerberos.KerberosLoginCallBack;
import com.meiya.whalex.db.kerberos.KerberosUniformAuth;
import com.meiya.whalex.db.kerberos.KerberosUniformAuthFactory;
import com.meiya.whalex.db.kerberos.exception.GetKerberosException;
import com.meiya.whalex.db.util.common.DatatypePeriodUtils;
import com.meiya.whalex.db.util.helper.AbstractDbModuleConfigHelper;
import com.meiya.whalex.exception.BusinessException;
import com.meiya.whalex.exception.ExceptionCode;
import com.meiya.whalex.interior.db.constant.DbResourceEnum;
import com.meiya.whalex.interior.db.constant.PeriodCycleEnum;
import com.meiya.whalex.util.JsonUtil;
import com.meiya.whalex.util.concurrent.ThreadNamedFactory;
import com.meiya.whalex.util.encrypt.AESUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.message.BasicHeader;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

/**
 * Es 组件配置信息服务工具
 *
 * @author 黄河森
 * @date 2019/12/19
 * @project whale-cloud-platformX
 */
@Slf4j
public class BaseEsConfigHelper<S extends EsClient
        , D extends EsDatabaseInfo
        , T extends EsTableInfo
        , C extends EsCursorCache> extends AbstractDbModuleConfigHelper<S, D, T, C> {

    public int connectTimeout = 5000;
    public int socketTimeout = 60000;
    public int maxRetryTimeoutMillis = 60000;
    public int connectionRequestTimeout = 60000;

    private static String esRolloverTemplate = "{\n" +
            "    \"conditions\": {\n" +
            "        \"max_age\": \"7d\",\n" +
            "        \"max_docs\": \"1000\",\n" +
            "        \"max_size\": \"5gb\"\n" +
            "    }\n" +
            "}";

    @Override
    public void init(boolean loadDbConf) {
        socketTimeout = threadConfig.getTimeOut() * 1000;
        maxRetryTimeoutMillis = threadConfig.getTimeOut() * 1000;
        connectionRequestTimeout = threadConfig.getTimeOut() * 1000;
        super.init(loadDbConf);
    }

    @Override
    public boolean checkDataSourceStatus(S esClient) throws Exception {
        RestClient connect = esClient.getRestClient();
        testConnectRestClient(connect);
        RestClient queryClient = esClient.getQueryClient();
        if (queryClient != null) {
            testConnectRestClient(queryClient);
        }
        return true;
    }

    /**
     * 连接测试
     *
     * @param connect
     * @throws Exception
     */
    private void testConnectRestClient(RestClient connect) throws Exception {
        String endpoint = "_cluster/health";
        Response response = connect.performRequest(new Request("GET", endpoint));
        boolean check = HttpStatus.SC_OK == response.getStatusLine().getStatusCode() || HttpStatus.SC_CREATED == response.getStatusLine().getStatusCode();
        if (!check) {
            throw new BusinessException(ExceptionCode.LINK_DATABASE_EXCEPTION
                    , "es statusCode: [" + response.getStatusLine().getStatusCode() + "] msg: [" + EntityUtils.toString(response.getEntity()) + "]");
        }
    }

    @Override
    public D initDbModuleConfig(DatabaseConf conf) {
        try {
            EsDatabaseInfo esDatabaseInfo = new EsDatabaseInfo();
            String connSetting = conf.getConnSetting();
            Map<String, String> dbMap = JsonUtil.jsonStrToObject(connSetting, Map.class);
            String serviceUrl = dbMap.get("serviceUrl");
            if (StringUtils.isBlank(serviceUrl)) {
                log.error("ES 数据库配置加载失败, 服务地址为空! bigResourceId: [{}]", conf.getBigdataResourceId());
                throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "数据库配置必填参数缺失，请校验参数配置是否正确!");
            }
            String authType = dbMap.get("authType");
            String certificateId = dbMap.get("certficateId");
            if (StringUtils.isBlank(certificateId)) {
                certificateId = dbMap.get("certificateId");
            }
            String preLoadBalancing = dbMap.get("isFrontNginx");
            if (StringUtils.isBlank(preLoadBalancing)) {
                preLoadBalancing = dbMap.get("isFrontBalancing");
                if (StringUtils.isBlank(preLoadBalancing)) {
                    preLoadBalancing = "false";
                }
            }
            String queryServiceUrl = dbMap.get("queryServiceUrl");
            String serviceType = dbMap.get("serviceType");
            if (StringUtils.isBlank(serviceType)) {
                serviceType = "http/https";
            }
            // krb5 地址
            String krb5Path = dbMap.get("krb5Path");
            // userKeytab 地址
            String userKeytabPath = dbMap.get("userKeytabPath");
            // kerberos 认证主题名称
            String kerberosPrincipal = dbMap.get("kerberosPrincipal");
            // 获取用户名
            String username = dbMap.get("userName") == null ? dbMap.get("username") : dbMap.get("userName");
            // xpack 认证密码
            String password = dbMap.get("password");
            // AES 解密
            if (StringUtils.isNotBlank(password)) {
                try {
                    password = AESUtil.decrypt(password);
                } catch (Exception e) {
//                    log.error("elasticsearch init db config password [{}] decrypt fail!!!", password, e);
                }
            }

            //证书，base64字符串
            String certificateBase64Str = dbMap.get("certificateBase64Str");

            esDatabaseInfo.setCertificateBase64Str(certificateBase64Str);
            esDatabaseInfo.setKrb5Path(krb5Path);
            esDatabaseInfo.setUserKeytabPath(userKeytabPath);
            esDatabaseInfo.setQueryServerAddress(queryServiceUrl);
            esDatabaseInfo.setServerAddress(serviceUrl);
            esDatabaseInfo.setRequestType(serviceType);
            esDatabaseInfo.setPreLoadBalancing(Boolean.valueOf(preLoadBalancing));
            esDatabaseInfo.setCertificateId(certificateId);
            esDatabaseInfo.setKerberosPrincipal(kerberosPrincipal);
            esDatabaseInfo.setAuthType(authType);
            esDatabaseInfo.setParentDir(dbMap.get("parentDir"));
            esDatabaseInfo.setPassword(password);
            esDatabaseInfo.setUserName(username);

            // 设置认证类型
            if (StringUtils.isBlank(esDatabaseInfo.getAuthType())) {
                if (StringUtils.isNotBlank(esDatabaseInfo.getCertificateId())
                        || (StringUtils.isNotBlank(esDatabaseInfo.getKrb5Path()) && StringUtils.isNotBlank(esDatabaseInfo.getUserKeytabPath()))
                        || StringUtils.isNotBlank(esDatabaseInfo.getCertificateBase64Str())) {
                    esDatabaseInfo.setAuthType(EsDatabaseInfo.KERBEROS_AUTH);
                } else if (StringUtils.isNotBlank(esDatabaseInfo.getUserName()) && StringUtils.isNotBlank(esDatabaseInfo.getPassword())) {
                    esDatabaseInfo.setAuthType(EsDatabaseInfo.X_PACK_AUTH);
                }
            }

            // 设置是否并发处理结果
            String openParallel = dbMap.get("openParallel");
            if (StringUtils.isNotBlank(openParallel)) {
                esDatabaseInfo.setOpenParallel(Boolean.parseBoolean(openParallel));
            }

            return (D) esDatabaseInfo;
        } catch (Exception e) {
            log.error("es 数据库配置信息加载失败! bigResourceId: [{}]", conf.getBigdataResourceId(), e);
            return null;
        }
    }

    @Override
    public T initTableConfig(TableConf conf) {
        try {
            EsTableInfo esTableInfo = new EsTableInfo();
            String tableJson = conf.getTableJson();
            String coreName = conf.getTableName();
            if (StringUtils.isNotBlank(tableJson)) {
                Map<String, Object> tableJsonMap = JsonUtil.jsonStrToMap(tableJson);
                // 是否创建模板
                Object createTemplate = tableJsonMap.get("createTemplate");
                if(createTemplate == null) {
                    esTableInfo.setCreateTemplate(false);
                }else {
                    esTableInfo.setCreateTemplate(Boolean.valueOf(createTemplate.toString()));
                }

                // 存储周期类型
                String periodType = (String) tableJsonMap.get("periodType");
                if(periodType == null) {
                    periodType = PeriodCycleEnum.ONLY_ONE.getPeriod();
                }
                // 存储周期值
                String storePeriodValueString = (String) tableJsonMap.get("storePeriodValue");
                // schema
                String schemaName = (String) tableJsonMap.get("schema");
                // 分片数量
                Object numregions = tableJsonMap.get("numregions");
                if (numregions == null) {
                    numregions = tableJsonMap.get("numRegions");
                }
                String schema = null;
                if (StringUtils.isNotBlank(schemaName)) {
                    if (getTableConfService() != null) {
                        schema = getTableConfService().querySchemaTemPlate(schemaName, DbResourceEnum.es.getVal());
                    } else {
                        schema = schemaName;
                    }
                    if (StringUtils.isBlank(schema)) {
                        log.warn("elasticsearch table [{}] init not found schemaName: [{}]!", conf.getId(), schemaName);
                    }
                }
                // 未来分片数量
                String futurePeriodValue = (String) tableJsonMap.get("backwardPeriod");
                // 过往分片周期值
                String prePeriodValue = (String) tableJsonMap.get("prePeriod");
                esTableInfo.setPeriodType(periodType);
                if (numregions != null) {
                    if (numregions instanceof Integer) {
                        esTableInfo.setBurstZoneNum((Integer) numregions);
                    } else if (numregions instanceof String && StringUtils.isNotBlank((String) numregions) && StringUtils.isNumeric((String) numregions)) {
                        esTableInfo.setBurstZoneNum(Integer.valueOf((String) numregions));
                    }
                }
                if (futurePeriodValue != null
                        && StringUtils.isNumeric(futurePeriodValue)) {
                    esTableInfo.setFuturePeriodValue(Integer.valueOf(futurePeriodValue));
                }
                if (prePeriodValue != null
                        && StringUtils.isNumeric(prePeriodValue)) {
                    esTableInfo.setPrePeriodValue(Integer.valueOf(prePeriodValue));
                }
                if (storePeriodValueString != null
                        && StringUtils.isNumeric(storePeriodValueString)) {
                    esTableInfo.setPeriodValue(Integer.valueOf(storePeriodValueString));
                }

                //副本数量
                Object replicas = tableJsonMap.get("replicas");
                if (replicas != null) {
                    if (replicas instanceof Integer) {
                        esTableInfo.setReplicas((Integer) replicas);
                    } else if (replicas instanceof String && StringUtils.isNotBlank((String) replicas) && StringUtils.isNumeric((String) replicas)) {
                        esTableInfo.setReplicas(Integer.valueOf((String) replicas));
                    }
                }

                //分片数量
                Object numberOfShards = tableJsonMap.get("numberOfShards");
                if (numberOfShards != null) {
                    if (numberOfShards instanceof Integer) {
                        esTableInfo.setNumberOfShards((Integer) numberOfShards);
                    } else if (numberOfShards instanceof String && StringUtils.isNotBlank((String) numberOfShards) && StringUtils.isNumeric((String) numberOfShards)) {
                        esTableInfo.setNumberOfShards(Integer.valueOf((String) numberOfShards));
                    }
                }

                //totalShardsPerNode
                Object totalShardsPerNode = tableJsonMap.get("totalShardsPerNode");
                if (totalShardsPerNode != null) {
                    if (totalShardsPerNode instanceof Integer) {
                        esTableInfo.setTotalShardsPerNode((Integer) totalShardsPerNode);
                    } else if (totalShardsPerNode instanceof String && StringUtils.isNotBlank((String) totalShardsPerNode) && StringUtils.isNumeric((String) totalShardsPerNode)) {
                        esTableInfo.setTotalShardsPerNode(Integer.valueOf((String) totalShardsPerNode));
                    }
                }


                // es滚动策略参数
                String maxAge = tableJsonMap.get("maxAge") == null ? null : String.valueOf(tableJsonMap.get("maxAge"));
                String maxDocs = tableJsonMap.get("maxDocs") == null ? null : String.valueOf(tableJsonMap.get("maxDocs"));
                String maxSize = tableJsonMap.get("maxSize") == null ? null : String.valueOf(tableJsonMap.get("maxSize"));
                if (!StringUtils.isAllBlank(maxAge,maxDocs,maxSize)){
                    EsTableInfo.EsRolloverPolicy esRolloverPolicy = new EsTableInfo.EsRolloverPolicy();
                    if (StringUtils.isNotBlank(maxAge)){
                        esRolloverPolicy.setMaxAge(maxAge);
                    }
                    if (StringUtils.isNotBlank(maxDocs) && StringUtils.isNumeric(maxDocs)){
                        esRolloverPolicy.setMaxDocs(Integer.valueOf(maxDocs));
                    }
                    if (StringUtils.isNotBlank(maxSize)){
                        esRolloverPolicy.setMaxSize(maxSize);
                    }
                    esTableInfo.setEsRolloverPolicy(esRolloverPolicy);
                }

                esTableInfo.setSchema(schema);
                // 设置是否忽略不存在的索引
                Boolean ignoreUnavailable = tableJsonMap.get("ignoreUnavailable") == null ? null : Boolean.valueOf(String.valueOf(tableJsonMap.get("ignoreUnavailable")));
                if (ignoreUnavailable != null) {
                    esTableInfo.setIgnoreUnavailable(ignoreUnavailable);
                }
                // 获取文档类型配置
                String docType = tableJsonMap.get("docType") == null ? null : (String) tableJsonMap.get("docType");
                if (StringUtils.isNotBlank(docType)) {
                    esTableInfo.setDocType(docType);
                }

                // 设置刷新时间
                Object refreshInterval = tableJsonMap.get("refreshInterval");
                if (refreshInterval != null) {
                    if (refreshInterval instanceof Integer) {
                        esTableInfo.setRefreshInterval((Integer) refreshInterval);
                    } else if (refreshInterval instanceof String && StringUtils.isNotBlank((String) refreshInterval) && StringUtils.isNumeric((String) refreshInterval)) {
                        esTableInfo.setRefreshInterval(Integer.valueOf((String) refreshInterval));
                    }
                }

                // 设置查询切分规则
                Object segmentationQuery = tableJsonMap.get("segmentationQuery");
                if (segmentationQuery != null) {
                    esTableInfo.setSegmentationQuery(Boolean.parseBoolean(String.valueOf(segmentationQuery)));
                    // 切分数量
                    Object segmentalQuantity = tableJsonMap.get("segmentalQuantity");
                    if (segmentalQuantity != null) {
                        esTableInfo.setSegmentalQuantity(Integer.valueOf(String.valueOf(segmentalQuantity)));
                    }
                }
            } else {
                esTableInfo.setCreateTemplate(false);
                esTableInfo.setPeriodType(PeriodCycleEnum.ONLY_ONE.getPeriod());
            }
            esTableInfo.setTableName(coreName);
            return (T) esTableInfo;
        } catch (Exception e) {
            log.error("ES 数据库表配置加载失败! dbId: [{}]", conf.getId(), e);
            throw new BusinessException(e, ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "数据表配置解析异常，请校验参数配置是否正确!");
        }
    }

    @Override
    public S initDbConnect(D databaseConf, T tableConf) {
        String authType = databaseConf.getAuthType();
        String certificateId = databaseConf.getCertificateId();
        KerberosUniformAuth kerberosUniformLogin;
        // 获取证书
        if (!StringUtils.endsWithIgnoreCase(authType, EsDatabaseInfo.CUSTOM_PROXY_AUTH) && (StringUtils.equalsIgnoreCase(authType, EsDatabaseInfo.KERBEROS_AUTH) || StringUtils.isNotBlank(certificateId))) {
            try {
                DbHelper annotation = this.getClass().getAnnotation(DbHelper.class);
                DatabaseConfService databaseConfService = getDatabaseConfService();

                //certificateBase64Str存在
                if(StringUtils.isNotBlank(databaseConf.getCertificateBase64Str())) {
                    byte[] content = Base64.decode(databaseConf.getCertificateBase64Str());
                    kerberosUniformLogin = KerberosUniformAuthFactory.getKerberosUniformLoginByZip(annotation.dbType(), annotation.version(), annotation.cloudVendors()
                            , content, ".zip", databaseConf.getUserName(), null, databaseConf.getParentDir(), null);

                } else if (databaseConfService == null) {
                    // 本地获取
                    kerberosUniformLogin =  KerberosUniformAuthFactory.getKerberosUniformLoginByLocal(annotation.dbType(), annotation.version(), annotation.cloudVendors()
                            , databaseConf.getUserName(), null, databaseConf.getKrb5Path(), databaseConf.getUserKeytabPath(), databaseConf.getParentDir(), null);
                } else {
                    // 从数据库获取
                    CertificateConf certificateConf = databaseConfService.queryCertificateConfById(certificateId);
                    if (certificateConf == null) {
                        throw new BusinessException(ExceptionCode.NOT_FOUND_CERTIFICATE, certificateId);
                    }
                    kerberosUniformLogin = KerberosUniformAuthFactory.getKerberosUniformLoginByZip(annotation.dbType(), annotation.version(), annotation.cloudVendors()
                            , certificateConf.getContent(), certificateConf.getFileName(), certificateConf.getUserName(), null, databaseConf.getParentDir(), null);
                }
            } catch (GetKerberosException e) {
                throw new BusinessException(e, ExceptionCode.GET_CERTIFICATE_EXCEPTION, certificateId);
            }
            // 证书登录
            try {
                log.info("Kerberos es 进行证书登录!");
                kerberosUniformLogin.login(KerberosJaasConfigurationUtil.Module.ES ,new KerberosLoginCallBack() {
                    @Override
                    public void loginExtend(KerberosUniformAuth kerberosUniformLogin) throws Exception {
                        System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
                    }
                });
            } catch (Exception e) {
                throw new BusinessException(e, ExceptionCode.CERTIFICATE_LOGIN_EXCEPTION, certificateId);
            }
        }

        EsClient esClient = new EsClient();
        String uri = databaseConf.getServerAddress();
        esClient.setRestClient(getRestClient(uri, authType, databaseConf.getUserName(), databaseConf.getPassword()));

        // 读写分离情况下，建立读连接
        if (StringUtils.isNotBlank(databaseConf.getQueryServerAddress())) {
            String queryUri = databaseConf.getQueryServerAddress();
            esClient.setQueryRestClient(getRestClient(queryUri, authType, databaseConf.getUserName(), databaseConf.getPassword()));
        }

        // 设置是否前置负载均衡
        esClient.setPreLoadBalancing(databaseConf.isPreLoadBalancing());

        return (S) esClient;
    }

    /**
     * 创建连接
     *
     * @param uri
     * @return
     */
    private RestClient getRestClient(String uri, String authType, String username, String password) {
        // 存在证书需要进行认证登录
        String httpType = "http";
        if (StringUtils.equalsIgnoreCase(authType, EsDatabaseInfo.KERBEROS_AUTH) || StringUtils.equalsIgnoreCase(authType, EsDatabaseInfo.CUSTOM_PROXY_AUTH)) {
            log.info("uri: [{}] 开启 authType: [{}] 模式 username: [{}] password: [{}]", uri, authType, username, password);
            httpType = "https";
        }
        RestClientBuilder builder = null;
        RestClient restClient = null;
        uri = uri.replaceAll("http[s]?://", "");
        List<HttpHost> httpHostList = new ArrayList<>();
        for (String ipPortStr : uri.split("[,;]")) {
            String[] ip_port = ipPortStr.split(":");
            if (ip_port.length != 2) {
                throw new RuntimeException("ip或端口配置格式问题, 地址：[" + uri + "], 异常部分: [" + ipPortStr + "]");
            }
            String ip = ip_port[0];
            int port = Integer.parseInt(ip_port[1]);
            httpHostList.add(new HttpHost(ip, port, httpType));
        }
        // 创建构建对象
        builder = RestClient.builder(httpHostList.toArray(new HttpHost[1]));
        // 配置Http连接
        String finalUri = uri;
        builder.setHttpClientConfigCallback(httpAsyncClientBuilder -> {
            // 最大连接数
            httpAsyncClientBuilder.setMaxConnTotal(this.threadConfig.getMaximumPoolSize());
            // 最大并发数
            httpAsyncClientBuilder.setMaxConnPerRoute(this.threadConfig.getMaximumPoolSize());
            // xPack 认证方式
            if (StringUtils.equalsIgnoreCase(authType, EsDatabaseInfo.X_PACK_AUTH)) {
                log.info("uri: [{}] 开启 authType: [{}] 模式 username: [{}] password: [{}]", finalUri, authType, username, password);
                CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
                httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
            }
            // 头部信息
            List<Header> headers = new ArrayList<>(4);
            headers.add(new BasicHeader("Accept", "application/json"));
            headers.add(new BasicHeader("Content-type", "application/json"));
            headers.add(new BasicHeader("Connection", "keep-alive"));
            headers.add(new BasicHeader("Keep-Alive", "720"));
            // 通过 CUSTOM_PROXY_AUTH 方式绕过 kerberos 认证
            if (StringUtils.equalsIgnoreCase(authType, EsDatabaseInfo.CUSTOM_PROXY_AUTH)) {
                try {
                    SSLContext sslContext = SSLContext.getInstance("TLS");
                    sslContext.init(null, new TrustManager[]{UnsafeX509ExtendedTrustManager.getInstance()}, null);
                    httpAsyncClientBuilder.setSSLContext(sslContext).setSSLHostnameVerifier((host, session) -> true);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                String esAuth = Base64.encode(username + ":" + password);
                headers.add(new BasicHeader("custom-proxy-username", esAuth));
            }
            // search-guard 认证方式
            if (StringUtils.endsWithIgnoreCase(authType, EsDatabaseInfo.SEARCH_GUARD_AUTH)) {
                log.info("uri: [{}] 开启 authType: [{}] 模式 username: [{}] password: [{}]", finalUri, authType, username, password);
                String esAuth = "Basic " + Base64.encode(username + ":" + password);
                headers.add(new BasicHeader("Authorization", esAuth));
            }
            httpAsyncClientBuilder.setDefaultHeaders(headers);
            httpAsyncClientBuilder.setKeepAliveStrategy(CustomConnectionKeepAliveStrategy.INSTANCE);
            IOReactorConfig ioReactorConfig = IOReactorConfig.custom().setConnectTimeout(connectTimeout).setIoThreadCount(Math.max(Runtime.getRuntime().availableProcessors() / 2, 12)).setSoTimeout(socketTimeout).setSoKeepAlive(true).build();
            httpAsyncClientBuilder.setDefaultIOReactorConfig(ioReactorConfig);
            httpAsyncClientBuilder.setThreadFactory(new ThreadNamedFactory("elasticsearch-rest-io-" + httpHostList.get(0).getHostName()));
            return httpAsyncClientBuilder;
        }).setRequestConfigCallback(new EsRequestConfigCallback(connectTimeout, socketTimeout, connectionRequestTimeout))
                .setMaxRetryTimeoutMillis(maxRetryTimeoutMillis);
        // 创建连接
        restClient = builder.build();
        return restClient;
    }

    /**
     * 客户端连接存活配置
     */
    public static class CustomConnectionKeepAliveStrategy extends DefaultConnectionKeepAliveStrategy {
        public static final CustomConnectionKeepAliveStrategy INSTANCE =  new CustomConnectionKeepAliveStrategy();
        private CustomConnectionKeepAliveStrategy() {
            super();
        }

        /**
         * 最大keep alive的时间（分钟）
         * 设置默认10分钟，可以观察机器状态未 TIME_WAIT 的TCP连接数，如果太多，可以调大
         */
        private final long MAX_KEEP_ALIVE_MINUTES = 1;

        @Override
        public long getKeepAliveDuration(HttpResponse response, HttpContext context) {
            long keepAliveDuration = super.getKeepAliveDuration(response, context);
            // < 0 为无限期keepalive
            // 将无限期替换成为一个默认时间
            if (keepAliveDuration < 0) {
                return TimeUnit.MINUTES.toMillis(MAX_KEEP_ALIVE_MINUTES);
            }
            return keepAliveDuration;
        }
    }

    @Override
    public void destroyDbConnect(String cacheKey, S connect) throws Exception {
        RestClient restClient = connect.getRestClient();
        RestClient queryClient = connect.getQueryRestClient();
        try {
            if (restClient != null) {
                restClient.close();
                restClient = null;
            }
            if (queryClient != null) {
                queryClient.close();
                queryClient = null;
            }
        } catch (IOException e) {
            log.error("es destroyDbConnect fail! cacheKey: [{}] wait gc recycle!", cacheKey, e);
        }
        connect.setQueryRestClient(null);
        connect.setRestClient(null);
        connect = null;
    }

    /**
     * 请求回调配置
     */
    public static class EsRequestConfigCallback implements RestClientBuilder.RequestConfigCallback {
        private int connectTimeout;
        private int socketTimeout;
        private int connectionRequestTimeout;

        public EsRequestConfigCallback(int connectTimeout, int socketTimeout, int connectionRequestTimeout) {
            this.connectTimeout = connectTimeout;
            this.socketTimeout = socketTimeout;
            this.connectionRequestTimeout = connectionRequestTimeout;
        }

        @Override
        public RequestConfig.Builder customizeRequestConfig(RequestConfig.Builder requestConfigBuilder) {
            return requestConfigBuilder.setConnectTimeout(connectTimeout).setSocketTimeout(socketTimeout).setConnectionRequestTimeout(connectionRequestTimeout);
        }
    }

    /**
     * 根据分表时间获取对应的es索引名
     *
     * @return
     */
    public List<String> getQueryHttpEsIndex(EsHandler queryEntity, EsTableInfo tableConf) {
        // 查询条件
        EsHandler.EsQuery esQuery = queryEntity.getEsQuery();
        // 位移
        int from = esQuery.getFrom();
        // 结果数
        int size = esQuery.getSize();
        // 分表起始时间
        String startTime = esQuery.getStartTime();
        // 分表结束时间
        String stopTime = esQuery.getStopTime();
        // 根据条件获取所有时间范围内的表名
        List<String> tableNameList = getIndexName(startTime, stopTime, tableConf);
        List<String> esQueryHeadList = new LinkedList<>();
        long timeOut = threadConfig.getTimeOut() * 1000L;
        for (int i = 0; i < tableNameList.size(); i++) {
            String periodIndexName = tableNameList.get(i);
            StringBuilder indexSearchHead = new StringBuilder();
            indexSearchHead.append("/")
                    .append(periodIndexName)
                    .append("/_search?")
                    .append("from=")
                    .append(from)
                    .append("&size=")
                    .append(size)
                    .append("&timeout=")
                    .append(timeOut)
                    .append("ms");
            esQueryHeadList.add(indexSearchHead.toString());
        }
        return esQueryHeadList;
    }

    /**
     * 根据分表时间获取对应的es索引名
     *
     * @return
     */
    public List<String> getIndexName(String startTime, String stopTime, EsTableInfo tableConf) {
        // 分表类型
        String periodType = tableConf.getPeriodType();
        // 存储周期
        Integer periodValue = tableConf.getPeriodValue();
        // 表名
        String preIndexName = tableConf.getTableName();
        // 根据条件获取所有时间范围内的表名
        List<String> tableNameList = DatatypePeriodUtils.getPeriodTableNames(preIndexName, periodType, startTime, stopTime, DbResourceEnum.es, periodValue);
        // > 1 表示存在分片 index_i
        List<String> esQueryHeadList = new LinkedList<>();
        esQueryHeadList.addAll(tableNameList);
        return esQueryHeadList;
    }

    /**
     * 根据分表时间获取对应的es索引名并进行压缩
     *
     * @return
     */
    public String getCompressIndexName(String startTime, String stopTime, EsTableInfo tableConf) {
        // 分表类型
        String periodType = tableConf.getPeriodType();
        // 存储周期
        Integer periodValue = tableConf.getPeriodValue();
        // 表名
        String preIndexName = tableConf.getTableName();
        // 根据条件获取所有时间范围内的表名
        return DatatypePeriodUtils.getCompressPeriodTableNames(preIndexName, periodType, startTime, stopTime, periodValue);
    }
}
