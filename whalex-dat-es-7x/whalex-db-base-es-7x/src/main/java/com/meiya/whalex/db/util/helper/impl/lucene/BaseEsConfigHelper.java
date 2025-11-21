package com.meiya.whalex.db.util.helper.impl.lucene;

import cn.hutool.core.codec.Base64;
import com.meiya.whalex.annotation.DbHelper;
import com.meiya.whalex.db.kerberos.KerberosJaasConfigurationUtil;
import com.meiya.whalex.db.kerberos.KerberosLoginCallBack;
import com.meiya.whalex.db.kerberos.KerberosUniformAuth;
import com.meiya.whalex.db.kerberos.KerberosUniformAuthFactory;
import com.meiya.whalex.db.kerberos.exception.GetKerberosException;
import com.meiya.whalex.util.concurrent.ThreadNamedFactory;
import org.apache.http.config.ConnectionConfig;
import org.apache.http.config.Registry;
import org.apache.http.conn.ssl.DefaultHostnameVerifier;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.util.PublicSuffixMatcher;
import org.apache.http.conn.util.PublicSuffixMatcherLoader;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import com.meiya.whalex.business.entity.CertificateConf;
import com.meiya.whalex.business.entity.DatabaseConf;
import com.meiya.whalex.business.entity.TableConf;
import com.meiya.whalex.business.service.DatabaseConfService;
import com.meiya.whalex.db.entity.AbstractCursorCache;
import com.meiya.whalex.db.entity.lucene.EsClient;
import com.meiya.whalex.db.entity.lucene.EsDatabaseInfo;
import com.meiya.whalex.db.entity.lucene.EsTableInfo;
import com.meiya.whalex.db.util.common.DatatypePeriodUtils;
import com.meiya.whalex.db.util.helper.AbstractDbModuleConfigHelper;
import com.meiya.whalex.exception.BusinessException;
import com.meiya.whalex.exception.ExceptionCode;
import com.meiya.whalex.interior.db.constant.DbResourceEnum;
import com.meiya.whalex.interior.db.constant.PeriodCycleEnum;
import com.meiya.whalex.util.JsonUtil;
import com.meiya.whalex.util.encrypt.AESUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.auth.*;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.http.message.BasicHeader;
import org.apache.http.nio.conn.NHttpClientConnectionManager;
import org.apache.http.nio.conn.NoopIOSessionStrategy;
import org.apache.http.nio.conn.SchemeIOSessionStrategy;
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.apache.http.nio.reactor.ConnectingIOReactor;
import org.apache.http.nio.reactor.IOReactorException;
import org.apache.http.nio.reactor.IOReactorExceptionHandler;
import org.apache.http.protocol.HttpContext;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;
import org.apache.http.util.EntityUtils;
import org.apache.http.util.TextUtils;
import org.elasticsearch.client.*;

import javax.net.ssl.*;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
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
        , C extends AbstractCursorCache> extends AbstractDbModuleConfigHelper<S, D, T, C> {

    private int connectTimeout = 5000;
    private int socketTimeout = 60000;
    private int connectionRequestTimeout = 60000;

    @Override
    public void init(boolean loadDbConf) {
        socketTimeout = threadConfig.getTimeOut() * 1000;
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
                return null;
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
                serviceType = EsDatabaseInfo.DEFAULT_REQUEST_TYPE;
            } else {
                if (!StringUtils.equalsAnyIgnoreCase(serviceType, EsDatabaseInfo.DEFAULT_REQUEST_TYPE, EsDatabaseInfo.HTTP_REQUEST_TYPE, EsDatabaseInfo.HTTPS_REQUEST_TYPE)) {
                    throw new BusinessException("ES 服务协议类型无法识别，仅支持 HTTP/HTTPS、HTTP、HTTPS 三种可选值!");
                }
            }
            // krb5 地址
            String krb5Path = dbMap.get("krb5Path");
            // userKeytab 地址
            String userKeytabPath = dbMap.get("userKeytabPath");
            // 用户名，当为 kerberos 认证时，为 kerberos 用户名，否正为 xpack 用户米
            String username = dbMap.get("username");
            if (StringUtils.isBlank(username)) {
                username = dbMap.get("userName");
            }
            // 密码
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
            esDatabaseInfo.setAuthType(authType);
            esDatabaseInfo.setUserName(username);
            esDatabaseInfo.setParentDir(dbMap.get("parentDir"));
            esDatabaseInfo.setPassword(password);

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

            // 设置滚动模板配置
            String openIsmTemplate = dbMap.get("openIsmTemplate");
            if (StringUtils.isNotBlank(openIsmTemplate)) {
                esDatabaseInfo.setOpenIsmTemplate(Boolean.parseBoolean(openIsmTemplate));
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
                // 分片数量
                Object numregions = tableJsonMap.get("numregions");
                if (numregions == null) {
                    numregions = tableJsonMap.get("numRegions");
                }
                // schema
                String schemaName = (String) tableJsonMap.get("schema");
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
                if (!StringUtils.isAllBlank(maxAge, maxDocs, maxSize)) {
                    EsTableInfo.EsRolloverPolicy esRolloverPolicy = new EsTableInfo.EsRolloverPolicy();
                    if (StringUtils.isNotBlank(maxAge)) {
                        esRolloverPolicy.setMaxAge(maxAge);
                    }
                    if (StringUtils.isNotBlank(maxDocs)) {
                        esRolloverPolicy.setMaxDocs(Integer.valueOf(maxDocs));
                    }
                    if (StringUtils.isNotBlank(maxSize)) {
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

                // 设置最大窗口
                Object maxResultWindow = tableJsonMap.get("maxResultWindow");
                if (maxResultWindow != null) {
                    esTableInfo.setMaxResultWindow(Long.parseLong(String.valueOf(maxResultWindow)));
                }
            } else {
                esTableInfo.setCreateTemplate(false);
                esTableInfo.setPeriodType(PeriodCycleEnum.ONLY_ONE.getPeriod());
            }
            esTableInfo.setTableName(coreName);
            return (T) esTableInfo;
        } catch (Exception e) {
            log.error("ES 数据库表配置加载失败! dbId: [{}]", conf.getId(), e);
            return null;
        }
    }

    @Override
    public S initDbConnect(D databaseConf, T tableConf) {
        String authType = databaseConf.getAuthType();
        String certificateId = databaseConf.getCertificateId();
        KerberosUniformAuth kerberosUniformLogin;
        // 获取证书
        if (!StringUtils.equalsIgnoreCase(authType, EsDatabaseInfo.CUSTOM_PROXY_AUTH) && (StringUtils.equalsIgnoreCase(authType, EsDatabaseInfo.KERBEROS_AUTH) || StringUtils.isNotBlank(certificateId))) {
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
                log.info("kerberos es 进行证书登录!");
                kerberosUniformLogin.login(KerberosJaasConfigurationUtil.Module.ES, new KerberosLoginCallBack() {
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
        esClient.setRestClient(getRestClient(databaseConf.getRequestType(), uri, authType, databaseConf.getUserName(), databaseConf.getPassword()));

        // 读写分离情况下，建立读连接
        if (StringUtils.isNotBlank(databaseConf.getQueryServerAddress())) {
            String queryUri = databaseConf.getQueryServerAddress();
            esClient.setQueryRestClient(getRestClient(databaseConf.getRequestType(), queryUri, authType, databaseConf.getUserName(), databaseConf.getPassword()));
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
    protected RestClient getRestClient(String requestType, String uri, String authType, String username, String password) {
        // 存在证书需要进行认证登录
        String httpType = null;
        if (StringUtils.equalsIgnoreCase(requestType, EsDatabaseInfo.DEFAULT_REQUEST_TYPE)) {
            if (StringUtils.equalsIgnoreCase(authType, EsDatabaseInfo.KERBEROS_AUTH) || StringUtils.equalsIgnoreCase(authType, EsDatabaseInfo.CUSTOM_PROXY_AUTH)) {
                log.info("uri: [{}] 开启 authType: [{}] 模式 username: [{}] password: [{}]", uri, authType, username, password);
                httpType = kerberosHttpType();
            } else {
                httpType = "http";
            }
        } else {
            httpType = requestType;
        }
        RestClientBuilder builder = null;
        RestClient restClient = null;
        uri = uri.replaceAll("http[s]?://", "");
        List<HttpHost> httpHostList = new ArrayList<>();
        for (String ipPortStr : uri.split("[,;]")) {
            String[] ip_port = ipPortStr.split(":");
            if (ip_port.length != 2) {
                throw new RuntimeException("ip或端口配置格式问题, 地址：[" + uri + "], 异常部分: [" + ip_port + "]");
            }
            String ip = ip_port[0];
            int port = Integer.parseInt(ip_port[1]);
            httpHostList.add(new HttpHost(ip, port, httpType));
        }
        log.info("ES REST Client httpType: [{}]", httpType);
        // 创建构建对象
        builder = RestClient.builder(httpHostList.toArray(new HttpHost[httpHostList.size()]));
        // 配置Http连接
        String finalUri = uri;
        String finalHttpType = httpType;
        builder.setHttpClientConfigCallback(httpAsyncClientBuilder -> {
            // 最大连接数
            httpAsyncClientBuilder.setMaxConnTotal(this.threadConfig.getMaximumPoolSize());
            // 最大并发数
            httpAsyncClientBuilder.setMaxConnPerRoute(this.threadConfig.getMaximumPoolSize());
            SSLContext sslContext = null;
            // xPack 认证方式
            if (StringUtils.equalsIgnoreCase(authType, EsDatabaseInfo.X_PACK_AUTH)) {
                log.info("uri: [{}] 开启 authType: [{}] 模式 username: [{}] password: [{}]", finalUri, authType, username, password);
                CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
                httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                // 判断是否是 https
                if (StringUtils.equalsIgnoreCase(finalHttpType, EsDatabaseInfo.HTTPS_REQUEST_TYPE)) {
                    try {
                        sslContext = new SSLContextBuilder().loadTrustMaterial(null,  (chain, auth) -> true).build();
                    } catch (Exception e) {
                        throw new BusinessException(EsDatabaseInfo.X_PACK_AUTH + " 认证方式 HTTPS 连接添加 SSLContext 异常!", e);
                    }
                    SSLIOSessionStrategy sessionStrategy = new SSLIOSessionStrategy(sslContext, NoopHostnameVerifier.INSTANCE);
                    httpAsyncClientBuilder.setSSLStrategy(sessionStrategy);
                }
            }
            // 头部信息
            List<Header> headers = new ArrayList<>(4);
            headers.add(new BasicHeader("Accept", "application/json"));
            headers.add(new BasicHeader("Content-type", "application/json"));
            headers.add(new BasicHeader("Connection", "keep-alive"));
            headers.add(new BasicHeader("Keep-Alive", "720"));
            // 开启 kerberos 认证
            if (StringUtils.equalsIgnoreCase(authType, EsDatabaseInfo.KERBEROS_AUTH)) {
                sslContext = kerberosAuth(httpAsyncClientBuilder, username, password);
            }
            // 通过 CUSTOM_PROXY_AUTH 方式绕过 kerberos 认证
            if (StringUtils.equalsIgnoreCase(authType, EsDatabaseInfo.CUSTOM_PROXY_AUTH)) {
                try {
                    sslContext = SSLContext.getInstance("TLS");
                    sslContext.init(null, new TrustManager[]{UnsafeX509ExtendedTrustManager.getInstance()}, null);
                    httpAsyncClientBuilder.setSSLContext(sslContext).setSSLHostnameVerifier(new NoopHostnameVerifier());
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
            // 设置请求头
            httpAsyncClientBuilder.setDefaultHeaders(headers);
            // 设置 keep-alive 策略
            httpAsyncClientBuilder.setKeepAliveStrategy(CustomConnectionKeepAliveStrategy.INSTANCE);
            // 配置 IO 线程池相关配置
            IOReactorConfig ioReactorConfig = IOReactorConfig.custom().setConnectTimeout(connectTimeout).setIoThreadCount(Math.max(Runtime.getRuntime().availableProcessors() / 2, 12)).setSoTimeout(socketTimeout).setSoKeepAlive(true).build();
            httpAsyncClientBuilder.setDefaultIOReactorConfig(ioReactorConfig);
            // 创建 IO Reactor 线程
            ConnectingIOReactor ioReactor = createConnectingIOReactor(ioReactorConfig, new ThreadNamedFactory("elasticsearch-rest-io-" + httpHostList.get(0).getHostName()));
            // 自定义连接管理器
            NHttpClientConnectionManager connectionManager = createConnectionManager(sslContext
                    , false
                    , new NoopHostnameVerifier()
                    , null
                    , ioReactor
                    , null
                    , this.threadConfig.getMaximumPoolSize(),
                    this.threadConfig.getMaximumPoolSize());
            httpAsyncClientBuilder.setConnectionManager(connectionManager);
            return httpAsyncClientBuilder;
        }).setRequestConfigCallback(new EsRequestConfigCallback(connectTimeout, socketTimeout, connectionRequestTimeout, true, 3));
        // 创建连接
        restClient = builder.build();
        return restClient;
    }

    /**
     * kerberos 认证协议类型
     *
     * @return
     */
    protected String kerberosHttpType() {
        return "http";
    }

    /**
     * kerberos 认证
     *
     * @param builder
     * @return
     */
    protected SSLContext kerberosAuth(HttpAsyncClientBuilder builder, String username, String password) {
        SpnegoHttpClientConfigCallbackHandler handler = new SpnegoHttpClientConfigCallbackHandler(username);
        handler.customizeHttpClient(builder);
        return null;
    }

    /**
     * 创建 IO 线程池
     * 处理多线程高并发入库场景下，IO 线程异常导致后续复用此线程的调用都包 IO STOP 异常
     *
     * @param config
     * @param threadFactory
     * @return
     */
    private ConnectingIOReactor createConnectingIOReactor(IOReactorConfig config, ThreadFactory threadFactory) {
        try {
            DefaultConnectingIOReactor reactor = new DefaultConnectingIOReactor(config, threadFactory);
            reactor.setExceptionHandler(new IOReactorExceptionHandler() {
                @Override
                public boolean handle(IOException e) {
                    log.warn("System may be unstable: IOReactor encountered a checked exception : " + e.getMessage(), e);
                    // 返回 true 标记当前异常已经被处理
                    return true;
                }

                @Override
                public boolean handle(RuntimeException e) {
                    log.warn("System may be unstable: IOReactor encountered a runtime exception : " + e.getMessage(), e);
                    // 返回 true 标记当前异常已经被处理
                    return true;
                }
            });
            return reactor;
        } catch (IOReactorException exception) {
            throw new IllegalStateException(exception);
        }
    }

    /**
     * 创建连接管理器
     *
     * @param sslContext
     * @param systemProperties
     * @param hostnameVerifier
     * @param publicSuffixMatcher
     * @param ioReactor
     * @param defaultConnectionConfig
     * @param maxConnTotal
     * @param maxConnPerRoute
     * @return
     */
    private NHttpClientConnectionManager createConnectionManager(SSLContext sslContext
            , boolean systemProperties
            , HostnameVerifier hostnameVerifier
            , PublicSuffixMatcher publicSuffixMatcher
            , ConnectingIOReactor ioReactor
            , ConnectionConfig defaultConnectionConfig
            , int maxConnTotal
            , int maxConnPerRoute) {
        if (publicSuffixMatcher == null) {
            publicSuffixMatcher = PublicSuffixMatcherLoader.getDefault();
        }
        NHttpClientConnectionManager connManager;
        Object userTokenHandler;
        SSLContext sslcontext = sslContext;
        if (sslcontext == null) {
            if (systemProperties) {
                sslcontext = SSLContexts.createSystemDefault();
            } else {
                sslcontext = SSLContexts.createDefault();
            }
        }

        String[] supportedProtocols = systemProperties ? split(System.getProperty("https.protocols")) : null;
        String[] supportedCipherSuites = systemProperties ? split(System.getProperty("https.cipherSuites")) : null;
        userTokenHandler = hostnameVerifier;
        if (userTokenHandler == null) {
            userTokenHandler = new DefaultHostnameVerifier(publicSuffixMatcher);
        }

        SSLIOSessionStrategy reuseStrategy = new SSLIOSessionStrategy(sslcontext, supportedProtocols, supportedCipherSuites, (HostnameVerifier) userTokenHandler);

        RegistryBuilder<SchemeIOSessionStrategy> registryBuilder = RegistryBuilder.create();
        Registry<SchemeIOSessionStrategy> strategyRegistry = registryBuilder.register("http", NoopIOSessionStrategy.INSTANCE).register("https", reuseStrategy).build();
        PoolingNHttpClientConnectionManager poolingmgr = new PoolingNHttpClientConnectionManager(ioReactor, strategyRegistry);
        if (defaultConnectionConfig != null) {
            poolingmgr.setDefaultConnectionConfig(defaultConnectionConfig);
        }

        if (systemProperties) {
            String s = System.getProperty("http.keepAlive", "true");
            if ("true".equalsIgnoreCase(s)) {
                s = System.getProperty("http.maxConnections", "5");
                int max = Integer.parseInt(s);
                poolingmgr.setDefaultMaxPerRoute(max);
                poolingmgr.setMaxTotal(2 * max);
            }
        } else {
            if (maxConnTotal > 0) {
                poolingmgr.setMaxTotal(maxConnTotal);
            }

            if (maxConnPerRoute > 0) {
                poolingmgr.setDefaultMaxPerRoute(maxConnPerRoute);
            }
        }
        connManager = poolingmgr;
        return connManager;
    }

    private static String[] split(String s) {
        return TextUtils.isBlank(s) ? null : s.split(" *, *");
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
         * 设置默认1分钟，可以观察机器状态未 TIME_WAIT 的TCP连接数，如果太多，可以调大
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
        private boolean redirectsEnabled;
        private int maxRedirects;

        public EsRequestConfigCallback(int connectTimeout, int socketTimeout, int connectionRequestTimeout, boolean redirectsEnabled, int maxRedirects) {
            this.connectTimeout = connectTimeout;
            this.socketTimeout = socketTimeout;
            this.connectionRequestTimeout = connectionRequestTimeout;
            this.redirectsEnabled = redirectsEnabled;
            this.maxRedirects = maxRedirects;
        }

        @Override
        public RequestConfig.Builder customizeRequestConfig(RequestConfig.Builder requestConfigBuilder) {
            return requestConfigBuilder.setConnectTimeout(connectTimeout).setSocketTimeout(socketTimeout).setConnectionRequestTimeout(connectionRequestTimeout).setRedirectsEnabled(redirectsEnabled).setMaxRedirects(maxRedirects);
        }
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
