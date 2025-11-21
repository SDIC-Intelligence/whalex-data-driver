package com.meiya.whalex.db.util.helper.impl.document;

import com.meiya.whalex.business.entity.DatabaseConf;
import com.meiya.whalex.business.entity.TableConf;
import com.meiya.whalex.db.entity.document.MongoCursorCache;
import com.meiya.whalex.db.entity.document.MongoDatabaseInfo;
import com.meiya.whalex.db.entity.document.MongoTableInfo;
import com.meiya.whalex.db.util.helper.AbstractDbModuleConfigHelper;
import com.meiya.whalex.exception.BusinessException;
import com.meiya.whalex.exception.ExceptionCode;
import com.meiya.whalex.util.JsonUtil;
import com.meiya.whalex.util.encrypt.AESUtil;
import com.mongodb.*;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.connection.ClusterType;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * MONGODB 数据源配置管理
 *
 * @author 黄河森
 * @date 2019/9/18
 * @project whale-cloud-platformX
 */
@Slf4j
public class BaseMongoConfigHelper extends AbstractDbModuleConfigHelper<MongoClient, MongoDatabaseInfo, MongoTableInfo, MongoCursorCache> {

    private final static Logger LOGGER = LoggerFactory.getLogger(BaseMongoConfigHelper.class);

    /**
     * 连接建立保持时间
     */
    private final static int SOCKET_TIMEOUT = 3 * 60 * 1000;

    /**
     * 连接超时
     */
    private final static int CONNECT_TIMEOUT = 5 * 1000;

    @Override
    public boolean checkDataSourceStatus(MongoClient connect) throws Exception {
        connect.listDatabases();
        return true;
    }

    @Override
    public MongoDatabaseInfo initDbModuleConfig(DatabaseConf conf) {
        try {
            String connSetting = conf.getConnSetting();
            if (StringUtils.isBlank(connSetting)) {
                LOGGER.warn("获取mongodb组件数据库部署定义为空 " + conf.getBigdataResourceId());
                throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "数据库配置必填参数缺失，请校验参数配置是否正确!");
            }
            MongoDatabaseInfo mongoDatabaseInfo = new MongoDatabaseInfo();
            Map<String, Object> dbMap = JsonUtil.jsonStrToObject(connSetting, Map.class);
            mongoDatabaseInfo.setAuthDatabase((String) dbMap.get("authDatabase"));
            mongoDatabaseInfo.setUserName((String) dbMap.get("userName"));
            mongoDatabaseInfo.setPassword((String) dbMap.get("password"));
            // 认证方式
            Object encrypt = dbMap.get("encrypt");
            if (encrypt == null) {
                mongoDatabaseInfo.setEncrypt(true);
            } else {
                if (StringUtils.isNotBlank(StringUtils.trim(String.valueOf(encrypt)))) {
                    String valueOf = String.valueOf(encrypt);
                    if (valueOf.equalsIgnoreCase("false")) {
                        mongoDatabaseInfo.setEncrypt(false);
                    } else {
                        mongoDatabaseInfo.setEncrypt(true);
                    }
                } else {
                    mongoDatabaseInfo.setEncrypt(true);
                }
            }
            List<String> serverUrlList = new ArrayList<>();
            mongoDatabaseInfo.setServerUrl(serverUrlList);
            String url = (String) dbMap.get("url");
            String serviceUrl = (String) dbMap.get("serviceUrl");
            String port = (String) dbMap.get("port");
            String databaseName = (String) dbMap.get("database");
            // 是否开启SSL
            Object enableSSL = dbMap.get("enableSSL");
            if (enableSSL != null && String.valueOf(enableSSL).equalsIgnoreCase(Boolean.TRUE.toString())) {
                String trustStorePath = (String) dbMap.get("trustStorePath");
                String trustStorePassword = (String) dbMap.get("trustStorePassword");
                String keyStore = (String) dbMap.get("keyStorePath");
                String keyStorePassword = (String) dbMap.get("keyStorePassword");
                Object sslInvalidHostNameAllowed = dbMap.get("sslInvalidHostNameAllowed");
                mongoDatabaseInfo.setEnableSSL(Boolean.TRUE);
                mongoDatabaseInfo.setTrustStorePath(trustStorePath);
                mongoDatabaseInfo.setTrustStorePassword(trustStorePassword);
                mongoDatabaseInfo.setKeyStorePath(keyStore);
                mongoDatabaseInfo.setKeyStorePassword(keyStorePassword);
                if (sslInvalidHostNameAllowed != null && String.valueOf(sslInvalidHostNameAllowed).equalsIgnoreCase(Boolean.TRUE.toString())) {
                    mongoDatabaseInfo.setSslInvalidHostNameAllowed(Boolean.TRUE);
                }
            }
            // 认证类型
            Object authTypeObj = dbMap.get("authType");
            if (authTypeObj != null && StringUtils.isNotBlank(String.valueOf(authTypeObj))) {
                String authType = String.valueOf(authTypeObj);
                try {
                    AuthenticationMechanism authenticationMechanism = AuthenticationMechanism.fromMechanismName(authType.toUpperCase());
                    mongoDatabaseInfo.setAuthenticationMechanism(authenticationMechanism);
                } catch (Exception e) {
                    log.error("mongodb databaseInfo:[{}] 不能够识别 authType: [{}]!", StringUtils.isNotBlank(conf.getBigdataResourceId()) ? conf.getBigdataResourceId() : conf.getConnSetting(), authType, e);
                    throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "数据库配置 authType 无法识别!");
                }
            }

            Boolean isURIConnect = Boolean.FALSE;

            //URI 连接方式支持
            Object isURIConnectObj = dbMap.get("isURIConnect");
            if (isURIConnectObj != null) {
                isURIConnect = Boolean.valueOf(String.valueOf(isURIConnectObj));
                if (isURIConnect) {
                    mongoDatabaseInfo.setURIConnect(true);
                }
            }

            if (isURIConnect) {
                if (StringUtils.isBlank(serviceUrl)){
                    if(StringUtils.isBlank(url)) {
                        LOGGER.warn("获取mongodb组件数据库部署定义地址为空 " + conf.getBigdataResourceId());
                        throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "数据库配置必填参数缺失，请校验参数配置是否正确!");
                    }
                    serviceUrl = url;
                }
            } else {
                if (StringUtils.isBlank(serviceUrl) || StringUtils.isBlank(port) || StringUtils.isBlank(databaseName)) {
                    if(StringUtils.isBlank(url)) {
                        LOGGER.warn("获取mongodb组件数据库部署定义地址或端口或数据库名为空 " + conf.getBigdataResourceId());
                        throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "数据库配置必填参数缺失，请校验参数配置是否正确!");
                    }
                    String[] urlStr = url.split(",");
                    StringBuilder serviceUrlBuilder = new StringBuilder();
                    StringBuilder portBuilder = new StringBuilder();
                    for (int i = 0; i < urlStr.length; i++) {
                        String[] hostAndPort = urlStr[i].split(":");
                        if(hostAndPort.length != 2) {
                            LOGGER.warn("获取mongodb组件数据库部署定义地址或端口或数据库名为空 " + conf.getBigdataResourceId());
                            throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "数据库配置必填参数缺失，请校验参数配置是否正确!");
                        }

                        if(i != 0) {
                            serviceUrlBuilder.append(",");
                            portBuilder.append(",");
                        }

                        serviceUrlBuilder.append(hostAndPort[0]);
                        portBuilder.append(hostAndPort[1]);
                    }
                    serviceUrl = serviceUrlBuilder.toString();
                    port = portBuilder.toString();
                }
            }

            mongoDatabaseInfo.setDatabaseName(databaseName);

            if (!isURIConnect) {
                String isRange = StringUtils.substringBetween(serviceUrl, "(", ")");
                if (StringUtils.isNotBlank(isRange) || StringUtils.contains(serviceUrl, ",")) {
                    Set<String> serverSet = new HashSet<>();
                    String[] servers = StringUtils.split(serviceUrl, ",");
                    String[] ports = StringUtils.split(port, ",");
                    for (int i = 0; i < servers.length; i++) {
                        String server = servers[i];
                        String portIndex;
                        if (ports.length == 1) {
                            portIndex = ports[0];
                        } else {
                            portIndex = ports[i];
                        }
                        String range = StringUtils.substringBetween(server, "(", ")");
                        if (StringUtils.isNotBlank(range)) {
                            String[] rangeSplit = StringUtils.split(range, "|");
                            for (int k = 0; k < rangeSplit.length; k++) {
                                String aRangeSplit = StringUtils.trim(rangeSplit[k]);
                                String[] splits = StringUtils.split(aRangeSplit, "-");
                                if (null != splits && splits.length > 1) {
                                    int start = Integer.valueOf(splits[0]);
                                    int stop = Integer.valueOf(splits[1]);
                                    for (int j = start; j <= stop; j++) {
                                        String trueServer = StringUtils.replace(server, "(" + range + ")", "" + j);
                                        trueServer = trueServer + ":" + portIndex;
                                        serverSet.add(trueServer);
                                    }
                                } else if (StringUtils.isNotBlank(aRangeSplit)) {
                                    String trueServer = StringUtils.replace(server, "(" + range + ")", aRangeSplit);
                                    trueServer = trueServer + ":" + portIndex;
                                    serverSet.add(trueServer);
                                }
                            }
                        } else {
                            server = server + ":" + portIndex;
                            serverSet.add(server);
                        }
                    }
                    serverUrlList.addAll(serverSet);
                } else {
                    serverUrlList.add(serviceUrl + ":" + port);
                }
            } else {
                serverUrlList.add(serviceUrl);
            }
            // 集群模式
            Object clusterTypeObj = dbMap.get("clusterType");
            if (clusterTypeObj != null) {
                String clusterType = String.valueOf(clusterTypeObj);
                ClusterType clusterTypeEnum = ClusterType.valueOf(clusterType.toUpperCase());
                mongoDatabaseInfo.setClusterType(clusterTypeEnum);
            } else {
                if (serverUrlList.size() > 1) {
                    mongoDatabaseInfo.setClusterType(ClusterType.SHARDED);
                } else {
                    mongoDatabaseInfo.setClusterType(ClusterType.STANDALONE);
                }
            }
            return mongoDatabaseInfo;
        } catch (Exception e) {
            LOGGER.error("MONGODB 数据库配置加载失败! bigResourceId" + conf.getBigdataResourceId(), e);
            throw new BusinessException(e, ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "数据库配置解析异常，请校验参数配置是否正确!");
        }
    }

    @Override
    public MongoTableInfo initTableConfig(TableConf conf) {
        MongoTableInfo mongoTableInfo = new MongoTableInfo();
        mongoTableInfo.setTableName(conf.getTableName());
        String tableJson = conf.getTableJson();
        if (StringUtils.isNotBlank(tableJson)) {
            Map<String, Object> tableMap = JsonUtil.jsonStrToMap(tableJson);
            Object numInitialChunks = tableMap.get("numInitialChunks");
            if (numInitialChunks != null && StringUtils.isNumeric(String.valueOf(numInitialChunks))) {
                mongoTableInfo.setNumInitialChunks(Integer.valueOf(String.valueOf(numInitialChunks)));
            }
            Object replica = tableMap.get("replica");
            if (replica != null && StringUtils.isNumeric(String.valueOf(replica))) {
                mongoTableInfo.setReplica(Integer.valueOf(String.valueOf(replica)));
            }
            Object capped = tableMap.get("capped");
            if (capped != null) {
                mongoTableInfo.setCapped(Boolean.valueOf(String.valueOf(capped)));
            }
            Object sizeInBytes = tableMap.get("sizeInBytes");
            if (replica != null && StringUtils.isNumeric(String.valueOf(sizeInBytes))) {
                mongoTableInfo.setSizeInBytes(Long.valueOf(String.valueOf(sizeInBytes)));
            }
            Object maxDocuments = tableMap.get("maxDocuments");
            if (replica != null && StringUtils.isNumeric(String.valueOf(maxDocuments))) {
                mongoTableInfo.setMaxDocuments(Long.valueOf(String.valueOf(maxDocuments)));
            }
        }
        return mongoTableInfo;
    }

    @Override
    public MongoClient initDbConnect(MongoDatabaseInfo databaseConf, MongoTableInfo tableInfo) {
        String userName = databaseConf.getUserName();
        String password = databaseConf.getPassword();
        if (StringUtils.isNotBlank(password)) {
            // AES 解密
            try {
                password = AESUtil.decrypt(password, "utf-8");
            } catch (Exception e) {
            }
        } else {
            password = "";
        }

        MongoClientSettings.Builder builder = MongoClientSettings.builder();
        builder.writeConcern(WriteConcern.ACKNOWLEDGED);

        MongoClient mongoClient = null;
        // mongodb uri
        StringBuilder uriSb = new StringBuilder();
        List<String> serverUrl = databaseConf.getServerUrl();
        if (databaseConf.isURIConnect()) {
            uriSb.append(serverUrl.get(0));
        } else {
            uriSb.append("mongodb://");
            for (String server : serverUrl) {
                uriSb.append(server).append(",");
            }
            uriSb.delete(uriSb.length() - 1, uriSb.length());

            uriSb.append("/").append(StringUtils.isBlank(databaseConf.getAuthDatabase())? "admin" : databaseConf.getAuthDatabase());

        }


        if (StringUtils.isNotBlank(userName) && StringUtils.isNotBlank(password)) {
            MongoCredential mongoCredential = getMongoCredential(databaseConf, userName, password);
            builder.credential(mongoCredential);
        }

        // 设置相关连接参数
        uriSb
//                .append("?minConnectionPoolSize=").append(threadConfig.getCorePoolSize())
                .append("&maxConnectionPoolSize=").append(threadConfig.getMaximumPoolSize())
                        .append("&socketTimeout=").append(SOCKET_TIMEOUT)
                        .append("&connectTimeout=").append(CONNECT_TIMEOUT);

        // 是否开启 SSL
        if (databaseConf.isEnableSSL()) {
            uriSb.append("&sslEnabled=").append(true)
                 .append("&sslInvalidHostnameAllowed=").append(databaseConf.isSslInvalidHostNameAllowed());

            System.setProperty("javax.net.ssl.trustStore", databaseConf.getTrustStorePath());
            System.setProperty("javax.net.ssl.trustStorePassword", databaseConf.getTrustStorePath());
            System.setProperty("javax.net.ssl.keyStore", databaseConf.getKeyStorePath());
            System.setProperty("javax.net.ssl.keyStorePassword", databaseConf.getKeyStorePassword());
        }

        MongoClientSettings mongoClientSettings = builder.applyConnectionString(new ConnectionString(uriSb.toString())).build();

        mongoClient = MongoClients.create(mongoClientSettings);

        return mongoClient;
    }

    private MongoCredential getMongoCredential(MongoDatabaseInfo databaseConf, String userName, String password) {
        // 根据不太认证方式进行认证
        AuthenticationMechanism authenticationMechanism = databaseConf.getAuthenticationMechanism();
        MongoCredential mc = null;
        if (authenticationMechanism == null) {
            mc = MongoCredential.createCredential(userName, StringUtils.isBlank(databaseConf.getAuthDatabase()) ? "admin" : databaseConf.getAuthDatabase(), password.toCharArray());
        } else {
            switch (authenticationMechanism) {
                case SCRAM_SHA_1:
                    mc = MongoCredential.createScramSha1Credential(userName, StringUtils.isBlank(databaseConf.getAuthDatabase()) ? "admin" : databaseConf.getAuthDatabase(), password.toCharArray());
                    break;
                default:
                    throw new UnsupportedOperationException("mongodb组件 暂时不支持 [" + authenticationMechanism.getMechanismName() + "] 认证方式!");
            }
        }
        return mc;
    }

    @Override
    public void destroyDbConnect(String cacheKey, MongoClient connect) {
        connect.close();
    }
}
