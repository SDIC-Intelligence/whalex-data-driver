package com.meiya.whalex.db.util.helper.impl.dwh;

import cn.hutool.core.codec.Base64;
import com.meiya.whalex.annotation.DbHelper;
import com.meiya.whalex.business.entity.CertificateConf;
import com.meiya.whalex.business.entity.DatabaseConf;
import com.meiya.whalex.business.entity.TableConf;
import com.meiya.whalex.business.service.DatabaseConfService;
import com.meiya.whalex.db.entity.AbstractCursorCache;
import com.meiya.whalex.db.entity.dwh.HiveClient;
import com.meiya.whalex.db.entity.dwh.HiveDatabaseInfo;
import com.meiya.whalex.db.entity.dwh.HiveTableInfo;
import com.meiya.whalex.db.kerberos.KerberosLoginCallBack;
import com.meiya.whalex.db.kerberos.KerberosUniformAuth;
import com.meiya.whalex.db.kerberos.KerberosUniformAuthFactory;
import com.meiya.whalex.db.util.helper.AbstractDbModuleConfigHelper;
import com.meiya.whalex.exception.BusinessException;
import com.meiya.whalex.exception.ExceptionCode;
import com.meiya.whalex.util.JsonUtil;
import com.meiya.whalex.util.concurrent.ThreadNamedFactory;
import com.meiya.whalex.util.encrypt.AESUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.dbcp.BasicDataSourceFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.security.UserGroupInformation;import org.apache.zookeeper.server.util.KerberosUtil;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Hive 配置工具类
 *
 * @author 黄河森
 * @date 2019/12/26
 * @project whale-cloud-platformX
 */
@Slf4j
public class BaseHiveConfigHelper<S extends HiveClient
        , D extends HiveDatabaseInfo
        , T extends HiveTableInfo
        , C extends AbstractCursorCache> extends AbstractDbModuleConfigHelper<S, D, T, C> {

    /**
     * DEFAULT_REALM
     */
    protected static final String DEFAULT_REALM = "HADOOP.COM";

    /**
     * 登录异常提示
     */
    protected static final String logIN_FAILED_CAUSE_PASSWORD_WRONG =
            "(wrong password) keytab file and user not match, you can kinit -k -t keytab user in client server to check";

    protected static final String logIN_FAILED_CAUSE_TIME_WRONG =
            "(clock skew) time of local server and remote server not match, please check ntp to remote server";

    protected static final String logIN_FAILED_CAUSE_AES256_WRONG =
            "(aes256 not support) aes256 not support by default jdk/jre, need copy local_policy.jar and US_export_policy.jar from remote server in path /opt/huawei/Bigdata/jdk/jre/lib/security";

    protected static final String logIN_FAILED_CAUSE_PRINCIPAL_WRONG =
            "(no rule) principal format not support by default, need add property hadoop.security.auth_to_local(in core-site.xml) value RULE:[1:$1] RULE:[2:$1]";

    protected static final String logIN_FAILED_CAUSE_TIME_OUT =
            "(time out) can not connect to kdc server or there is fire wall in the network";

    private static final String DEFAULT_JOB_ENGINE = HiveDatabaseInfo.MR_JOB;

    private static final String DEFAULT_LINK_TYPE = HiveDatabaseInfo.HIVE_SERVER_LINK_TYPE;

    public String getDefaultJobEngine() {
        return DEFAULT_JOB_ENGINE;
    }

    public String getDefaultLinkType() {
        return DEFAULT_LINK_TYPE;
    }

    @Override
    public boolean checkDataSourceStatus(S connect) throws Exception {
        Connection connection = null;
        Statement statement = null;
        try {
            connection = connect.getDataSource().getConnection();
            statement = connection.createStatement();
            statement.execute("show tables");
        } finally {
            if (statement != null) {
                statement.close();
            }
            if(connection != null) {
                connection.close();
            }
        }
        return true;
    }

    @Override
    public D initDbModuleConfig(DatabaseConf conf) {
        // 历史遗留原因，只有连接方式是 02 的才为 hive jdbc
        if (StringUtils.isBlank(conf.getConnTypeId()) || "02".equals(conf.getConnTypeId())) {
            String connSetting = conf.getConnSetting();
            Map<String, String> dbMap = JsonUtil.jsonStrToObject(connSetting, Map.class);
            String serviceUrl = dbMap.get("serviceUrl");
            String database = dbMap.get("database");
            if (StringUtils.isBlank(serviceUrl)) {
                log.error("hive init database config fail, must field info is null! bigResourceId: [{}]", conf.getBigdataResourceId());
                throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "数据库配置必填参数缺失，请校验参数配置是否正确!");
            }
            HiveDatabaseInfo databaseInfo = new HiveDatabaseInfo();
            databaseInfo.setServiceUrl(serviceUrl);
            databaseInfo.setDatabaseName(database);
            databaseInfo.setUserName(dbMap.get("userName"));

            String password = dbMap.get("password");
            // AES 解密
            if (StringUtils.isNotBlank(password)) {
                try {
                    password = AESUtil.decrypt(password);
                } catch (Exception e) {
                }
            }

            databaseInfo.setPassword(password);

            // 证书信息
            String certificateId = dbMap.get("certficateId");
            if (StringUtils.isBlank(certificateId)) {
                certificateId = dbMap.get("certificateId");
            }

            databaseInfo.setUserKeytabPath(dbMap.get("userKeytabPath"));
            databaseInfo.setKrb5Path(dbMap.get("krb5Path"));
            // kerberos 认证主题名称
            databaseInfo.setKerberosPrincipal(dbMap.get("kerberosPrincipal"));
            if (StringUtils.isBlank(databaseInfo.getKerberosPrincipal())) {
                log.warn("HIVE bigResourceId: [{}] 未配置 principal, 将使用默认 [hive/hadoop.hadoop.com@HADOOP.COM] 作为 principal!", conf.getBigdataResourceId());
                databaseInfo.setKerberosPrincipal("hive/hadoop.hadoop.com@HADOOP.COM");
            }
            // zk 命名空间
            databaseInfo.setZooKeeperNamespace(dbMap.get("zooKeeperNamespace"));
            if (StringUtils.isBlank(databaseInfo.getZooKeeperNamespace())) {
                log.warn("HIVE bigResourceId: [{}] 未配置 zooKeeperNamespace, 将使用默认 [hiveserver2] 作为 zooKeeperNamespace!", conf.getBigdataResourceId());
                databaseInfo.setZooKeeperNamespace("hiveserver2");
            }

            //证书，base64字符串
            String certificateBase64Str = dbMap.get("certificateBase64Str");
            if (StringUtils.isNotBlank(certificateBase64Str)) {
                databaseInfo.setCertificateBase64Str(certificateBase64Str);
            }

            // 认证类型
            String authType = dbMap.get("authType");
            if (StringUtils.isNotBlank(certificateId)) {
                databaseInfo.setCertificateId(certificateId);
            }
            if (StringUtils.isNotBlank(authType)) {
                databaseInfo.setAuthType(authType);
            } else {
                if (StringUtils.isNotBlank(certificateId) || StringUtils.isNotBlank(certificateBase64Str)) {
                    databaseInfo.setAuthType("cer");
                } else {
                    databaseInfo.setAuthType("normal");
                }
            }

            // 资源队列
            databaseInfo.setQueueName(dbMap.get("queueName"));

            // 连接类型
            String linkType = dbMap.get("linkType");
            if (StringUtils.isNotBlank(linkType)) {
                databaseInfo.setLinkType(linkType);
            } else {
                databaseInfo.setLinkType(getDefaultLinkType());
            }

            // 任务引擎类型
            String jobEngine = dbMap.get("jobEngine");
            if (StringUtils.isNotBlank(jobEngine)) {
                databaseInfo.setJobEngine(jobEngine);
            } else {
                databaseInfo.setJobEngine(getDefaultJobEngine());
            }

            databaseInfo.setMaxActive(threadConfig.getMaximumPoolSize());

            // 证书存放根目录
            databaseInfo.setParentDir(dbMap.get("parentDir"));

            String metaStoreUrl = dbMap.get("metaStoreUrl");
            if (StringUtils.isNotBlank(metaStoreUrl)) {
                databaseInfo.setMetaStoreUrl(metaStoreUrl);
                String metaStoreMaxConnectionCount = dbMap.get("metaStoreMaxConnectionCount");
                int maxCount = 10;
                if(StringUtils.isNotBlank(metaStoreMaxConnectionCount)) {
                    maxCount = Integer.valueOf(metaStoreMaxConnectionCount);
                }
                databaseInfo.setMetaStoreMaxConnectionCount(maxCount);
                boolean executeSetugi = true;
                String metaStoreExecuteSetugi = dbMap.get("metaStoreExecuteSetugi");
                if(StringUtils.isNotBlank(metaStoreExecuteSetugi)) {
                    executeSetugi = Boolean.valueOf(metaStoreExecuteSetugi);
                }
                databaseInfo.setMetaStoreExecuteSetugi(executeSetugi);
            }

            return (D) databaseInfo;
        }
        return null;
    }

    @Override
    public T initTableConfig(TableConf conf) {
        HiveTableInfo hiveTableInfo = new HiveTableInfo();
        String tableName = conf.getTableName();
        if (StringUtils.isBlank(tableName)) {
            log.error("hive init table config fail, tableName is null! dbId: [{}]", conf.getId());
            throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "数据表配置必填参数缺失，请校验参数配置是否正确!");
        }
        hiveTableInfo.setTableName(tableName);
        String tableJson = conf.getTableJson();
        if (StringUtils.isBlank(tableJson)) {
            log.warn("HIVE 组件表配置缺少 tableJson !!! dbId:[{}]", conf.getId());
            return (T) hiveTableInfo;
        }
        Map<String, Object> tableConfMap = JsonUtil.jsonStrToObject(tableJson, Map.class);
        hiveTableInfo.setFileType(tableConfMap.get("fileType") == null ? null : (String) tableConfMap.get("fileType"));
        hiveTableInfo.setFieldsTerminated(tableConfMap.get("fieldsTerminated") == null ? null : (String) tableConfMap.get("fieldsTerminated"));
        hiveTableInfo.setCollectionItemsTerminated(tableConfMap.get("collectionItemsTerminated") == null ? null : (String) tableConfMap.get("collectionItemsTerminated"));
        hiveTableInfo.setMapKeysTerminated(tableConfMap.get("mapKeysTerminated") == null ? null : (String) tableConfMap.get("mapKeysTerminated"));
        hiveTableInfo.setLinesTerminated(tableConfMap.get("linesTerminated") == null ? null : (String) tableConfMap.get("linesTerminated"));
        hiveTableInfo.setPath(tableConfMap.get("path") == null ? null : (String) tableConfMap.get("path"));
        hiveTableInfo.setFormat(tableConfMap.get("format") == null ? null : (String) tableConfMap.get("format"));
        String randomMemory = String.valueOf(tableConfMap.get("randomMemory"));
        if (StringUtils.isNotBlank(randomMemory) && StringUtils.isNumeric(randomMemory)) {
            hiveTableInfo.setRandomMemory(tableConfMap.get("randomMemory") == null ? null : Integer.valueOf(randomMemory));
        }
        return (T) hiveTableInfo;
    }

    @Override
    public S initDbConnect(D databaseConf, T tableConf) {
        Properties prop = new Properties();
        // 判断是否需要认证
        if (StringUtils.equalsIgnoreCase(databaseConf.getAuthType(), "cer") || StringUtils.isNotBlank(databaseConf.getCertificateId())) {
            // 获取证书
            String certificateId = databaseConf.getCertificateId();
            try {
                DbHelper annotation = this.getClass().getAnnotation(DbHelper.class);
                DatabaseConfService databaseConfService = getDatabaseConfService();
                KerberosUniformAuth kerberosUniformLogin = null;

                //certificateBase64Str存在
                if(StringUtils.isNotBlank(databaseConf.getCertificateBase64Str())) {
                    byte[] content = Base64.decode(databaseConf.getCertificateBase64Str());
                    kerberosUniformLogin = KerberosUniformAuthFactory.getKerberosUniformLoginByZip(annotation.dbType(), annotation.version(), annotation.cloudVendors()
                            , content, ".zip", databaseConf.getUserName(), null, databaseConf.getParentDir(), null);
                } else if (databaseConfService == null) {
                    // 本地获取
                    kerberosUniformLogin = KerberosUniformAuthFactory.getKerberosUniformLoginByLocal(annotation.dbType()
                            , annotation.version()
                            , annotation.cloudVendors()
                            , databaseConf.getUserName()
                            , databaseConf.getKerberosPrincipal()
                            , databaseConf.getKrb5Path()
                            , databaseConf.getUserKeytabPath()
                            , databaseConf.getParentDir()
                            , new KerberosUniformAuth.RefreshKerberos() {
                                @Override
                                public void refresh(KerberosUniformAuth kerberosUniformLogin) throws Exception {
                                    UserGroupInformation userGroupInformation = UserGroupInformation.getLoginUser();
                                    if (userGroupInformation != null) {
                                        userGroupInformation.checkTGTAndReloginFromKeytab();
                                        log.info("进行 kerberos TGT 检查并重新认证");
                                    }
                                }
                            });
                } else {
                    // 从数据库获取
                    CertificateConf certificateConf = databaseConfService.queryCertificateConfById(certificateId);
                    if (certificateConf == null) {
                        throw new BusinessException(ExceptionCode.NOT_FOUND_CERTIFICATE, certificateId);
                    }
                    kerberosUniformLogin = KerberosUniformAuthFactory.getKerberosUniformLoginByZip(annotation.dbType(), annotation.version(), annotation.cloudVendors()
                            , certificateConf.getContent(), certificateConf.getFileName(), certificateConf.getUserName(), databaseConf.getKerberosPrincipal(), databaseConf.getParentDir()
                            , new KerberosUniformAuth.RefreshKerberos() {
                                @Override
                                public void refresh(KerberosUniformAuth kerberosUniformLogin) throws Exception {
                                    UserGroupInformation userGroupInformation = UserGroupInformation.getLoginUser();
                                    if (userGroupInformation != null) {
                                        userGroupInformation.checkTGTAndReloginFromKeytab();
                                        log.info("进行 kerberos TGT 检查并重新认证");
                                    }
                                }
                            });
                }
                // hive 证书登录
                Configuration conf = new Configuration();
                loginTargetMethod(databaseConf, kerberosUniformLogin, conf);
                createAuthUrl(databaseConf, prop, kerberosUniformLogin);
                log.info("hive addr: [{}] url: [{}]", databaseConf.getServiceUrl(),prop.getProperty("url"));
            } catch (Exception e) {
                throw new BusinessException(e, ExceptionCode.GET_CERTIFICATE_EXCEPTION, certificateId);
            }
        } else {
            if (HiveDatabaseInfo.ZK_LINK_TYPE.equals(databaseConf.getLinkType())) {
                // 若是 zk 连接方式
                if(StringUtils.isNotBlank(databaseConf.getDatabaseName())) {
                    prop.put("url", String.format(HiveDatabaseInfo.HIVE_ZK_URL_TEMPLATE,
                            databaseConf.getServiceUrl(),
                            databaseConf.getDatabaseName(),
                            databaseConf.getZooKeeperNamespace()));
                }else {
                    prop.put("url", String.format(HiveDatabaseInfo.HIVE_ZK_URL_TEMPLATE2,
                            databaseConf.getServiceUrl(),
                            databaseConf.getZooKeeperNamespace()));
                }
            } else {
                if(StringUtils.isNotBlank(databaseConf.getDatabaseName())) {
                    // 若是 hive server 连接方式
                    prop.put("url", String.format(HiveDatabaseInfo.HIVE_SERVER_URL_TEMPLATE,
                            databaseConf.getServiceUrl(),
                            databaseConf.getDatabaseName()));
                }else {
                    prop.put("url", String.format(HiveDatabaseInfo.HIVE_SERVER_URL_TEMPLATE2,
                            databaseConf.getServiceUrl()));
                }
            }

        }
        if (StringUtils.isNotBlank(databaseConf.getUserName())) {
            prop.put("username", databaseConf.getUserName());
        }
        if (StringUtils.isNotBlank(databaseConf.getPassword())) {
            prop.put("password", databaseConf.getPassword());
        }
        prop.put("driverClassName", HiveDatabaseInfo.DRIVER_CLASS_NAME);
        prop.put("initialSize", databaseConf.getInitialSize());
        prop.put("minIdle", databaseConf.getMinIdle());
        prop.put("maxActive", databaseConf.getMaxActive());
        prop.put("poolPreparedStatements", HiveDatabaseInfo.POOL_PREPARED_STATEMENTS);
        DataSource dataSource = null;
        try {
            dataSource = BasicDataSourceFactory.createDataSource(prop);
        } catch (Exception e) {
            throw new BusinessException(e, ExceptionCode.CREATE_DB_CONNECT_EXCEPTION);
        }

        HiveClient hiveClient = new HiveClient(dataSource);

        //元数据查询配置
        String metaStoreUrl = databaseConf.getMetaStoreUrl();
        if(StringUtils.isNotBlank(metaStoreUrl)) {
            boolean metaStoreExecuteSetugi = databaseConf.isMetaStoreExecuteSetugi();
            HiveConf hiveConf = new HiveConf();
            hiveConf.set("hive.metastore.uris", "thrift://" + metaStoreUrl);
            hiveConf.set("hive.metastore.execute.setugi", metaStoreExecuteSetugi + "");
            if (StringUtils.equalsIgnoreCase(databaseConf.getAuthType(), "cer")
                    || StringUtils.isNotBlank(databaseConf.getCertificateId())) {
                hiveConf.set("hadoop.security.authentication", "kerberos");
                hiveConf.set("hive.metastore.sasl.enabled", "true");
                hiveConf.set("hive.metastore.kerberos.principal", databaseConf.getKerberosPrincipal());
            }
            int metaStoreMaxConnectionCount = databaseConf.getMetaStoreMaxConnectionCount();
            ExecutorService executorService = Executors.newFixedThreadPool(metaStoreMaxConnectionCount,
                    new ThreadNamedFactory("metastore"));
            hiveClient.setExecutorService(executorService);
            hiveClient.setHiveConf(hiveConf);
        }


        return (S) hiveClient;
    }

    /**
     * 登录方法
     *
     * @param databaseConf
     * @param kerberosUniformLogin
     * @param conf
     * @throws Exception
     */
    protected void loginTargetMethod(D databaseConf, KerberosUniformAuth kerberosUniformLogin, Configuration conf) throws Exception {
        kerberosUniformLogin.login(null, new KerberosLoginCallBack() {
            @Override
            public void loginExtend(KerberosUniformAuth kerberosUniformLogin) throws Exception {
                conf.set("hadoop.security.authentication", "Kerberos");
                UserGroupInformation.setConfiguration(conf);
                try {
                    UserGroupInformation.loginUserFromKeytab(kerberosUniformLogin.getUserName(), kerberosUniformLogin.getUserKeytabPath());
                } catch (IOException e) {
                    log.error("login failed with " + kerberosUniformLogin.getUserName() + " and " + kerberosUniformLogin.getUserKeytabPath() + ".");
                    log.error("perhaps cause 1 is " + logIN_FAILED_CAUSE_PASSWORD_WRONG + ".");
                    log.error("perhaps cause 2 is " + logIN_FAILED_CAUSE_TIME_WRONG + ".");
                    log.error("perhaps cause 3 is " + logIN_FAILED_CAUSE_AES256_WRONG + ".");
                    log.error("perhaps cause 4 is " + logIN_FAILED_CAUSE_PRINCIPAL_WRONG + ".");
                    log.error("perhaps cause 5 is " + logIN_FAILED_CAUSE_TIME_OUT + ".");
                    throw e;
                }
                log.info("Login kerberos success!!!!!!!!!!!!!!");
            }
        });
    }

    /**
     * Get user realm process
     */
    public String getUserRealm() {
        String userRealm;
        String serverRealm = System.getProperty("SERVER_REALM");
        if (StringUtils.isNotBlank(serverRealm)) {
            userRealm = "hadoop." + serverRealm.toLowerCase();
        } else {
            try {
                serverRealm = KerberosUtil.getDefaultRealm();
            } catch (Exception e) {
                serverRealm = DEFAULT_REALM;
                log.warn("Get default realm failed, use default value : " + DEFAULT_REALM);
            }
            if (StringUtils.isNotBlank(serverRealm)) {
                userRealm = "hadoop." + serverRealm.toLowerCase();
            } else {
                userRealm = "hadoop";
            }
        }
        return userRealm;
    }

    /**
     * 构建认证url
     *
     * @param databaseConf
     * @param prop
     */
    protected void createAuthUrl(D databaseConf, Properties prop, KerberosUniformAuth kerberosUniformLogin) {
        if (HiveDatabaseInfo.HIVE_SERVER_LINK_TYPE.equals(databaseConf.getLinkType())) {

            if(StringUtils.isNotBlank(databaseConf.getDatabaseName())) {
                prop.put("url", String.format(HiveDatabaseInfo.HIVE_AUTH_SERVER_URL_TEMPLATE,
                        databaseConf.getServiceUrl(),
                        databaseConf.getDatabaseName(),
                        databaseConf.getKerberosPrincipal()).replaceAll("\\\\", "/"));
            }else {
                prop.put("url", String.format(HiveDatabaseInfo.HIVE_AUTH_SERVER_URL_TEMPLATE2,
                        databaseConf.getServiceUrl(),
                        databaseConf.getKerberosPrincipal()).replaceAll("\\\\", "/"));
            }

        } else {
            if(StringUtils.isNotBlank(databaseConf.getDatabaseName())) {
                prop.put("url", String.format(HiveDatabaseInfo.AUTH_ZK_URL_TEMPLATE,
                        databaseConf.getServiceUrl(),
                        databaseConf.getDatabaseName(),
                        databaseConf.getZooKeeperNamespace(),
                        databaseConf.getKerberosPrincipal()
                ).replaceAll("\\\\", "/"));
            }else {
                prop.put("url", String.format(HiveDatabaseInfo.AUTH_ZK_URL_TEMPLATE2,
                        databaseConf.getServiceUrl(),
                        databaseConf.getZooKeeperNamespace(),
                        databaseConf.getKerberosPrincipal()
                ).replaceAll("\\\\", "/"));
            }
        }
    }

    @Override
    public void destroyDbConnect(String cacheKey, S connect) throws Exception {
    }
}
