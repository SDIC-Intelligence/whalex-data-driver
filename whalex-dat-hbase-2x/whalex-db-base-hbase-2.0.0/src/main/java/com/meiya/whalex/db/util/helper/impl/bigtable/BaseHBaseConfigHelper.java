package com.meiya.whalex.db.util.helper.impl.bigtable;

import cn.hutool.core.codec.Base64;
import com.meiya.whalex.annotation.DbHelper;
import com.meiya.whalex.business.entity.CertificateConf;
import com.meiya.whalex.business.entity.DatabaseConf;
import com.meiya.whalex.business.entity.TableConf;
import com.meiya.whalex.business.service.DatabaseConfService;
import com.meiya.whalex.db.constant.DatatypePeriodConstants;
import com.meiya.whalex.db.custom.bigtable.HBaseTemplate;
import com.meiya.whalex.db.entity.bigtable.HBaseCursorCache;
import com.meiya.whalex.db.entity.bigtable.HBaseDatabaseInfo;
import com.meiya.whalex.db.entity.bigtable.HBaseTableInfo;
import com.meiya.whalex.db.kerberos.*;
import com.meiya.whalex.db.kerberos.exception.GetKerberosException;
import com.meiya.whalex.db.util.helper.AbstractDbModuleConfigHelper;
import com.meiya.whalex.exception.BusinessException;
import com.meiya.whalex.exception.ExceptionCode;
import com.meiya.whalex.interior.db.constant.DbResourceEnum;
import com.meiya.whalex.util.JsonUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Executors;

/**
 * HBase 配置、连接对象缓存工具
 *
 * @author 黄河森
 * @date 2019/12/16
 * @project whale-cloud-platformX
 */
@Slf4j
public class BaseHBaseConfigHelper<S extends HBaseTemplate
        , D extends HBaseDatabaseInfo
        , T extends HBaseTableInfo
        , C extends HBaseCursorCache> extends AbstractDbModuleConfigHelper<S, D, T, C> {

    private static final String logIN_FAILED_CAUSE_PASSWORD_WRONG =
            "(wrong password) keytab file and user not match, you can kinit -k -t keytab user in client server to check";

    private static final String logIN_FAILED_CAUSE_TIME_WRONG =
            "(clock skew) time of local server and remote server not match, please check ntp to remote server";

    private static final String logIN_FAILED_CAUSE_AES256_WRONG =
            "(aes256 not support) aes256 not support by default jdk/jre, need copy local_policy.jar and US_export_policy.jar from remote server in path /opt/huawei/Bigdata/jdk/jre/lib/security";

    private static final String logIN_FAILED_CAUSE_PRINCIPAL_WRONG =
            "(no rule) principal format not support by default, need add property hadoop.security.auth_to_local(in core-site.xml) value RULE:[1:$1] RULE:[2:$1]";

    private static final String logIN_FAILED_CAUSE_TIME_OUT =
            "(time out) can not connect to kdc server or there is fire wall in the network";

    /**
     * 默认 HBase schema
     */
    public static final String DEFAULT_BC_FAMILY_SCHEME = "[\n" +
            "{\"NAME\":\"B\",\"BLOOMFILTER\":\"ROW\",\"VERSIONS\":\"1\",\"COMPRESSION\":\"SNAPPY\"},\n" +
            "{\"NAME\":\"C\",\"BLOOMFILTER\":\"ROW\",\"VERSIONS\":\"1\",\"COMPRESSION\":\"SNAPPY\",\"BLOCKCACHE\":\"false\"},\n" +
            "{\"NUMREGIONS\":\"10\",\"SPLITALGO\":\"HexStringSplit\"}\n" +
            "]";

    @Override
    public boolean checkDataSourceStatus(S connect) throws Exception {
        Admin admin = connect.getHTablePool().getAdmin();
        admin.listTableNames();
        return true;
    }

    @Override
    public D initDbModuleConfig(DatabaseConf conf) {
        HBaseDatabaseInfo hBaseDatabaseInfo = new HBaseDatabaseInfo();
        String connSetting = conf.getConnSetting();
        Map<String, String> dbJsonMap = JsonUtil.jsonStrToObject(connSetting, Map.class);
        // 服务地址
        String serviceUrl = dbJsonMap.get("serviceUrl");
        if (StringUtils.isBlank(serviceUrl)) {
            log.error("HBase 数据库配置加载失败, 服务地址为空! bigResourceId: [{}]", conf.getBigdataResourceId());
            throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "数据库配置必填参数缺失，请校验参数配置是否正确!");
        }
        // 证书信息
        String certificateId = dbJsonMap.get("certficateId");
        if (StringUtils.isBlank(certificateId)) {
            certificateId = dbJsonMap.get("certificateId");
        }
        // 认证类型
        String authType = dbJsonMap.get("authType");
        // krb5 地址
        String krb5Path = dbJsonMap.get("krb5Path");
        // userKeytab 地址
        String userKeytabPath = dbJsonMap.get("userKeytabPath");
        // kerberos 认证主题名称
        String kerberosPrincipal = dbJsonMap.get("kerberosPrincipal");
        // zk 目录配置
        String zkNodeParent = dbJsonMap.get("zkNodeParent");
        // 安全SASL连接保护
        String rpcProtection = dbJsonMap.get("rpcProtection");
        // zk Principal
        String zookeeperPrincipal = dbJsonMap.get("zookeeperPrincipal");
        hBaseDatabaseInfo.setZookeeperAddr(serviceUrl);
        hBaseDatabaseInfo.setKrb5Path(krb5Path);
        hBaseDatabaseInfo.setUserKeytabPath(userKeytabPath);
        hBaseDatabaseInfo.setKerberosPrincipal(kerberosPrincipal);
        hBaseDatabaseInfo.setZkNodeParent(zkNodeParent);
        if (StringUtils.isNotBlank(zookeeperPrincipal)) {
            hBaseDatabaseInfo.setZookeeperPrincipal(zookeeperPrincipal);
        }
        hBaseDatabaseInfo.setUserName(dbJsonMap.get("userName"));
        if (StringUtils.isBlank(hBaseDatabaseInfo.getUserName())) {
            hBaseDatabaseInfo.setUserName(dbJsonMap.get("username"));
        }
        hBaseDatabaseInfo.setNamespace(dbJsonMap.get("namespace"));
        hBaseDatabaseInfo.setRpcProtection(rpcProtection);
        hBaseDatabaseInfo.setParentDir(dbJsonMap.get("parentDir"));
        if (StringUtils.isNotBlank(certificateId)) {
            hBaseDatabaseInfo.setCertificateId(certificateId);
        }
        //证书，base64字符串
        String certificateBase64Str = dbJsonMap.get("certificateBase64Str");
        if (StringUtils.isNotBlank(certificateBase64Str)) {
            hBaseDatabaseInfo.setCertificateBase64Str(certificateBase64Str);
        }
        //设置认证类型
        if (StringUtils.isNotBlank(authType)) {
            hBaseDatabaseInfo.setAuthType(authType);
        } else {
            if (StringUtils.isNotBlank(certificateId) || StringUtils.isNotBlank(certificateBase64Str)) {
                hBaseDatabaseInfo.setAuthType("cer");
            } else {
                hBaseDatabaseInfo.setAuthType("normal");
            }
        }

        // 获取是否存在指定资源池类型
        String ipcPollType = dbJsonMap.get("ipcPollType");
        if (StringUtils.isNotBlank(ipcPollType)) {
            if (StringUtils.equalsAny(ipcPollType, HBaseDatabaseInfo.REUSABLE_IPC_POOL_TYPE, HBaseDatabaseInfo.ROUND_ROBIN_IPC_POOL_TYPE, HBaseDatabaseInfo.THREAD_LOCAL_IPC_POOL_TYPE)) {
                hBaseDatabaseInfo.setIpcPollType(ipcPollType);
            } else {
                log.warn("HBase 连接配置中接收的 [ipcPollType] 参数值 [{}] 不合法, 仅支持 {}、{}、{}!", ipcPollType, HBaseDatabaseInfo.REUSABLE_IPC_POOL_TYPE, HBaseDatabaseInfo.ROUND_ROBIN_IPC_POOL_TYPE, HBaseDatabaseInfo.THREAD_LOCAL_IPC_POOL_TYPE);
            }
        }

        return (D) hBaseDatabaseInfo;
    }

    @Override
    public T initTableConfig(TableConf conf) {
        HBaseTableInfo hBaseTableInfo = new HBaseTableInfo();
        String tableName = conf.getTableName();
        hBaseTableInfo.setTableName(tableName);
        String tableJson = conf.getTableJson();
        if (StringUtils.isBlank(tableName)) {
            log.error("HBase 数据库表配置加载失败, 表配置信息 or 表名为空! dbId: [{}]", conf.getId());
            throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "数据表配置必填参数缺失，请校验参数配置是否正确!");
        }
        if (StringUtils.isNotBlank(tableJson)) {
            Map<String, String> tableConfMap = JsonUtil.jsonStrToObject(tableJson, Map.class);
            // {"schema":"family_BC","periodType":"m","storePeriodValue":"12","prePeriod":"3","backwardPeriod":"3","numregions":"30"}
            // 存储周期类型
            String periodType = tableConfMap.get("periodType");
            // 表结构
            String schemaName = tableConfMap.get("schema");
            // 存储周期
            String storePeriodValue = tableConfMap.get("storePeriodValue");
            // 过往分片周期值
            String prePeriod = tableConfMap.get("prePeriod");
            // 未来分片数量
            String backwardPeriod = tableConfMap.get("backwardPeriod");
            // 分片数量
            String regionCount = tableConfMap.get("numregions");
            hBaseTableInfo.setPeriodType(periodType);
            if (StringUtils.isNumeric(backwardPeriod)) {
                hBaseTableInfo.setFuturePeriodValue(Integer.valueOf(backwardPeriod));
            }
            if (StringUtils.isNumeric(prePeriod)) {
                hBaseTableInfo.setPrePeriodValue(Integer.valueOf(prePeriod));
            }
            if (StringUtils.isNumeric(storePeriodValue)) {
                hBaseTableInfo.setPeriodValue(Integer.valueOf(storePeriodValue));
            }
            if (StringUtils.isNumeric(regionCount)) {
                hBaseTableInfo.setRegionCount(Integer.valueOf(regionCount));
            }
            if (StringUtils.isNotBlank(schemaName)) {
                if (getTableConfService() == null) {
                    hBaseTableInfo.setSchema(schemaName);
                } else {
                    String schema = getTableConfService().querySchemaTemPlate(schemaName, DbResourceEnum.hbase.getVal());
                    if (StringUtils.isBlank(schema)) {
                        log.warn("HBase table [{}] init not found schemaName: [{}]!", conf.getId(), schemaName);
                    } else {
                        hBaseTableInfo.setSchema(schema);
                    }
                }
            }
        } else {
            hBaseTableInfo.setPeriodType(DatatypePeriodConstants.PERIOD_TYPE_ONLY_ONE);
        }

        return (T) hBaseTableInfo;
    }

    @Override
    public S initDbConnect(D databaseConf, T tableConf) {
        String authType = databaseConf.getAuthType();
        String certificateId = databaseConf.getCertificateId();
        String ipcPollType = databaseConf.getIpcPollType();
        KerberosUniformAuth kerberosUniformLogin = null;
        // 获取证书
        if (StringUtils.equalsIgnoreCase(authType, "cer") || StringUtils.isNotBlank(certificateId)) {
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
                    kerberosUniformLogin = KerberosUniformAuthFactory.getKerberosUniformLoginByLocal(annotation.dbType(), annotation.version(), annotation.cloudVendors()
                            , databaseConf.getUserName(), databaseConf.getKerberosPrincipal(), databaseConf.getKrb5Path(), databaseConf.getUserKeytabPath()
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
            } catch (GetKerberosException e) {
                throw new BusinessException(e, ExceptionCode.GET_CERTIFICATE_EXCEPTION, certificateId);
            }
        } else if (StringUtils.endsWithIgnoreCase(authType, "acl") && StringUtils.isNotBlank(databaseConf.getUserName())){
            // 连接开启 acl 控制的HBase
            System.setProperty("HADOOP_USER_NAME", databaseConf.getUserName());
        }
        // 创建连接
        String userName = null;
        String zookeeperAddr = databaseConf.getZookeeperAddr();
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", zookeeperAddr);
        // 配置zk节点
        if (StringUtils.isNoneBlank(databaseConf.getZkNodeParent())) {
            conf.set("zookeeper.znode.parent", databaseConf.getZkNodeParent());
        }
        // 设置连接参数
        Integer timeOut = this.threadConfig.getTimeOut();
        conf.setInt("hbase.client.meta.operation.timeout", timeOut * 1000);
        conf.setInt("hbase.client.operation.timeout", timeOut * 1000);
        conf.setInt("hbase.client.replicaCallTimeout.scan", timeOut * 1000);
        conf.setInt("hbase.client.meta.replica.scan.timeout", timeOut * 1000);
        conf.setInt("hbase.rpc.timeout", timeOut * 1000);
        // 设置连接池大小
        conf.setInt("hbase.client.ipc.pool.size", this.threadConfig.getCorePoolSize());
        if (StringUtils.isNotBlank(ipcPollType)) {
            conf.set("hbase.client.ipc.pool.type", ipcPollType);
            log.info("HBase 配置 hbase.client.ipc.pool.type 为 {}", ipcPollType);
        }

        // 优化参数
        // 1.hbase.client.write.buffer
        // 2.hbase.client.pause
        // 3.hbase.client.pause.cqtbe
        // 4.hbase.client.retries.number 默认15次，间隔 10s
        // 5.hbase.client.perserver.requests.threshold 所有客户端线程中对一台服务器的最大并发挂起数，超出请求将立即排出服务器繁忙的错误，防止用户线程被一个慢的服务器占用和阻塞，设置与线程数量相关的适当值
        // 6.hbase.client.keyvalue.maxsize 指定KeyValue实例的组合允许的最大大小
        // 7.hbase.client.scanner.timeout.period 客户端扫描器租赁期（毫秒）
        // 8.hbase.ipc.client.fallback-to-simple-auth-allowed 是否在配置安全连接情况下接受非安全连接，默认 false
        // 9.hbase.client.scanner.max.result.size 扫描下一个记录返回的最大字节数

        if (kerberosUniformLogin != null) {
            try {
                //暂时还需要 hbase-site.xml 配置文件，配置hadoop.security.authentication 为kerberos 要需要优化
                conf.set("hadoop.security.authentication", "kerberos");
                conf.set("hbase.security.authentication", "kerberos");
                conf.set("hadoop.security.authorization", "true");
                if (StringUtils.isNotBlank(databaseConf.getKerberosPrincipal())) {
                    conf.set("hbase.kerberos.principal", databaseConf.getKerberosPrincipal());
                    conf.set("hadoop.http.kerberos.internal.spnego.principal", databaseConf.getKerberosPrincipal());
                    conf.set("hadoop.http.kerberos.internal.spnego.principal", databaseConf.getKerberosPrincipal());
                    conf.set("hbase.master.kerberos.principal", databaseConf.getKerberosPrincipal());
                    conf.set("hbase.regionserver.kerberos.principal", databaseConf.getKerberosPrincipal());
                    conf.set("hadoop.http.kerberos.internal.spnego.principal", databaseConf.getKerberosPrincipal());
                }
                if (StringUtils.isNoneBlank(databaseConf.getRpcProtection())) {
                    String rpcProtection = databaseConf.getRpcProtection();
                    if (!StringUtils.equals(rpcProtection, "authentication")
                            && !StringUtils.equals(rpcProtection, "integrity")
                            && !StringUtils.equals(rpcProtection, "privacy")) {
                        log.error("HBase 参数配置异常, rpcProtection: [{}] 无法识别", rpcProtection);
                        throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_EXCEPTION);
                    }
                    conf.set("hbase.rpc.protection", databaseConf.getRpcProtection());
                }

                kerberosUniformLogin.login(KerberosJaasConfigurationUtil.Module.CLIENT, new KerberosLoginCallBack() {
                    @Override
                    public void loginExtend(KerberosUniformAuth kerberosUniformLogin) throws Exception {
                        KerberosEnvConfigurationUtil.setZookeeperServerPrincipal(databaseConf.getZookeeperPrincipal());
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
                userName = kerberosUniformLogin.getUserName();
            } catch (Exception e) {
                throw new BusinessException(e, ExceptionCode.CERTIFICATE_LOGIN_EXCEPTION, certificateId);
            }
        }

        Connection connection = null;
        try {
            connection = ConnectionFactory.createConnection(conf, Executors.newFixedThreadPool(threadConfig.getMaximumPoolSize()));
        } catch (IOException e) {
            throw new BusinessException(e, ExceptionCode.CREATE_DB_CONNECT_EXCEPTION);
        }
        return (S) new HBaseTemplate(connection, StringUtils.isBlank(databaseConf.getNamespace()) ? userName : databaseConf.getNamespace());
    }

    @Override
    public void destroyDbConnect(String cacheKey, HBaseTemplate connect) throws Exception {
        connect.cleanup();
    }
}
