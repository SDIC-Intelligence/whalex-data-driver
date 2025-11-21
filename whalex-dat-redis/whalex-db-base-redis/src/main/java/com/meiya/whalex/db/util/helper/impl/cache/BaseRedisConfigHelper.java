package com.meiya.whalex.db.util.helper.impl.cache;

import cn.hutool.core.codec.Base64;
import com.meiya.whalex.annotation.DbHelper;
import com.meiya.whalex.business.entity.CertificateConf;
import com.meiya.whalex.business.entity.DatabaseConf;
import com.meiya.whalex.business.entity.TableConf;
import com.meiya.whalex.business.service.DatabaseConfService;
import com.meiya.whalex.db.entity.AbstractCursorCache;
import com.meiya.whalex.db.entity.cache.RedisClient;
import com.meiya.whalex.db.entity.cache.RedisConfig;
import com.meiya.whalex.db.entity.cache.RedisDatabaseInfo;
import com.meiya.whalex.db.entity.cache.RedisTableInfo;
import com.meiya.whalex.db.kerberos.*;
import com.meiya.whalex.db.kerberos.exception.GetKerberosException;
import com.meiya.whalex.db.util.helper.AbstractDbModuleConfigHelper;
import com.meiya.whalex.exception.BusinessException;
import com.meiya.whalex.exception.ExceptionCode;
import com.meiya.whalex.util.JsonUtil;
import com.meiya.whalex.util.encrypt.AESUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import redis.clients.jedis.exceptions.JedisAccessControlException;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author chenjp
 * @date 2020/9/8
 */
@Slf4j
public class BaseRedisConfigHelper extends AbstractDbModuleConfigHelper<RedisClient, RedisDatabaseInfo, RedisTableInfo, AbstractCursorCache> {

    @Override
    public boolean checkDataSourceStatus(RedisClient connect) throws Exception {
        try {
            connect.get("dat__redis_ck_status");
        } catch (JedisAccessControlException ignored) {
        }
        return true;
    }

    @SuppressWarnings("unchecked")
    @Override
    public RedisDatabaseInfo initDbModuleConfig(DatabaseConf conf) {
        try {
            String connSetting = conf.getConnSetting();
            if (StringUtils.isBlank(connSetting)) {
                log.warn("获取Redis组件数据库部署定义为空，bigResourceId：{}", conf.getBigdataResourceId());
                throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "数据库配置必填参数缺失，请校验参数配置是否正确!");
            }
            Map<String, Object> dbMap = JsonUtil.jsonStrToObject(connSetting, Map.class);
            List<String> serverUrls = new ArrayList<>();
            String serviceUrl = (String) dbMap.get("serviceUrl");
            if (StringUtils.isBlank(serviceUrl)) {
                log.warn("获取Redis组件数据库部署定义地址或端口为空，bigResourceId：{}", conf.getBigdataResourceId());
                throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "数据库配置必填参数缺失，请校验参数配置是否正确!");
            }
            List<String> serverSet = convertService(serviceUrl);
            if (CollectionUtils.isNotEmpty(serverSet)) {
                serverUrls.addAll(serverSet);
            }
            return getDatabaseInfo(dbMap, serverUrls);
        } catch (Exception e) {
            if (e instanceof BusinessException) {
                throw e;
            }
            log.error("Redis 数据库配置加载失败! bigResourceId：{}", conf.getBigdataResourceId(), e);
            throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "数据库配置解析异常，请校验参数配置是否正确!");
        }
    }

    @Override
    public RedisTableInfo initTableConfig(TableConf conf) {
        RedisTableInfo redisTableInfo = new RedisTableInfo();
        redisTableInfo.setTableName(conf.getTableName());
        return redisTableInfo;
    }

    @Override
    public RedisClient initDbConnect(RedisDatabaseInfo databaseConf, RedisTableInfo tableConf) {
        String authType = databaseConf.getAuthType();
        String certificateId = databaseConf.getCertificateId();
        KerberosUniformAuth kerberosUniformLogin;
        // 获取证书
        if (StringUtils.equalsIgnoreCase(authType, RedisDatabaseInfo.KERBEROS_AUTH) || StringUtils.isNotBlank(certificateId)) {
            try {
                DbHelper annotation = this.getClass().getAnnotation(DbHelper.class);
                DatabaseConfService databaseConfService = getDatabaseConfService();

                //certificateBase64Str存在
                if(StringUtils.isNotBlank(databaseConf.getCertificateBase64Str())) {
                    byte[] content = Base64.decode(databaseConf.getCertificateBase64Str());
                    kerberosUniformLogin = KerberosUniformAuthFactory.getKerberosUniformLoginByZip(annotation.dbType(), annotation.version(), annotation.cloudVendors()
                            , content, ".zip", databaseConf.getUsername(), null, databaseConf.getParentDir()
                            , null);
                } else if (databaseConfService == null) {
                    // 本地获取
                    kerberosUniformLogin = KerberosUniformAuthFactory.getKerberosUniformLoginByLocal(annotation.dbType(), annotation.version(), annotation.cloudVendors()
                            , databaseConf.getUsername(), null, databaseConf.getKrb5Path(), databaseConf.getUserKeytabPath()
                            , databaseConf.getParentDir()
                            , null);
                } else {
                    // 从数据库获取
                    CertificateConf certificateConf = databaseConfService.queryCertificateConfById(certificateId);
                    if (certificateConf == null) {
                        throw new BusinessException(ExceptionCode.NOT_FOUND_CERTIFICATE, certificateId);
                    }
                    kerberosUniformLogin = KerberosUniformAuthFactory.getKerberosUniformLoginByZip(annotation.dbType(), annotation.version(), annotation.cloudVendors()
                            , certificateConf.getContent(), certificateConf.getFileName(), certificateConf.getUserName(), null, databaseConf.getParentDir()
                            , null);
                }
            } catch (GetKerberosException e) {
                throw new BusinessException(e, ExceptionCode.GET_CERTIFICATE_EXCEPTION, certificateId);
            }
            // 证书登录
            try {
                log.info("kerberos redis 进行证书登录!");
                kerberosUniformLogin.login(KerberosJaasConfigurationUtil.Module.CLIENT
                        , new KerberosLoginCallBack() {
                            @Override
                            public void loginExtend(KerberosUniformAuth kerberosUniformLogin) throws Exception {
                                System.setProperty("redis.authentication.jaas", "true");
                                System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
                            }
                        });
            } catch (Exception e) {
                throw new BusinessException(e, ExceptionCode.CERTIFICATE_LOGIN_EXCEPTION, certificateId);
            }
        }

        List<String> serverUrl = databaseConf.getServerUrl();
        RedisClient redisClient = null;
        if (CollectionUtils.isNotEmpty(serverUrl)) {
            if (serverUrl.size() == 1) {
                redisClient = new RedisClient(RedisConfig.getJedis(serverUrl.get(0), databaseConf.getUsername(), databaseConf.getPassword(), databaseConf.getDbIndex()), RedisClient.TYPE_0);
            } else {
                AtomicBoolean isShardJedisModel = new AtomicBoolean(false);
                serverUrl.forEach(sUrl -> {
                    isShardJedisModel.set(sUrl.contains("_"));
                });
                redisClient = isShardJedisModel.get() ?
                        new RedisClient(RedisConfig.getShardedJedis(serverUrl, databaseConf.getUsername(), databaseConf.getPassword(), databaseConf.getDbIndex()), RedisClient.TYPE_1) :
                        new RedisClient(RedisConfig.getJedisClient(serverUrl, databaseConf.getUsername(), databaseConf.getPassword(), databaseConf.getDbIndex()), RedisClient.TYPE_2);
            }
        }

        if(databaseConf.isUseLock()) {
            redisClient.setRedissonClient(RedisConfig.getRedissonClient(serverUrl, databaseConf.getPassword(), databaseConf.getDbIndex()));
        }

        return redisClient;
    }

    @Override
    public void destroyDbConnect(String cacheKey, RedisClient connect) throws Exception {
        if (null != connect.getJedisCluster()) {
            connect.getJedisCluster().close();
        }
    }

    /**
     * 获取 RedisDatabaseInfo 信息
     */
    private RedisDatabaseInfo getDatabaseInfo(Map<String, Object> dbMap, List<String> serverList) {


        String authType = (String) dbMap.get("authType");
        String certificateId = (String) dbMap.get("certficateId");
        if (StringUtils.isBlank(certificateId)) {
            certificateId = (String) dbMap.get("certificateId");
        }

        // krb5 地址
        String krb5Path = (String) dbMap.get("krb5Path");
        // userKeytab 地址
        String userKeytabPath = (String) dbMap.get("userKeytabPath");
        // 用户名，当为 kerberos 认证时，为 kerberos 用户名，否正为 xpack 用户米
        String username = (String) dbMap.get("username");
        if (StringUtils.isBlank(username)) {
            username = (String) dbMap.get("userName");
        }
        // 密码
        String password = (String) dbMap.get("password");
        // AES 解密
        if (StringUtils.isNotBlank(password)) {
            try {
                password = AESUtil.decrypt(password);
            } catch (Exception e) {
//                    log.error("elasticsearch init db config password [{}] decrypt fail!!!", password, e);
            }
        }

        //证书，base64字符串
        String certificateBase64Str = (String) dbMap.get("certificateBase64Str");
        RedisDatabaseInfo redisDatabaseInfo = new RedisDatabaseInfo();
        redisDatabaseInfo.setCertificateBase64Str(certificateBase64Str);
        redisDatabaseInfo.setKrb5Path(krb5Path);
        redisDatabaseInfo.setUserKeytabPath(userKeytabPath);
        redisDatabaseInfo.setServiceType((String) dbMap.get("serviceType"));
        redisDatabaseInfo.setCertificateId(certificateId);
        redisDatabaseInfo.setUsername(username);
        redisDatabaseInfo.setParentDir((String) dbMap.get("parentDir"));
        redisDatabaseInfo.setPassword(password);
        redisDatabaseInfo.setServerUrl(serverList);
        redisDatabaseInfo.setAuthType(authType);
        // 判断是否开启支持分布式锁
        Object useLock = dbMap.get("useLock");
        if (useLock == null) {
            redisDatabaseInfo.setUseLock(false);
        } else {
            try {
                redisDatabaseInfo.setUseLock(Boolean.parseBoolean(useLock.toString()));
            } catch (Exception e) {
                throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "useLock 参数必须为布尔类型!");
            }
        }

        Object dbIndexObj = dbMap.get("dbIndex");
        if (dbIndexObj != null) {
            if (!StringUtils.isNumeric(String.valueOf(dbIndexObj))) {
                throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "dbIndex 参数必须为数值型!");
            }
            redisDatabaseInfo.setDbIndex(Integer.valueOf(String.valueOf(dbIndexObj)));
        }
        // 设置认证类型
        if (StringUtils.isBlank(authType)) {
            if (StringUtils.isNotBlank(certificateId)
                    || (StringUtils.isNotBlank(krb5Path) && StringUtils.isNotBlank(userKeytabPath))
                    || StringUtils.isNotBlank(certificateBase64Str)) {
                redisDatabaseInfo.setAuthType(RedisDatabaseInfo.KERBEROS_AUTH);
            } else if (StringUtils.isNotBlank(password)) {
                redisDatabaseInfo.setAuthType(RedisDatabaseInfo.PASSWORD_AUTH);
            }
        }

        return redisDatabaseInfo;
    }

    /**
     * 转换serviceUrl
     */
    private List<String> convertService(String serviceUrl) {
        List<String> serverSet = new ArrayList<>();
        String isRange = StringUtils.substringBetween(serviceUrl, "(", ")");
        if (StringUtils.isNotBlank(isRange) || StringUtils.contains(serviceUrl, ",")) {
            String[] servers = StringUtils.split(serviceUrl, ",");
            serverSet.addAll(Arrays.asList(servers));
        } else {
            serverSet.add(serviceUrl);
        }
        return serverSet;
    }

}
