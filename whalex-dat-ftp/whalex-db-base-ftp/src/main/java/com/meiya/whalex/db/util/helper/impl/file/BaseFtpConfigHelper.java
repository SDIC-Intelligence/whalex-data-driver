package com.meiya.whalex.db.util.helper.impl.file;

import cn.hutool.core.codec.Base64;
import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.util.StrUtil;
import com.github.benmanes.caffeine.cache.Cache;
import com.meiya.whalex.annotation.DbHelper;
import com.meiya.whalex.business.entity.CertificateConf;
import com.meiya.whalex.business.entity.DatabaseConf;
import com.meiya.whalex.business.entity.TableConf;
import com.meiya.whalex.business.service.DatabaseConfService;
import com.meiya.whalex.db.entity.AbstractCursorCache;
import com.meiya.whalex.db.entity.file.FtpClient;
import com.meiya.whalex.db.entity.file.FtpDatabaseInfo;
import com.meiya.whalex.db.entity.file.FtpTableInfo;
import com.meiya.whalex.db.entity.file.OrcWriter;
import com.meiya.whalex.db.kerberos.KerberosUniformAuth;
import com.meiya.whalex.db.kerberos.KerberosUniformAuthFactory;
import com.meiya.whalex.db.kerberos.exception.GetKerberosException;
import com.meiya.whalex.db.module.file.BaseFtpServiceImpl;
import com.meiya.whalex.db.util.helper.AbstractDbModuleConfigHelper;
import com.meiya.whalex.exception.BusinessException;
import com.meiya.whalex.exception.ExceptionCode;
import com.meiya.whalex.util.DbBeanManagerUtil;
import com.meiya.whalex.util.JsonUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetWriter;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Hadoop 配置信息工具类
 *
 * @author wangkm1
 * @date 2025/04/01
 * @project whale-cloud-platformX
 */
@Slf4j
public class BaseFtpConfigHelper<S extends FtpClient
        , D extends FtpDatabaseInfo
        , T extends FtpTableInfo
        , C extends AbstractCursorCache> extends AbstractDbModuleConfigHelper<S, D, T, C> {

    @Override
    public boolean checkDataSourceStatus(S connect) throws Exception {
        connect.getFileSystem().getStatus();
        return true;
    }

    @Override
    public D initDbModuleConfig(DatabaseConf conf) {
        String connSetting = conf.getConnSetting();
        if (StringUtils.isBlank(connSetting)) {
            log.error("Hadoop database connSetting is null! bigResourceId: [{}]", conf.getBigdataResourceId());
            throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "数据库配置必填参数缺失，请校验参数配置是否正确!");
        }
        Map<String, String> dbMap = JsonUtil.jsonStrToObject(connSetting, Map.class);
        String serviceUrl = dbMap.get("serviceUrl");
        if (serviceUrl == null) {
            log.error("Hadoop database serviceUrl is null! bigResourceId: [{}]", conf.getBigdataResourceId());
            throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "数据库配置必填参数缺失，请校验参数配置是否正确!");
        }


        // 认证类型
        String authType = dbMap.get("authType");

        // 用户名
        String userName = dbMap.get("userName");
        if (StringUtils.isBlank(userName)) {
            userName = dbMap.get("username");
        }

        // 编码
        String controlEncoding = dbMap.get("controlEncoding");
        // 密码
        String password = dbMap.get("password");




        //设置认证类型
//        if (StringUtils.isBlank(authType)) {
//            if (StringUtils.isNotBlank(certificateId) || StringUtils.isNotBlank(certificateBase64Str)) {
//                authType = "cer";
//            } else {
//                authType = "normal";
//            }
//        }
        // 基础路径 生成  /xxx 这种格式
        String bashPath = dbMap.get("bashPath");
        // 若没有传递路径则默认为空字符串
        if (StringUtils.isNotBlank(bashPath)) {
            // 若已经配置 bashPath 那么判断是否以 / 为开头
            if (!StringUtils.startsWithIgnoreCase(bashPath, "/")) {
                bashPath = "/" + bashPath;
            }
            // 若以 / 结尾则去除掉，因为在 table 配置中会加上 /
            if (StringUtils.endsWithIgnoreCase(bashPath, "/")) {
                bashPath = StringUtils.substringBeforeLast(bashPath, "/");
            }
        } else {
            // 若未设置 bashPath 则配置为空字符串
            bashPath = "";
        }
        return (D) FtpDatabaseInfo.builder()
                .serviceUrl(serviceUrl)
                .authType(authType)
                .userName(userName)
                .bashPath(bashPath)
                .password(password)
                .controlEncoding(controlEncoding)
                .build();
    }

    @Override
    public T initTableConfig(TableConf conf) {
        String tableJson = conf.getTableJson();
        if (StringUtils.isBlank(tableJson)) {
            log.warn("Hadoop database tableJson is null! dbId: [{}]", conf.getId());
            FtpTableInfo build = FtpTableInfo.builder().build();
            build.setTableName(conf.getTableName());
            String path = conf.getTableName();
            // 若没有 path，可能以 tableName 作为路径
            if (StringUtils.isNotBlank(path)) {
                // 若不以 / 开头，则补上
                if (!StringUtils.startsWithIgnoreCase(path, "/")) {
                    path = "/" + path;
                }
                // path 长度 > 1，则说明不止含有 /，则判断是否 结尾以 /，若是则去除
                if (StringUtils.length(path) > 1 && StringUtils.endsWithIgnoreCase(path, "/")) {
                    path = StringUtils.substringBeforeLast(path, "/");
                }
                build.setPath(path);
            }
            return (T) build;
        }
        Map<String, Object> tableMap = JsonUtil.jsonStrToObject(tableJson, Map.class);
        // 获取路径
        String path = (String) tableMap.get("path");
        // 若没有 path，可能以 tableName 作为路径
        if (StringUtils.isBlank(path)) {
            path = conf.getTableName();;
        }
        if (StringUtils.isNotBlank(path)) {
            // 若不以 / 开头，则补上
            if (!StringUtils.startsWithIgnoreCase(path, "/")) {
                path = "/" + path;
            }
            // path 长度 > 1，则说明不止含有 /，则判断是否 结尾以 /，若是则去除
            if (StringUtils.length(path) > 1 && StringUtils.endsWithIgnoreCase(path, "/")) {
                path = StringUtils.substringBeforeLast(path, "/");
            }
        }
        String fileType = (String) tableMap.get("fileType");
        if (StringUtils.isBlank(fileType)) {
            fileType = FtpTableInfo.PARQUET_TYPE;
        }
        String format = (String) tableMap.get("format");
        String compressionCode = (String) tableMap.get("compressionCode");
        if (StringUtils.isNotBlank(compressionCode)) {
            if (!StrUtil.equalsAnyIgnoreCase(compressionCode
                    , FtpTableInfo.COMPRESSION_SNAPPY
                    , FtpTableInfo.COMPRESSION_BROTLI
                    , FtpTableInfo.COMPRESSION_GZIP
                    , FtpTableInfo.COMPRESSION_LZ4
                    , FtpTableInfo.COMPRESSION_LZO
                    , FtpTableInfo.COMPRESSION_ZSTD
                    , FtpTableInfo.COMPRESSION_NONE
            )) {
                throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "HDFS 不支持当前配置的压缩方式!");
            }
        } else {
            compressionCode = FtpTableInfo.COMPRESSION_SNAPPY;
        }
        Object blockSizeObj = tableMap.get("blockSize");
        Long blockSize = blockSizeObj == null ? 1024L * 1024L * 125L : Long.valueOf(String.valueOf(blockSizeObj));
        FtpTableInfo ftpTableInfo = FtpTableInfo.builder().fileType(fileType).format(format).path(path).compressionCode(compressionCode).blockSize(blockSize).build();
        ftpTableInfo.setTableName(conf.getTableName());
        return (T) ftpTableInfo;
    }

    @Override
    public S initDbConnect(FtpDatabaseInfo databaseConf, FtpTableInfo tableConf) {


        try {
            // 进行认证登录并创建连接
            return (S) new FtpClient(databaseConf);
        } catch (Exception e) {
            throw new BusinessException(e, ExceptionCode.CREATE_DB_CONNECT_EXCEPTION);
        }
    }

    @Override
    public void destroyDbConnect(String cacheKey, FtpClient connect) throws Exception {
        DbHelper annotation = this.getClass().getAnnotation(DbHelper.class);
        // 获取 Hadoop 服务对象
        BaseFtpServiceImpl serviceBean = (BaseFtpServiceImpl) DbBeanManagerUtil.getServiceBean(annotation.dbType(), annotation.version(), annotation.cloudVendors());
        // 获取 写入对象缓存
        ConcurrentHashMap<String, Cache<String, Map<String, ParquetWriter<Group>>>> parquetWriterCache = serviceBean.getParquetWriterCache();
        ConcurrentHashMap<String, Cache<String, Map<String, OrcWriter>>> orcWriterCache = serviceBean.getOrcWriterCache();
        // 移除当前连接对应的写入缓存
        Cache<String, Map<String, ParquetWriter<Group>>> parquetWriteCache = parquetWriterCache.remove(cacheKey);
        Cache<String, Map<String, OrcWriter>> orcWriteCache = orcWriterCache.remove(cacheKey);

        // 获取锁缓存
        ConcurrentHashMap<String, ReentrantLock> writeLockCache = serviceBean.getWriteLockCache();

        Map<String, Map<String, ? extends Object>> writeMap = new HashMap<>();

        if (parquetWriteCache != null) {
            ConcurrentMap<String, Map<String, ParquetWriter<Group>>> writerConcurrentMap = parquetWriteCache.asMap();
            writeMap.putAll(writerConcurrentMap);
        }

        if (orcWriteCache != null) {
            ConcurrentMap<String, Map<String, OrcWriter>> writerConcurrentMap = orcWriteCache.asMap();
            writeMap.putAll(writerConcurrentMap);
        }

        if (writeMap.size() > 0) {
            Set<String> removeLock = new HashSet<>();
            for (Map.Entry<String, Map<String, ? extends Object>> entry : writeMap.entrySet()) {
                Map<String, ? extends Object> value = entry.getValue();
                String key = value.keySet().iterator().next();
                String[] split = StringUtils.split(key, "$");
                String writeKey = entry.getKey();
                String tableName = StringUtils.split(writeKey, "$")[0];
                String path = split[0];
                String _cacheKey = split[1];
                // 获取锁
                ReentrantLock reentrantLock = writeLockCache.get(tableName + "$" + _cacheKey);
                if (reentrantLock.tryLock()) {
                    reentrantLock.lock();
                    // 关闭写对象
                    try {
                        Object next = value.values().iterator().next();
                        if (next instanceof ParquetWriter) {
                            serviceBean.closeParquetWriter(connect, path, (ParquetWriter<Group>) next);
                        } else {
                            serviceBean.closeOrcWriter(connect, path, ((OrcWriter) next).getWriter());
                        }
                    } catch (Exception e) {
                        log.error("写入对象流关闭失败! cacheKey: [{}] file: [{}]", _cacheKey, path, e);
                    } finally {
                        removeLock.add(tableName + "$" + _cacheKey);
                        reentrantLock.unlock();
                    }
                }
            }
            if (CollectionUtil.isNotEmpty(removeLock)) {
                for (String lock : removeLock) {
                    writeLockCache.remove(lock);
                }
            }
            writeMap.clear();
        }

        if (parquetWriteCache != null) {
            parquetWriteCache.invalidateAll();
        }

        if (orcWriteCache != null) {
            orcWriteCache.invalidateAll();
        }

        connect.getFileSystem().close();
    }
}
