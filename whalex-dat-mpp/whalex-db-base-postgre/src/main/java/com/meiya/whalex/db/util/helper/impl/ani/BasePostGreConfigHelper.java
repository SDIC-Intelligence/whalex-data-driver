package com.meiya.whalex.db.util.helper.impl.ani;

import cn.hutool.crypto.digest.DigestUtil;
import com.github.benmanes.caffeine.cache.Cache;
import com.meiya.whalex.business.entity.DatabaseConf;
import com.meiya.whalex.business.entity.TableConf;
import com.meiya.whalex.cache.DatCaffeine;
import com.meiya.whalex.db.entity.ani.BasePostGreDatabaseInfo;
import com.meiya.whalex.db.entity.ani.BasePostGreTableInfo;
import com.meiya.whalex.db.util.helper.AbstractDbModuleConfigHelper;
import com.meiya.whalex.exception.BusinessException;
import com.meiya.whalex.exception.ExceptionCode;
import com.meiya.whalex.sql.ani.RdbmsCursorCache;
import com.meiya.whalex.util.JsonUtil;
import com.meiya.whalex.util.encrypt.AESUtil;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.lang3.StringUtils;
import java.sql.Connection;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * MySql 配置管理工具
 *
 * @author 黄河森
 * @date 2019/12/23
 * @project whale-cloud-platformX
 */

@Slf4j
public class BasePostGreConfigHelper<S extends QueryRunner, D extends BasePostGreDatabaseInfo, T extends BasePostGreTableInfo, C extends RdbmsCursorCache>
        extends AbstractDbModuleConfigHelper<S, D, T, C> {

    private Cache<String, String> TABLE_PRIMARY_KEY_CACHE;

    @Override
    public void init(boolean loadDbConf) {
        super.init(loadDbConf);
        TABLE_PRIMARY_KEY_CACHE = DatCaffeine.<String, String>newBuilder().expireAfterAccess(1, TimeUnit.HOURS)
                .removalListener((k, v, removalCause) -> {
                }).build();
    }

    @Override
    public boolean checkDataSourceStatus(S connect) throws Exception {
        Connection connection = connect.getDataSource().getConnection();
        if (connection != null) {
            connection.close();
        }
        return true;
    }

    @Override
    public D initDbModuleConfig(DatabaseConf conf) {
        String connSetting = conf.getConnSetting();
        Map<String, String> dbConfMap = JsonUtil.jsonStrToObject(connSetting, Map.class);
        String serviceUrl = dbConfMap.get("serviceUrl");
        String port = dbConfMap.get("port");
        String database = dbConfMap.get("database");
        if (StringUtils.isBlank(database)) {
            database = dbConfMap.get("dbaseName");
        }
        String userName = dbConfMap.get("userName");
        if (StringUtils.isBlank(userName)) {
            userName = dbConfMap.get("username");
        }
        String password = dbConfMap.get("password");
        if (StringUtils.isBlank(password)) {
            password = dbConfMap.get("userPassword");
        }
        // AES 解密
        if (StringUtils.isNotBlank(password)) {
            try {
                password = AESUtil.decrypt(password);
            } catch (Exception e) {
//                log.error("PostGre init db config password [{}] decrypt fail!!!", password, e);
            }
        }
        String schema = dbConfMap.get("schema");
        if (StringUtils.isBlank(schema)) {
            schema = dbConfMap.get("scheme");
        }

        // 是否开启 tinyInt1isBit
        Boolean tinyInt1isBit = dbConfMap.get("tinyInt1isBit") == null ? true : Boolean.valueOf(String.valueOf(dbConfMap.get("tinyInt1isBit")));

        if (StringUtils.isBlank(userName)
                || StringUtils.isBlank(password)
                || StringUtils.isBlank(serviceUrl)
                || StringUtils.isBlank(port)) {
            log.error("PostGre init database config fail, must field info is null! bigResourceId: [{}]", conf.getBigdataResourceId());
            throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "数据库配置必填参数缺失，请校验参数配置是否正确!");
        }

        boolean ignoreCase = dbConfMap.get("ignoreCase") == null ? true : Boolean.parseBoolean(String.valueOf(dbConfMap.get("ignoreCase")));
        boolean likeToiLike = dbConfMap.get("likeToiLike") == null ? false : Boolean.parseBoolean(String.valueOf(dbConfMap.get("likeToiLike")));

        BasePostGreDatabaseInfo basePostGreDatabaseInfo = new BasePostGreDatabaseInfo(userName, password, serviceUrl, port, database, schema, ignoreCase);
        basePostGreDatabaseInfo.setTinyInt1isBit(tinyInt1isBit);
        basePostGreDatabaseInfo.setLikeToiLike(likeToiLike);
        basePostGreDatabaseInfo.setMaxActive(threadConfig.getMaximumPoolSize());
        return (D) basePostGreDatabaseInfo;
    }

    @Override
    public T initTableConfig(TableConf conf) {
        String tableName = conf.getTableName();
        if (StringUtils.isBlank(tableName)) {
            log.error("PostGre init table config fail, tableName is null! dbId: [{}]", conf.getId());
            throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "数据表配置必填参数缺失，请校验参数配置是否正确!");
        }
        BasePostGreTableInfo tableInfo = new BasePostGreTableInfo();
        tableInfo.setTableName(tableName);
        return (T) tableInfo;
    }

    @Override
    public S initDbConnect(D databaseConf, T tableConf) {
        HikariConfig hikariConfig = new HikariConfig();

        StringBuilder sb = new StringBuilder();
        sb.append("jdbc:postgresql://").append(databaseConf.getServiceUrl()).append(":").append(databaseConf.getPort()).append("/");

        if(StringUtils.isNotBlank(databaseConf.getDatabase())) {
            sb.append(databaseConf.getDatabase());
        }

        hikariConfig.setJdbcUrl(sb.toString());
        hikariConfig.setUsername(databaseConf.getUserName());
        hikariConfig.setPassword(databaseConf.getPassword());
        hikariConfig.setDriverClassName(BasePostGreDatabaseInfo.DRIVER_CLASS_NAME);
        hikariConfig.setMinimumIdle(databaseConf.getMinIdle());
        hikariConfig.setMaximumPoolSize(Math.min((Runtime.getRuntime().availableProcessors() * 2) + 1, databaseConf.getMaxActive()));
        hikariConfig.setPoolName(DigestUtil.md5Hex(getCacheKey(databaseConf, tableConf)));
//        hikariConfig.setMetricsTrackerFactory(new MicrometerMetricsTrackerFactory(Metrics.globalRegistry));
        HikariDataSource dataSource = null;
        try {
            dataSource = new HikariDataSource(hikariConfig);
        } catch (Exception e) {
            throw new BusinessException(e, ExceptionCode.CREATE_DB_CONNECT_EXCEPTION);
        }
        return (S) new QueryRunner(dataSource);
    }

    @Override
    public void destroyDbConnect(String cacheKey, S connect) throws Exception {
    }



    public String getPrimaryKeyField(D databaseConf, T tableConf, Function<String, String> function) {
        String cacheKey = getCacheKey(databaseConf, tableConf);
        return TABLE_PRIMARY_KEY_CACHE.get(cacheKey, (ck)->{
            return function.apply(ck);
        });
    }
}
