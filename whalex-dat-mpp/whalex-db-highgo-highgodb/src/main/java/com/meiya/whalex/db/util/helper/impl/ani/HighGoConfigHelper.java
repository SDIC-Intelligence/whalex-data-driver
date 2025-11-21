package com.meiya.whalex.db.util.helper.impl.ani;

import cn.hutool.crypto.digest.DigestUtil;
import com.meiya.whalex.annotation.DbHelper;
import com.meiya.whalex.business.entity.DatabaseConf;
import com.meiya.whalex.db.entity.ani.BasePostGreTableInfo;
import com.meiya.whalex.db.entity.ani.HighGoDatabaseInfo;
import com.meiya.whalex.exception.BusinessException;
import com.meiya.whalex.exception.ExceptionCode;
import com.meiya.whalex.interior.db.constant.CloudVendorsEnum;
import com.meiya.whalex.interior.db.constant.DbResourceEnum;
import com.meiya.whalex.interior.db.constant.DbVersionEnum;
import com.meiya.whalex.sql.ani.RdbmsCursorCache;
import com.meiya.whalex.util.JsonUtil;
import com.meiya.whalex.util.encrypt.AESUtil;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;

/**
 * Gauss 配置管理工具
 *
 * @author 黄河森
 * @date 2019/12/23
 * @project whale-cloud-platformX
 */

@DbHelper(dbType = DbResourceEnum.highgo, version = DbVersionEnum.HIGHGO_1_2, cloudVendors = CloudVendorsEnum.HighGo)
@Slf4j
public class HighGoConfigHelper extends BasePostGreConfigHelper<QueryRunner
        , HighGoDatabaseInfo, BasePostGreTableInfo, RdbmsCursorCache> {

    public static final String HIGHGO_URL_TEMPLATE = "jdbc:highgo://%s:%s/%s";

    public static final String HIGNGO_DRIVER_CLASS_NAME = "com.highgo.jdbc.Driver";

    @Override
    public HighGoDatabaseInfo initDbModuleConfig(DatabaseConf conf) {
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
            }
        }
        String schema = dbConfMap.get("schema");
        if (StringUtils.isBlank(schema)) {
            schema = dbConfMap.get("scheme");
        }
        if (StringUtils.isBlank(userName)
                || StringUtils.isBlank(password)
                || StringUtils.isBlank(serviceUrl)
                || StringUtils.isBlank(port)
                || StringUtils.isBlank(database)
                || StringUtils.isBlank(schema)) {
            log.error("PostGre init database config fail, must field info is null! bigResourceId: [{}]", conf.getBigdataResourceId());
            throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "数据库配置必填参数缺失，请校验参数配置是否正确!");
        }

        boolean ignoreCase = dbConfMap.get("ignoreCase") == null ? true : Boolean.parseBoolean(String.valueOf(dbConfMap.get("ignoreCase")));

        HighGoDatabaseInfo highGoDatabaseInfo = new HighGoDatabaseInfo(userName, password, serviceUrl, port, database, schema, ignoreCase);
        highGoDatabaseInfo.setMaxActive(threadConfig.getMaximumPoolSize());

        Boolean tinyInt1isBit = dbConfMap.get("tinyInt1isBit") == null ? true : Boolean.valueOf(String.valueOf(dbConfMap.get("tinyInt1isBit")));
        highGoDatabaseInfo.setTinyInt1isBit(tinyInt1isBit);
        return highGoDatabaseInfo;
    }

    @Override
    public QueryRunner initDbConnect(HighGoDatabaseInfo databaseConf, BasePostGreTableInfo tableConf) {
        HikariConfig hikariConfig = new HikariConfig();
        String url = String.format(HIGHGO_URL_TEMPLATE,
                databaseConf.getServiceUrl(),
                databaseConf.getPort(),
                databaseConf.getDatabase());

        hikariConfig.setJdbcUrl(url);
        hikariConfig.setUsername(databaseConf.getUserName());
        hikariConfig.setPassword(databaseConf.getPassword());
        hikariConfig.setDriverClassName(HIGNGO_DRIVER_CLASS_NAME);
        hikariConfig.setMinimumIdle(databaseConf.getMinIdle());
        hikariConfig.setMaximumPoolSize(Math.min((Runtime.getRuntime().availableProcessors() * 2) + 1, databaseConf.getMaxActive()));
        hikariConfig.setPoolName(DigestUtil.md5Hex(getCacheKey(databaseConf, tableConf)));
        HikariDataSource dataSource = null;
        try {
            dataSource = new HikariDataSource(hikariConfig);
        } catch (Exception e) {
            throw new BusinessException(e, ExceptionCode.CREATE_DB_CONNECT_EXCEPTION);
        }
        return new QueryRunner(dataSource);
    }
}
