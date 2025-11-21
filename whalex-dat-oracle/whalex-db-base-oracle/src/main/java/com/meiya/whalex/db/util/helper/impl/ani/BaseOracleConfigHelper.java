package com.meiya.whalex.db.util.helper.impl.ani;

import com.github.benmanes.caffeine.cache.Cache;
import com.meiya.whalex.business.entity.DatabaseConf;
import com.meiya.whalex.business.entity.TableConf;
import com.meiya.whalex.cache.DatCaffeine;
import com.meiya.whalex.db.entity.AbstractCursorCache;
import com.meiya.whalex.db.entity.ani.BaseOracleDatabaseInfo;
import com.meiya.whalex.db.entity.ani.BaseOracleTableInfo;
import com.meiya.whalex.db.util.helper.AbstractDbModuleConfigHelper;
import com.meiya.whalex.exception.BusinessException;
import com.meiya.whalex.exception.ExceptionCode;
import com.meiya.whalex.util.JsonUtil;
import com.meiya.whalex.util.encrypt.AESUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.dbcp2.BasicDataSourceFactory;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.lang3.StringUtils;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Oracle 配置管理工具
 *
 * @author 蔡荣桂
 * @date 2021/4/14
 * @project whale-cloud-platformX
 */

@Slf4j
public class BaseOracleConfigHelper extends AbstractDbModuleConfigHelper<QueryRunner, BaseOracleDatabaseInfo, BaseOracleTableInfo, AbstractCursorCache> {

    /**
     * 库 -> 表 -> 字段 缓存
     * cacheKey -> tableName -> column:datatype
     */
    private Cache<String, Cache<String, Map<String, Integer>>> TABLE_COLUMN_CACHE;

    /**
     * 获取表对应的字段类型信息
     *
     * @param databaseInfo
     * @param tableInfo
     * @param tableColumnCallback
     * @return
     */
    public Map<String, Integer> getTableColumn(BaseOracleDatabaseInfo databaseInfo, BaseOracleTableInfo tableInfo, Function<String, Map<String, Integer>> tableColumnCallback) {
        String cacheKey = getCacheKey(databaseInfo, tableInfo);
        Cache<String, Map<String, Integer>> tableCloumnCache = TABLE_COLUMN_CACHE.get(cacheKey, new Function<String, Cache<String, Map<String, Integer>>>() {
            @Override
            public Cache<String, Map<String, Integer>> apply(String cacheKey) {
                return DatCaffeine.<String, Map<String, Integer>>newBuilder().expireAfterAccess(1, TimeUnit.HOURS)
                        .removalListener((k, v, removalCause) -> {
                            v.clear();
                        }).build();
            }
        });
        Map<String, Integer> fieldTypeEnumMap = tableCloumnCache.get(tableInfo.getTableName(), tableColumnCallback);
        return fieldTypeEnumMap;
    }

    @Override
    public void init(boolean loadDbConf) {
        super.init(loadDbConf);
        TABLE_COLUMN_CACHE = DatCaffeine.<String, Cache<String, Map<String, Integer>>>newBuilder().expireAfterAccess(1, TimeUnit.HOURS)
                .removalListener((k, v, removalCause) -> {
                    v.invalidateAll();
                }).build();
    }

    @Override
    public boolean checkDataSourceStatus(QueryRunner connect) throws Exception {
        Connection connection = connect.getDataSource().getConnection();
        if (connection != null) {
            connection.close();
        }
        return true;
    }

    @Override
    public BaseOracleDatabaseInfo initDbModuleConfig(DatabaseConf conf) {
        String connSetting = conf.getConnSetting();
        Map<String, Object> dbConfMap = JsonUtil.jsonStrToObject(connSetting, Map.class);
        String serviceUrl = (String) dbConfMap.get("serviceUrl");
        String port = (String) dbConfMap.get("port");
        String databaseName = (String) dbConfMap.get("dbaseName");
        String connectionType = (String) dbConfMap.get("connectionType");
        if (StringUtils.isBlank(databaseName)) {
            databaseName = (String) dbConfMap.get("database");
        }
        String userName = (String) dbConfMap.get("userName");
        if (StringUtils.isBlank(userName)) {
            userName = (String) dbConfMap.get("username");
        }
        String password = (String) dbConfMap.get("password");
        if (StringUtils.isBlank(password)) {
            password = (String) dbConfMap.get("userPassword");
        }

        Boolean ignoreCase = (Boolean) dbConfMap.get("ignoreCase");
        if(ignoreCase == null) {
            ignoreCase = true;
        }


        // AES 解密
        if (StringUtils.isNotBlank(password)) {
            try {
                password = AESUtil.decrypt(password);
            } catch (Exception e) {
//                log.error("Oracle init db config password [{}] decrypt fail!!!", password, e);
            }
        }
        if (StringUtils.isBlank(userName)
                || StringUtils.isBlank(password)
                || StringUtils.isBlank(serviceUrl)
                || StringUtils.isBlank(port)
                || StringUtils.isBlank(databaseName)) {
            log.error("Oracle init database config fail, must field info is null! bigResourceId: [{}]", conf.getBigdataResourceId());
            throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "数据库配置必填参数缺失，请校验参数配置是否正确!");
        }

        String schema = (String) dbConfMap.get("schema");
        if(StringUtils.isBlank(schema)) {
            schema = userName.toUpperCase();
        }

        BaseOracleDatabaseInfo oracleDatabaseInfo = new BaseOracleDatabaseInfo(userName, password, serviceUrl, port, databaseName, connectionType);
        oracleDatabaseInfo.setSchema(schema);
        oracleDatabaseInfo.setMaxActive(threadConfig.getMaximumPoolSize());
        oracleDatabaseInfo.setIgnoreCase(ignoreCase);
        return oracleDatabaseInfo;
    }

    @Override
    public BaseOracleTableInfo initTableConfig(TableConf conf) {
        String tableName = conf.getTableName();
        if (StringUtils.isBlank(tableName)) {
            log.error("Oracle init table config fail, tableName is null! dbId: [{}]", conf.getId());
            throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "数据表配置必填参数缺失，请校验参数配置是否正确!");
        }
        BaseOracleTableInfo tableInfo = new BaseOracleTableInfo();
        tableInfo.setTableName(tableName);
        return tableInfo;
    }

    @Override
    public QueryRunner initDbConnect(BaseOracleDatabaseInfo databaseConf, BaseOracleTableInfo tableConf) {
        Properties prop = new Properties();
        String urlTemplate = databaseConf.getUrlTemplate();
        prop.put("url", String.format(urlTemplate,
                databaseConf.getServiceUrl(),
                databaseConf.getPort(),
                databaseConf.getDatabaseName()));
        prop.put("username", databaseConf.getUserName());
        prop.put("password", databaseConf.getPassword());
        prop.put("driverClassName", BaseOracleDatabaseInfo.DRIVER_CLASS_NAME);
        prop.put("validationQuery", BaseOracleDatabaseInfo.VALIDATION_QUERY);
        prop.put("initialSize", databaseConf.getInitialSize());
        prop.put("minIdle", databaseConf.getMinIdle());
        prop.put("maxIdle", databaseConf.getMaxActive());
        prop.put("maxWait",  threadConfig.getTimeOut() * 1000);
        prop.put("timeBetWeenEvictionRunsMillis",  threadConfig.getTimeOut() * 1000);
        prop.put("minEvictableIdleTimeMillis",  300000);
        prop.put("maxActive", databaseConf.getMaxActive());
        prop.put("poolPreparedStatements", BaseOracleDatabaseInfo.POOL_PREPARED_STATEMENTS);
        prop.put("testWhileIdle", BaseOracleDatabaseInfo.TEST_WHILE_IDLE);
        DataSource dataSource = null;
        try {
            dataSource = BasicDataSourceFactory.createDataSource(prop);
        } catch (Exception e) {
            throw new BusinessException(e, ExceptionCode.CREATE_DB_CONNECT_EXCEPTION);
        }
        return new QueryRunner(dataSource);
    }

    @Override
    public void destroyDbConnect(String cacheKey, QueryRunner connect) throws Exception {
        TABLE_COLUMN_CACHE.invalidate(cacheKey);
    }
}
