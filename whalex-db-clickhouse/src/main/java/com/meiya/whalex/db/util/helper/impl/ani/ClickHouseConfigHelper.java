package com.meiya.whalex.db.util.helper.impl.ani;

import com.meiya.whalex.annotation.DbHelper;
import com.meiya.whalex.business.entity.DatabaseConf;
import com.meiya.whalex.business.entity.TableConf;
import com.meiya.whalex.db.entity.ani.ClickHouseDatabaseInfo;
import com.meiya.whalex.db.entity.ani.ClickHouseTableInfo;
import com.meiya.whalex.db.entity.ani.EngineType;
import com.meiya.whalex.db.util.helper.AbstractDbModuleConfigHelper;
import com.meiya.whalex.exception.BusinessException;
import com.meiya.whalex.exception.ExceptionCode;
import com.meiya.whalex.interior.db.constant.CloudVendorsEnum;
import com.meiya.whalex.interior.db.constant.DbResourceEnum;
import com.meiya.whalex.interior.db.constant.DbVersionEnum;
import com.meiya.whalex.sql.ani.RdbmsCursorCache;
import com.meiya.whalex.util.JsonUtil;
import com.meiya.whalex.util.encrypt.AESUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.dbcp.BasicDataSourceFactory;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.lang3.StringUtils;
import ru.yandex.clickhouse.BalancedClickhouseDataSource;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.*;

/**
 * @author 黄河森
 * @date 2022/7/6
 * @package com.meiya.whalex.db.util.helper.impl.ani
 * @project whalex-data-driver
 */
@DbHelper(dbType = DbResourceEnum.clickhouse, version = DbVersionEnum.CLICKHOUSE_22_2_2_1, cloudVendors = CloudVendorsEnum.OPEN)
@Slf4j
public class ClickHouseConfigHelper extends AbstractDbModuleConfigHelper<QueryRunner, ClickHouseDatabaseInfo, ClickHouseTableInfo, RdbmsCursorCache> {
    @Override
    public boolean checkDataSourceStatus(QueryRunner connect) throws Exception {
        DataSource dataSource = connect.getDataSource();
        if (dataSource instanceof BalancedClickhouseDataSource) {
            ((BalancedClickhouseDataSource) dataSource).actualize();
        }
        Connection connection = dataSource.getConnection();
        if (connection != null) {
            connection.close();
        }
        return true;
    }

    @Override
    public ClickHouseDatabaseInfo initDbModuleConfig(DatabaseConf conf) {
        String connSetting = conf.getConnSetting();
        Map<String, String> dbConfMap = JsonUtil.jsonStrToObject(connSetting, Map.class);
        String serviceUrl = dbConfMap.get("serviceUrl");
        // 集群名称
        String clusterName = dbConfMap.get("clusterName");
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
//                log.error("ClickHouse init db config password [{}] decrypt fail!!!", password, e);
            }
        }
        if (StringUtils.isBlank(userName)
                || StringUtils.isBlank(password)
                || StringUtils.isBlank(serviceUrl)
                || StringUtils.isBlank(database)) {
            log.error("ClickHouse init database config fail, must field info is null! bigResourceId: [{}]", conf.getBigdataResourceId());
            throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "数据库配置必填参数缺失，请校验参数配置是否正确!");
        }
        ClickHouseDatabaseInfo clickHouseDatabaseInfo = new ClickHouseDatabaseInfo(userName, password, serviceUrl, database);
        clickHouseDatabaseInfo.setMaxActive(threadConfig.getMaximumPoolSize());
        clickHouseDatabaseInfo.setClusterName(clusterName);
        return clickHouseDatabaseInfo;
    }

    @Override
    public ClickHouseTableInfo initTableConfig(TableConf conf) {
        String tableName = conf.getTableName();
        if (StringUtils.isBlank(tableName)) {
            log.error("ClickHouse init table config fail, tableName is null! dbId: [{}]", conf.getId());
            throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "数据表配置必填参数缺失，请校验参数配置是否正确!");
        }
        ClickHouseTableInfo tableInfo = new ClickHouseTableInfo();
        tableInfo.setTableName(tableName);

        // 解析tableJson配置
        String tableJson = conf.getTableJson();
        if (StringUtils.isNotBlank(tableJson)) {
            Map<String, Object> tableSettingMap = JsonUtil.jsonStrToMap(tableJson);
            Object engine = tableSettingMap.get("engine");
            Object openDistributed = tableSettingMap.get("openDistributed");
            Object openReplica = tableSettingMap.get("openReplica");
            if (engine != null) {
                EngineType engineType = EngineType.findEngineType(String.valueOf(engine));
                tableInfo.setEngine(engineType);
            }
            if (openDistributed != null) {
                if (Boolean.parseBoolean(String.valueOf(openDistributed))) {
                    tableInfo.setOpenDistributed(true);
                }
            }
            if (openReplica != null) {
                if (Boolean.parseBoolean(String.valueOf(openReplica))) {
                    tableInfo.setOpenReplica(true);
                }
            }
        }
        return tableInfo;
    }

    @Override
    public QueryRunner initDbConnect(ClickHouseDatabaseInfo databaseConf, ClickHouseTableInfo tableConf) {
        Properties prop = new Properties();
        prop.put("url", String.format(ClickHouseDatabaseInfo.URL_TEMPLATE,
                databaseConf.getServiceUrl(),
                databaseConf.getDatabase()));
        prop.put("username", databaseConf.getUserName());
        prop.put("password", databaseConf.getPassword());
        prop.put("driverClassName", ClickHouseDatabaseInfo.DRIVER_CLASS_NAME);
        prop.put("initialSize", String.valueOf(databaseConf.getInitialSize()));
        prop.put("minIdle", String.valueOf(databaseConf.getMinIdle()));
        prop.put("maxActive", String.valueOf(databaseConf.getMaxActive()));
        prop.put("maxWait", String.valueOf(threadConfig.getTimeOut() * 1000));
        prop.put("poolPreparedStatements", String.valueOf(ClickHouseDatabaseInfo.POOL_PREPARED_STATEMENTS));
        prop.put("testWhileIdle", String.valueOf(ClickHouseDatabaseInfo.TEST_WHILE_IDLE));
        prop.put("validationQuery", ClickHouseDatabaseInfo.VALIDATION_QUERY);
        DataSource dataSource = null;
        try {
            if (StringUtils.contains(databaseConf.getServiceUrl(), ",")) {
                dataSource = createClusterDataSource(prop);
            } else {
                dataSource = BasicDataSourceFactory.createDataSource(prop);
            }
            Connection connection = dataSource.getConnection();
            connection.close();
        } catch (Exception e) {
            throw new BusinessException(e, ExceptionCode.CREATE_DB_CONNECT_EXCEPTION);
        }
        return new QueryRunner(dataSource);
    }

    @Override
    public void destroyDbConnect(String cacheKey, QueryRunner connect) throws Exception {
    }

    /**
     * 创建 clickhouse 高可用数据源
     *
     * @param properties
     * @return
     * @throws Exception
     */
    private BalancedClickhouseDataSource createClusterDataSource(Properties properties) {
        ClickHouseProperties clickHouseProperties = new ClickHouseProperties();
        String value = null;

        value = properties.getProperty("maxActive");
        if (value != null) {
            clickHouseProperties.setMaxTotal(Integer.parseInt(value));
            clickHouseProperties.setDefaultMaxPerRoute(Integer.parseInt(value));
        }

        value = properties.getProperty("maxWait");
        if (value != null) {
            clickHouseProperties.setSocketTimeout(Integer.parseInt(value));
        }

        value = properties.getProperty("password");
        if (value != null) {
            clickHouseProperties.setPassword(value);
        }

        value = properties.getProperty("username");
        if (value != null) {
            clickHouseProperties.setUser(value);
        }

        BalancedClickhouseDataSource balancedClickhouseDataSource = new BalancedClickhouseDataSource(properties.getProperty("url"), clickHouseProperties);
        balancedClickhouseDataSource.actualize();
        return balancedClickhouseDataSource;
    }
}
