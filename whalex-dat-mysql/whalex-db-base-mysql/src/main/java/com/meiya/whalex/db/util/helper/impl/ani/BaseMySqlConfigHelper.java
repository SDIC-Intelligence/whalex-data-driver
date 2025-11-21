package com.meiya.whalex.db.util.helper.impl.ani;

import com.meiya.whalex.business.entity.DatabaseConf;
import com.meiya.whalex.business.entity.TableConf;
import com.meiya.whalex.db.entity.ani.BaseMySqlDatabaseInfo;
import com.meiya.whalex.db.entity.ani.BaseMySqlTableInfo;
import com.meiya.whalex.db.util.helper.AbstractDbModuleConfigHelper;
import com.meiya.whalex.exception.BusinessException;
import com.meiya.whalex.exception.ExceptionCode;
import com.meiya.whalex.sql.ani.RdbmsCursorCache;
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

/**
 * MySql 配置管理工具
 *
 * @author 黄河森
 * @date 2019/12/23
 * @project whale-cloud-platformX
 */
@Slf4j
public class BaseMySqlConfigHelper<S extends QueryRunner, D extends BaseMySqlDatabaseInfo, T extends BaseMySqlTableInfo, C extends RdbmsCursorCache>
        extends AbstractDbModuleConfigHelper<S, D, T, C> {

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
        Map<String, Object> dbConfMap = JsonUtil.jsonStrToObject(connSetting, Map.class);
        String serviceUrl = (String) dbConfMap.get("serviceUrl");
        String port = dbConfMap.get("port") == null ? null : String.valueOf(dbConfMap.get("port"));
        if(serviceUrl != null && serviceUrl.contains(":")) {
            String[] split = serviceUrl.split(":");
            if(split.length != 2) {
                throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, serviceUrl + "地址错误");
            }
            serviceUrl = split[0];
            port = split[1];
        }
        String databaseName = (String) dbConfMap.get("dbaseName");
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
        // AES 解密
        if (StringUtils.isNotBlank(password)) {
            try {
                password = AESUtil.decrypt(password);
            } catch (Exception e) {
//                log.error("mysql init db config password [{}] decrypt fail!!!", password, e);
            }
        }
        if (StringUtils.isBlank(userName)
                || StringUtils.isBlank(password)
                || StringUtils.isBlank(serviceUrl)
                || StringUtils.isBlank(port)) {
            log.error("mysql init database config fail, must field info is null! bigResourceId: [{}]", conf.getBigdataResourceId());
            throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "数据库配置必填参数缺失，请校验参数配置是否正确!");
        }
        Integer initialSize = (Integer) dbConfMap.get("initialSize");
        Integer minIdle = (Integer) dbConfMap.get("minIdle");
        Integer maxActive = (Integer) dbConfMap.get("maxActive");
        Integer timeOut = (Integer) dbConfMap.get("timeOut");
        BaseMySqlDatabaseInfo mySqlDatabaseInfo = new BaseMySqlDatabaseInfo(userName, password, serviceUrl, port, databaseName);
        if (initialSize != null) {
            mySqlDatabaseInfo.setInitialSize(initialSize);
        }
        mySqlDatabaseInfo.setMinIdle(minIdle == null ? threadConfig.getCorePoolSize() : minIdle);
        mySqlDatabaseInfo.setMaxActive(maxActive == null ? threadConfig.getMaximumPoolSize() : maxActive);
        mySqlDatabaseInfo.setTimeOut(timeOut == null ? threadConfig.getTimeOut() * 1000 : timeOut * 1000);

        // 编码方式
        String characterEncoding = (String) dbConfMap.get("characterEncoding");
        if (StringUtils.isNotBlank(characterEncoding)) {
            mySqlDatabaseInfo.setCharacterEncoding(characterEncoding);
        }

        // 是否开启 SSL
        Boolean useSSL = dbConfMap.get("useSSL") == null ? null : Boolean.valueOf(String.valueOf(dbConfMap.get("useSSL")));
        if (useSSL != null) {
            mySqlDatabaseInfo.setUseSSL(useSSL);
        }

        // 是否开启 tinyInt1isBit
        Boolean tinyInt1isBit = dbConfMap.get("tinyInt1isBit") == null ? null : Boolean.valueOf(String.valueOf(dbConfMap.get("tinyInt1isBit")));
        if (tinyInt1isBit != null) {
            mySqlDatabaseInfo.setTinyInt1isBit(tinyInt1isBit);
        }

        // 时区
        Object serverTimezone = dbConfMap.get("serverTimezone");
        if (serverTimezone != null) {
            mySqlDatabaseInfo.setServerTimezone(String.valueOf(serverTimezone));
        }

        // 是否开启 tinyInt1isBit
        Boolean bit1isBoolean = dbConfMap.get("bit1isBoolean") == null ? null : Boolean.valueOf(String.valueOf(dbConfMap.get("bit1isBoolean")));
        if (bit1isBoolean != null) {
            mySqlDatabaseInfo.setBit1isBoolean(bit1isBoolean);
        } else {
            mySqlDatabaseInfo.setBit1isBoolean(true);
        }

        // 是否开启 tinyInt1isBit
        Boolean jsonToObject = dbConfMap.get("jsonToObject") == null ? null : Boolean.valueOf(String.valueOf(dbConfMap.get("jsonToObject")));
        if (jsonToObject != null) {
            mySqlDatabaseInfo.setJsonToObject(jsonToObject);
        } else {
            mySqlDatabaseInfo.setJsonToObject(false);
        }

        return (D) mySqlDatabaseInfo;
    }

    @Override
    public T initTableConfig(TableConf conf) {
        String tableName = conf.getTableName();
        if (StringUtils.isBlank(tableName)) {
            log.error("mysql init table config fail, tableName is null! dbId: [{}]", conf.getId());
            throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "数据表配置必填参数缺失，请校验参数配置是否正确!");
        }
        BaseMySqlTableInfo tableInfo = new BaseMySqlTableInfo();
        tableInfo.setTableName(tableName);
        String tableJson = conf.getTableJson();
        if (StringUtils.isNotBlank(tableJson)) {
            Map<String, Object> toMap = JsonUtil.jsonStrToMap(tableJson);
            Object engine = toMap.get("engine");
            if (engine != null) {
                tableInfo.setEngine((String) engine);
            } else {
                tableInfo.setEngine("InnoDB");
            }
        } else {
            tableInfo.setEngine("InnoDB");
        }

        return (T) tableInfo;
    }

    @Override
    public S initDbConnect(D databaseConf, T tableConf) {

        Properties prop = new Properties();
        //jdbc:mysql://%s:%s/%s?useUnicode=true&characterEncoding=%s&autoReconnect=true&autoReconnectForPools=true
        StringBuilder sb = new StringBuilder();
        sb.append("jdbc:mysql://").append(databaseConf.getServiceUrl()).append(":").append(databaseConf.getPort());

        if(StringUtils.isNotBlank(databaseConf.getDatabaseName())) {
            sb.append("/").append(databaseConf.getDatabaseName());
        }

        sb.append("?characterEncoding=").append(databaseConf.getCharacterEncoding())
                .append("&useUnicode=true&autoReconnect=true&autoReconnectForPools=true");

        String url = sb.toString();
        if (databaseConf.getUseSSL() != null) {
            url = url + "&useSSL=" + databaseConf.getUseSSL();
        }
        if (StringUtils.isNotBlank(databaseConf.getServerTimezone())) {
            url = url + "&serverTimezone=" + databaseConf.getServerTimezone();
        }
        if (databaseConf.getTinyInt1isBit() != null) {
            url = url + "&tinyInt1isBit=" + databaseConf.getTinyInt1isBit();
        }
        prop.put("url", url);
        prop.put("username", databaseConf.getUserName());
        prop.put("password", databaseConf.getPassword());
        prop.put("driverClassName", BaseMySqlDatabaseInfo.DRIVER_CLASS_NAME);
        prop.put("validationQuery", BaseMySqlDatabaseInfo.VALIDATION_QUERY);
        prop.put("initialSize", databaseConf.getInitialSize() + "");
        prop.put("minIdle", databaseConf.getMinIdle() + "");
        prop.put("maxIdle", databaseConf.getMaxActive() + "");
        prop.put("maxWait",  databaseConf.getTimeOut() + "");
        prop.put("timeBetweenEvictionRunsMillis",  databaseConf.getTimeOut() + "");
        prop.put("minEvictableIdleTimeMillis",  300000 + "");
        prop.put("maxActive", databaseConf.getMaxActive() + "");
        prop.put("poolPreparedStatements", BaseMySqlDatabaseInfo.POOL_PREPARED_STATEMENTS + "");
        prop.put("testWhileIdle", BaseMySqlDatabaseInfo.TEST_WHILE_IDLE + "");
        DataSource dataSource = null;
        try {
            dataSource = BasicDataSourceFactory.createDataSource(prop);
        } catch (Exception e) {
            throw new BusinessException(e, ExceptionCode.CREATE_DB_CONNECT_EXCEPTION);
        }
        return returnQueryRunner(databaseConf, tableConf, dataSource);
    }

    protected S returnQueryRunner(D databaseConf, T tableConf, DataSource dataSource) {
        return (S) new QueryRunner(dataSource);
    }

    @Override
    public void destroyDbConnect(String cacheKey, S connect) throws Exception {
    }
}
