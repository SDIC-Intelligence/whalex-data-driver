package com.meiya.whalex.db.util.helper.impl.ani;

import com.meiya.whalex.business.entity.DatabaseConf;
import com.meiya.whalex.business.entity.TableConf;
import com.meiya.whalex.db.entity.ani.BaseDmDatabaseInfo;
import com.meiya.whalex.db.entity.ani.BaseDmTableInfo;
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
 * Dm 配置管理工具
 *
 * @author 蔡荣桂
 * @date 2021/4/14
 * @project whale-cloud-platformX
 */

@Slf4j
public class BaseDmConfigHelper<S extends QueryRunner, D extends BaseDmDatabaseInfo, T extends BaseDmTableInfo, C extends RdbmsCursorCache>
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
        String port = (String) dbConfMap.get("port");
        String schema = (String) dbConfMap.get("schema");
        boolean ignoreCase = dbConfMap.get("ignoreCase") == null ? true : Boolean.parseBoolean(String.valueOf(dbConfMap.get("ignoreCase")));
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
            }
        }
        if (StringUtils.isBlank(userName)
                || StringUtils.isBlank(password)
                || StringUtils.isBlank(serviceUrl)
                || StringUtils.isBlank(port)) {
            log.error("Dm init database config fail, must field info is null! bigResourceId: [{}]", conf.getBigdataResourceId());
            throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "数据库配置必填参数缺失，请校验参数配置是否正确!");
        }
        BaseDmDatabaseInfo DmDatabaseInfo = new BaseDmDatabaseInfo(userName, password, serviceUrl, port, databaseName, schema, ignoreCase);
        DmDatabaseInfo.setMaxActive(threadConfig.getMaximumPoolSize());
        return (D) DmDatabaseInfo;
    }

    @Override
    public T initTableConfig(TableConf conf) {
        String tableName = conf.getTableName();
        if (StringUtils.isBlank(tableName)) {
            log.error("Dm init table config fail, tableName is null! dbId: [{}]", conf.getId());
            throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "数据表配置必填参数缺失，请校验参数配置是否正确!");
        }
        BaseDmTableInfo tableInfo = new BaseDmTableInfo();
        tableInfo.setTableName(tableName);
        String tableJson = conf.getTableJson();
        if (StringUtils.isNotBlank(tableJson)) {
            Map<String, Object> config = JsonUtil.jsonStrToMap(tableJson);
            Object ignoreCase = config.get("ignoreCase");
            if (ignoreCase != null) {
                tableInfo.setIgnoreCase(Boolean.parseBoolean(String.valueOf(ignoreCase)));
            }
        }
        return (T) tableInfo;
    }

    @Override
    public S initDbConnect(BaseDmDatabaseInfo databaseConf, BaseDmTableInfo tableConf) {
        Properties prop = new Properties();
        //jdbc:dm://%s:%s/%s
        StringBuilder sb = new StringBuilder();
        sb.append("jdbc:dm://").append(databaseConf.getServiceUrl()).append(":").append(databaseConf.getPort());
        if(StringUtils.isNotBlank(databaseConf.getDatabaseName())) {
            sb.append("/").append(databaseConf.getDatabaseName());
        }
        prop.put("url", sb.toString());
        prop.put("username", databaseConf.getUserName());
        prop.put("password", databaseConf.getPassword());
        prop.put("driverClassName", BaseDmDatabaseInfo.DRIVER_CLASS_NAME);
        prop.put("validationQuery", BaseDmDatabaseInfo.VALIDATION_QUERY);
        prop.put("initialSize", databaseConf.getInitialSize());
        prop.put("minIdle", databaseConf.getMinIdle());
        prop.put("maxIdle", databaseConf.getMaxActive());
        prop.put("maxWait",  threadConfig.getTimeOut() * 1000);
        prop.put("timeBetWeenEvictionRunsMillis",  threadConfig.getTimeOut() * 1000);
        prop.put("minEvictableIdleTimeMillis",  300000);
        prop.put("maxActive", databaseConf.getMaxActive());
        prop.put("poolPreparedStatements", BaseDmDatabaseInfo.POOL_PREPARED_STATEMENTS);
        prop.put("testWhileIdle", BaseDmDatabaseInfo.TEST_WHILE_IDLE);
        DataSource dataSource = null;
        try {
            dataSource = BasicDataSourceFactory.createDataSource(prop);
        } catch (Exception e) {
            throw new BusinessException(e, ExceptionCode.CREATE_DB_CONNECT_EXCEPTION);
        }
        return (S) new QueryRunner(dataSource);
    }

    @Override
    public void destroyDbConnect(String cacheKey, S connect) throws Exception {
    }
}
