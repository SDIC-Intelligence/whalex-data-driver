package com.meiya.whalex.db.util.helper.impl.ani;

import com.meiya.whalex.annotation.DbHelper;
import com.meiya.whalex.db.entity.ani.BaseMySqlDatabaseInfo;
import com.meiya.whalex.db.entity.ani.BaseMySqlTableInfo;
import com.meiya.whalex.exception.BusinessException;
import com.meiya.whalex.exception.ExceptionCode;
import com.meiya.whalex.interior.db.constant.CloudVendorsEnum;
import com.meiya.whalex.interior.db.constant.DbResourceEnum;
import com.meiya.whalex.interior.db.constant.DbVersionEnum;
import com.meiya.whalex.db.ani.GBaseDatabaseInfo;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.dbcp2.BasicDataSourceFactory;
import org.apache.commons.dbutils.QueryRunner;

import javax.sql.DataSource;
import java.util.Properties;

/**
 * GBase 配置管理工具
 *
 * @author 蔡荣桂
 * @date 2022/05/23
 * @project whale-cloud-platformX
 */

@DbHelper(dbType = DbResourceEnum.gbase, version = DbVersionEnum.GBASE_8_6_2_43, cloudVendors = CloudVendorsEnum.OPEN)
@Slf4j
public class GBaseConfigHelper extends BaseMySqlConfigHelper {

    @Override
    public QueryRunner initDbConnect(BaseMySqlDatabaseInfo databaseConf, BaseMySqlTableInfo tableConf) {
        Properties prop = new Properties();
        String url = String.format(GBaseDatabaseInfo.URL_TEMPLATE,
                databaseConf.getServiceUrl(),
                databaseConf.getPort(),
                databaseConf.getDatabaseName(),
                databaseConf.getCharacterEncoding());
        if (databaseConf.getUseSSL() != null) {
            url = url + "&useSSL=" + databaseConf.getUseSSL();
        }
        prop.put("url", url);
        prop.put("username", databaseConf.getUserName());
        prop.put("password", databaseConf.getPassword());
        prop.put("driverClassName", GBaseDatabaseInfo.DRIVER_CLASS_NAME);
        prop.put("validationQuery", BaseMySqlDatabaseInfo.VALIDATION_QUERY);
        prop.put("initialSize", databaseConf.getInitialSize());
        prop.put("minIdle", databaseConf.getMinIdle());
        prop.put("maxIdle", databaseConf.getMaxActive());
        prop.put("maxWait",  threadConfig.getTimeOut() * 1000);
        prop.put("timeBetWeenEvictionRunsMillis",  threadConfig.getTimeOut() * 1000);
        prop.put("minEvictableIdleTimeMillis",  300000);
        prop.put("maxActive", databaseConf.getMaxActive());
        prop.put("poolPreparedStatements", BaseMySqlDatabaseInfo.POOL_PREPARED_STATEMENTS);
        prop.put("testWhileIdle", BaseMySqlDatabaseInfo.TEST_WHILE_IDLE);
        DataSource dataSource = null;
        try {
            dataSource = BasicDataSourceFactory.createDataSource(prop);
        } catch (Exception e) {
            throw new BusinessException(e, ExceptionCode.CREATE_DB_CONNECT_EXCEPTION);
        }
        return new QueryRunner(dataSource);
    }
}
