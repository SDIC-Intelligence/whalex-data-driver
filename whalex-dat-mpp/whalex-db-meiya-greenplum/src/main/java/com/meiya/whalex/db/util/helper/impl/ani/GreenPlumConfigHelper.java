package com.meiya.whalex.db.util.helper.impl.ani;

import com.meiya.whalex.annotation.DbHelper;
import com.meiya.whalex.db.entity.ani.BasePostGreDatabaseInfo;
import com.meiya.whalex.db.entity.ani.BasePostGreTableInfo;
import com.meiya.whalex.exception.BusinessException;
import com.meiya.whalex.exception.ExceptionCode;
import com.meiya.whalex.interior.db.constant.CloudVendorsEnum;
import com.meiya.whalex.interior.db.constant.DbResourceEnum;
import com.meiya.whalex.interior.db.constant.DbVersionEnum;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.dbcp.BasicDataSourceFactory;
import org.apache.commons.dbutils.QueryRunner;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.Properties;

/**
 * Libra 配置管理工具
 *
 * @author 黄河森
 * @date 2019/12/23
 * @project whale-cloud-platformX
 */

@DbHelper(dbType = DbResourceEnum.greenplum, version = DbVersionEnum.POSTGRE_9_3, cloudVendors = CloudVendorsEnum.OPEN)
@Slf4j
public class GreenPlumConfigHelper extends BasePostGreConfigHelper {
    @Override
    public QueryRunner initDbConnect(BasePostGreDatabaseInfo databaseConf, BasePostGreTableInfo tableConf) {
        Properties prop = new Properties();
        prop.put("url", String.format(BasePostGreDatabaseInfo.URL_TEMPLATE,
                databaseConf.getServiceUrl(),
                databaseConf.getPort(),
                databaseConf.getDatabase()));
        prop.put("username", databaseConf.getUserName());
        prop.put("password", databaseConf.getPassword());
        prop.put("driverClassName", BasePostGreDatabaseInfo.DRIVER_CLASS_NAME);
        prop.put("initialSize", databaseConf.getInitialSize());
        prop.put("minIdle", databaseConf.getMinIdle());
        prop.put("maxActive", databaseConf.getMaxActive());
        prop.put("poolPreparedStatements", false);
        prop.put("testWhileIdle", true);
        prop.put("validationQuery", "select 1");
        DataSource dataSource = null;
        try {
            dataSource = BasicDataSourceFactory.createDataSource(prop);
            Connection connection = dataSource.getConnection();
            connection.close();
        } catch (Exception e) {
            throw new BusinessException(e, ExceptionCode.CREATE_DB_CONNECT_EXCEPTION);
        }
        return new QueryRunner(dataSource);
    }
}
