package com.meiya.whalex.db.util.helper.impl.ani;

import cn.hutool.crypto.digest.DigestUtil;
import com.meiya.whalex.annotation.DbHelper;
import com.meiya.whalex.db.entity.ani.BasePostGreDatabaseInfo;
import com.meiya.whalex.db.entity.ani.BasePostGreTableInfo;
import com.meiya.whalex.exception.BusinessException;
import com.meiya.whalex.exception.ExceptionCode;
import com.meiya.whalex.interior.db.constant.CloudVendorsEnum;
import com.meiya.whalex.interior.db.constant.DbResourceEnum;
import com.meiya.whalex.interior.db.constant.DbVersionEnum;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.dbutils.QueryRunner;

/**
 * Gauss 配置管理工具
 *
 * @author 黄河森
 * @date 2019/12/23
 * @project whale-cloud-platformX
 */

@DbHelper(dbType = DbResourceEnum.vertica, version = DbVersionEnum.VERTICA_10_0_1, cloudVendors = CloudVendorsEnum.H3C)
@Slf4j
public class VerticaConfigHelper extends BasePostGreConfigHelper {

    public static final String VERTICA_URL_TEMPLATE = "jdbc:vertica://%s:%s/%s";

    public static final String VERTICA_DRIVER_CLASS_NAME = "com.vertica.jdbc.Driver";

    @Override
    public QueryRunner initDbConnect(BasePostGreDatabaseInfo databaseConf, BasePostGreTableInfo tableConf) {
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(String.format(VERTICA_URL_TEMPLATE,
                databaseConf.getServiceUrl(),
                databaseConf.getPort(),
                databaseConf.getDatabase()));
        hikariConfig.setUsername(databaseConf.getUserName());
        hikariConfig.setPassword(databaseConf.getPassword());
        hikariConfig.setDriverClassName(VERTICA_DRIVER_CLASS_NAME);
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
