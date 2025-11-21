package com.meiya.whalex.db.util.helper.impl.ani;

import cn.hutool.crypto.digest.DigestUtil;
import com.meiya.whalex.annotation.DbHelper;
import com.meiya.whalex.business.entity.DatabaseConf;
import com.meiya.whalex.db.entity.ani.BasePostGreTableInfo;
import com.meiya.whalex.db.entity.ani.KingBaseEsDatabaseInfo;
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

@DbHelper(dbType = DbResourceEnum.kingbasees, version = DbVersionEnum.KINGBASEES_8_6_0, cloudVendors = CloudVendorsEnum.KingBase)
@Slf4j
public class KingBaseEsConfigHelper extends BasePostGreConfigHelper<QueryRunner
        , KingBaseEsDatabaseInfo, BasePostGreTableInfo, RdbmsCursorCache> {

    public static final String KINGBASE_URL_TEMPLATE = "jdbc:kingbase8://%s:%s/%s";

    public static final String KINGBASE_DRIVER_CLASS_NAME = "com.kingbase8.Driver";

    public static final String KINGBASE_SLAVE = "USEDISPATCH=true&SLAVE_ADD=%s&SLAVE_PORT=%s&nodeList=%s\n";

    @Override
    public KingBaseEsDatabaseInfo initDbModuleConfig(DatabaseConf conf) {
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

        // 读节点配置
        String slaveUrl = dbConfMap.get("slaveUrl");
        // 节点列表
        String nodeList = dbConfMap.get("nodeList");

        String slaveAddr = null;
        String slavePort = null;

        // 判断是否读写分离
        if (StringUtils.isNotBlank(slaveUrl)) {
            String[] split = StringUtils.split(slaveUrl, ",");
            StringBuilder urlSb = new StringBuilder();
            StringBuilder portSb = new StringBuilder();

            for (String str : split) {
                String[] urlNode = StringUtils.split(str, ":");
                if (urlNode.length != 2) {
                    throw new BusinessException("KingBaseES 组件读节点配置异常，请检查配置是否正确!");
                }
                urlSb.append(urlNode[0]).append(",");
                portSb.append(urlNode[1]).append(",");
            }
            if (urlSb.length() > 0) {
                slaveAddr = urlSb.toString();
            }
            if (portSb.length() > 0) {
                slavePort = portSb.toString();
            }
        }
        KingBaseEsDatabaseInfo kingBaseEsDatabaseInfo = new KingBaseEsDatabaseInfo(userName, password, serviceUrl, port, database, schema, ignoreCase, slaveAddr, slavePort, nodeList);
        kingBaseEsDatabaseInfo.setMaxActive(threadConfig.getMaximumPoolSize());

        Boolean tinyInt1isBit = dbConfMap.get("tinyInt1isBit") == null ? true : Boolean.valueOf(String.valueOf(dbConfMap.get("tinyInt1isBit")));
        kingBaseEsDatabaseInfo.setTinyInt1isBit(tinyInt1isBit);
        return kingBaseEsDatabaseInfo;
    }

    @Override
    public QueryRunner initDbConnect(KingBaseEsDatabaseInfo databaseConf, BasePostGreTableInfo tableConf) {
        HikariConfig hikariConfig = new HikariConfig();
        String url = String.format(KINGBASE_URL_TEMPLATE,
                databaseConf.getServiceUrl(),
                databaseConf.getPort(),
                databaseConf.getDatabase());

        if (StringUtils.isNotBlank(databaseConf.getSlaveUrl()) && StringUtils.isNotBlank(databaseConf.getSlavePort())) {
            String slaveConfig = String.format(KINGBASE_SLAVE, databaseConf.getSlaveUrl(), databaseConf.getSlavePort(), databaseConf.getNodeList());
            url = url + "?" + slaveConfig;
        }

        hikariConfig.setJdbcUrl(url);
        hikariConfig.setUsername(databaseConf.getUserName());
        hikariConfig.setPassword(databaseConf.getPassword());
        hikariConfig.setDriverClassName(KINGBASE_DRIVER_CLASS_NAME);
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
