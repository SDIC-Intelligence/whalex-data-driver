package com.meiya.whalex.db.util.helper.impl.graph;

import com.meiya.whalex.annotation.DbHelper;
import com.meiya.whalex.business.entity.DatabaseConf;
import com.meiya.whalex.business.entity.TableConf;
import com.meiya.whalex.db.entity.AbstractCursorCache;
import com.meiya.whalex.db.entity.graph.Neo4jDatabaseInfo;
import com.meiya.whalex.db.entity.graph.Neo4jTableInfo;
import com.meiya.whalex.db.util.helper.AbstractDbModuleConfigHelper;
import com.meiya.whalex.exception.BusinessException;
import com.meiya.whalex.exception.ExceptionCode;
import com.meiya.whalex.interior.db.constant.CloudVendorsEnum;
import com.meiya.whalex.interior.db.constant.DbResourceEnum;
import com.meiya.whalex.interior.db.constant.DbVersionEnum;
import com.meiya.whalex.util.JsonUtil;
import com.meiya.whalex.util.encrypt.AESUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.driver.*;

import static org.apache.commons.lang3.StringUtils.*;
import static org.apache.commons.collections.CollectionUtils.isNotEmpty;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author chenjp
 * @date 2020/9/27
 */
@Slf4j
@DbHelper(dbType = DbResourceEnum.neo4j, version = DbVersionEnum.NEO4J_4_4_11, cloudVendors = CloudVendorsEnum.OPEN)
public class Neo4jConfigHelper extends AbstractDbModuleConfigHelper<Driver, Neo4jDatabaseInfo, Neo4jTableInfo, AbstractCursorCache> {

    private static final String BOLT = "bolt://";

    @Override
    public boolean checkDataSourceStatus(Driver connect) throws Exception {
        Session session = connect.session();
        boolean open = session.isOpen();
        session.close();
        return open;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Neo4jDatabaseInfo initDbModuleConfig(DatabaseConf conf) {
        try {
            String connSetting = conf.getConnSetting();
            if (isBlank(connSetting)) {
                log.warn("获取Neo4j组件数据库部署定义为空，bigResourceId：{}", conf.getBigdataResourceId());
                throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "数据库配置必填参数缺失，请校验参数配置是否正确!");
            }
            Map<String, Object> dbMap = JsonUtil.jsonStrToObject(connSetting, Map.class);
            List<String> serverUrls = new ArrayList<>();
            String serviceUrl = (String) dbMap.get("serviceUrl");
            if (isBlank(serviceUrl)) {
                log.warn("获取Neo4j组件数据库部署定义地址或端口为空，bigResourceId：{}", conf.getBigdataResourceId());
                throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "数据库配置必填参数缺失，请校验参数配置是否正确!");
            }
            List<String> serverSet = convertService(serviceUrl);
            if (isNotEmpty(serverSet)) {
                serverUrls.addAll(serverSet);
            }
            return getDatabaseInfo(dbMap, serverUrls);
        } catch (Exception e) {
            log.error("Neo4j 数据库配置加载失败! bigResourceId：{}", conf.getBigdataResourceId());
            throw new BusinessException(e, ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "数据库配置解析异常，请校验参数配置是否正确!");
        }
    }

    @Override
    public Neo4jTableInfo initTableConfig(TableConf conf) {
        Neo4jTableInfo neo4jTableInfo = new Neo4jTableInfo();
        neo4jTableInfo.setTableName(conf.getTableName());
        return neo4jTableInfo;
    }

    @Override
    public Driver initDbConnect(Neo4jDatabaseInfo databaseConf, Neo4jTableInfo tableConf) {
        List<String> serverUrl = databaseConf.getServerUrl();
        String uri = isNotEmpty(serverUrl) && serverUrl.size() == 1 ?
                serverUrl.get(0) : join(serverUrl, ",");

        Config config = Config.builder()
                //  允许由连接池管理的每个主机的最大连接总数
                .withMaxConnectionPoolSize(threadConfig.getMaximumPoolSize())
                //  会话从连接池请求时等待的最长时间
                .withConnectionAcquisitionTimeout(2, TimeUnit.MINUTES)
                //  托管事务在失败之前重试的最长时间
                .withMaxTransactionRetryTime(10L, TimeUnit.SECONDS)
                //  从池中删除之前，驱动程序将保持连接的最长时间
                .withMaxConnectionLifetime(30, TimeUnit.SECONDS)
                //  等待建立TCP连接的最长时间
                .withConnectionTimeout(threadConfig.getTimeOut(), TimeUnit.SECONDS)
                .build();

        Driver driver = GraphDatabase.driver(BOLT + uri, AuthTokens.basic(databaseConf.getUsername(), databaseConf.getPassword()), config);
        return driver;
    }

    @Override
    public void destroyDbConnect(String cacheKey, Driver connect) throws Exception {
        connect.close();
    }

    private Neo4jDatabaseInfo getDatabaseInfo(Map<String, Object> dbMap, List<String> serverList) {
        Neo4jDatabaseInfo neo4jDatabaseInfo = new Neo4jDatabaseInfo();
        neo4jDatabaseInfo.setServerUrl(serverList);
        neo4jDatabaseInfo.setUsername((String) dbMap.get("username"));
        String password = (String) dbMap.get("password");
        if (StringUtils.isNotBlank(password)) {
            try {
                password = AESUtil.decrypt(password);
            } catch (Exception e) {
            }
        }
        neo4jDatabaseInfo.setPassword(password);
        return neo4jDatabaseInfo;
    }

    /**
     * 转换serviceUrl
     */
    private List<String> convertService(String serviceUrl) {
        List<String> serverSet = new ArrayList<>();
        String isRange = substringBetween(serviceUrl, "(", ")");
        if (isNotBlank(isRange) || contains(serviceUrl, ",")) {
            String[] servers = split(serviceUrl, ",");
            serverSet.addAll(Arrays.asList(servers));
        } else {
            serverSet.add(serviceUrl);
        }
        return serverSet;
    }
}
