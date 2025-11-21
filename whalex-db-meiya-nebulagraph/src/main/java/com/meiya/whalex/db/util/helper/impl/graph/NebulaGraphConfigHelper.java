package com.meiya.whalex.db.util.helper.impl.graph;

import com.meiya.whalex.annotation.DbHelper;
import com.meiya.whalex.business.entity.DatabaseConf;
import com.meiya.whalex.business.entity.TableConf;
import com.meiya.whalex.db.entity.AbstractCursorCache;
import com.meiya.whalex.db.entity.graph.NebulaGraphDatabaseInfo;
import com.meiya.whalex.db.entity.graph.NebulaGraphTableInfo;
import com.meiya.whalex.db.util.helper.AbstractDbModuleConfigHelper;
import com.meiya.whalex.exception.BusinessException;
import com.meiya.whalex.exception.ExceptionCode;
import com.meiya.whalex.interior.db.constant.CloudVendorsEnum;
import com.meiya.whalex.interior.db.constant.DbResourceEnum;
import com.meiya.whalex.interior.db.constant.DbVersionEnum;
import com.meiya.whalex.util.JsonUtil;
import com.meiya.whalex.util.encrypt.AESUtil;
import com.vesoft.nebula.client.graph.SessionPool;
import com.vesoft.nebula.client.graph.SessionPoolConfig;
import com.vesoft.nebula.client.graph.data.HostAddress;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.apache.commons.collections.CollectionUtils.isNotEmpty;
import static org.apache.commons.lang3.StringUtils.*;
import static org.apache.commons.lang3.StringUtils.split;

/**
 * @author 黄河森
 * @date 2024/3/7
 * @package com.meiya.whalex.db.util.helper.impl.graph
 * @project whalex-data-driver
 * @description NebulaGraphConfigHelper
 */
@Slf4j
@DbHelper(dbType = DbResourceEnum.nebulagraph, version = DbVersionEnum.NEBULAGRAPH_3_8_0, cloudVendors = CloudVendorsEnum.OPEN)
public class NebulaGraphConfigHelper extends AbstractDbModuleConfigHelper<SessionPool, NebulaGraphDatabaseInfo, NebulaGraphTableInfo, AbstractCursorCache> {
    @Override
    public boolean checkDataSourceStatus(SessionPool connect) throws Exception {
        return !connect.isClosed();
    }

    @Override
    public NebulaGraphDatabaseInfo initDbModuleConfig(DatabaseConf conf) {
        try {
            String connSetting = conf.getConnSetting();
            if (isBlank(connSetting)) {
                log.warn("获取NebulaGraph组件数据库部署定义为空，bigResourceId：{}", conf.getBigdataResourceId());
                throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "数据库配置必填参数缺失，请校验参数配置是否正确!");
            }
            Map<String, Object> dbMap = JsonUtil.jsonStrToObject(connSetting, Map.class);
            List<String> serverUrls = new ArrayList<>();
            String serviceUrl = (String) dbMap.get("serviceUrl");
            if (isBlank(serviceUrl)) {
                log.warn("获取NebulaGraph组件数据库部署定义地址或端口为空，bigResourceId：{}", conf.getBigdataResourceId());
                throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "数据库配置必填参数缺失，请校验参数配置是否正确!");
            }
            List<String> serverSet = convertService(serviceUrl);
            if (isNotEmpty(serverSet)) {
                serverUrls.addAll(serverSet);
            }
            NebulaGraphDatabaseInfo nebulaGraphDatabaseInfo = new NebulaGraphDatabaseInfo();
            nebulaGraphDatabaseInfo.setServerUrl(serverSet);
            nebulaGraphDatabaseInfo.setUsername((String) dbMap.get("username"));
            String password = (String) dbMap.get("password");
            if (StringUtils.isNotBlank(password)) {
                try {
                    password = AESUtil.decrypt(password);
                } catch (Exception e) {
                }
            }
            nebulaGraphDatabaseInfo.setPassword(password);
            nebulaGraphDatabaseInfo.setSpaceName((String) dbMap.get("spaceName"));
            return nebulaGraphDatabaseInfo;
        } catch (Exception e) {
            log.error("NebulaGraph 数据库配置加载失败! bigResourceId：{}", conf.getBigdataResourceId());
            throw new BusinessException(e, ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "数据库配置解析异常，请校验参数配置是否正确!");
        }
    }

    @Override
    public NebulaGraphTableInfo initTableConfig(TableConf conf) {
        NebulaGraphTableInfo nebulaGraphTableInfo = new NebulaGraphTableInfo();
        nebulaGraphTableInfo.setTableName(conf.getTableName());
        return nebulaGraphTableInfo;
    }

    @Override
    public SessionPool initDbConnect(NebulaGraphDatabaseInfo databaseConf, NebulaGraphTableInfo tableConf) {
        List<String> serverUrl = databaseConf.getServerUrl();
        List<HostAddress> addresses = new ArrayList<>(serverUrl.size());
        for (String url : serverUrl) {
            String[] split = split(url, ",");
            for (String address : split) {
                String[] _split = split(address, ":");
                HostAddress hostAddress = new HostAddress(_split[0], Integer.parseInt(_split[1]));
                addresses.add(hostAddress);
            }
        }

        SessionPoolConfig sessionPoolConfig = new SessionPoolConfig(addresses, databaseConf.getSpaceName(), databaseConf.getUsername(), databaseConf.getPassword())
                .setMaxSessionSize(this.threadConfig.getMaximumPoolSize())
                .setMinSessionSize(this.threadConfig.getCorePoolSize())
                .setWaitTime(this.threadConfig.getTimeOut())
                .setRetryTimes(3)
                .setIntervalTime(100);
        SessionPool sessionPool = new SessionPool(sessionPoolConfig);
        if (!sessionPool.init()) {
            throw new BusinessException(ExceptionCode.CREATE_DB_CONNECT_EXCEPTION);
        }
        return sessionPool;
    }

    @Override
    public void destroyDbConnect(String cacheKey, SessionPool connect) throws Exception {
        connect.close();
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
