package com.meiya.whalex.db.util.helper.impl.graph;

import cn.hutool.core.collection.CollectionUtil;
import com.meiya.whalex.annotation.DbHelper;
import com.meiya.whalex.business.entity.DatabaseConf;
import com.meiya.whalex.db.entity.AbstractCursorCache;
import com.meiya.whalex.db.entity.graph.*;
import com.meiya.whalex.interior.db.constant.CloudVendorsEnum;
import com.meiya.whalex.interior.db.constant.DbResourceEnum;
import com.meiya.whalex.interior.db.constant.DbVersionEnum;
import com.meiya.whalex.util.JsonUtil;
import org.apache.commons.lang3.StringUtils;
import java.util.Map;

/**
 * @author 黄河森
 * @date 2023/4/19
 * @package com.meiya.whalex.db.util.helper.impl.graph
 * @project whalex-data-driver
 */
@DbHelper(dbType = DbResourceEnum.janusgraph, version = DbVersionEnum.JANUSGRAPH_0_2_0, cloudVendors = CloudVendorsEnum.OPEN)
public class JanusGraphConfigHelper extends BaseGremlinConfigHelper<JanusGraphClient, JanusGraphDatabaseInfo, GremlinTableInfo, AbstractCursorCache> {

    @Override
    public JanusGraphDatabaseInfo initDbModuleConfig(DatabaseConf conf) {
        GremlinDatabaseInfo gremlinDatabaseInfo = super.initDbModuleConfig(conf);
        gremlinDatabaseInfo.setIoRegistries("org.janusgraph.graphdb.tinkerpop.JanusGraphIoRegistry");
        String connSetting = conf.getConnSetting();
        Map<String, Object> dbMap = JsonUtil.jsonStrToObject(connSetting, Map.class);
        String storageBackend = (String) dbMap.get("storageBackend");
        String indexBackend = (String) dbMap.get("indexBackend");
        String indexServerUrl = (String) dbMap.get("indexServerUrl");
        String mode = (String) dbMap.get("mode");
        if (StringUtils.isBlank(storageBackend)) {
            storageBackend = "hbase";
        }
        if (StringUtils.isBlank(mode)) {
            mode = JanusGraphDatabaseInfo.REMOTE_MODE;
        }
        if (StringUtils.isBlank(gremlinDatabaseInfo.getSerializer()) && JanusGraphDatabaseInfo.LOCAL_MODE.equalsIgnoreCase(mode)) {
            gremlinDatabaseInfo.setSerializer("org.apache.tinkerpop.gremlin.driver.ser.GryoMessageSerializerV3d0");
        }
        JanusGraphDatabaseInfo janusGraphDatabaseInfo = new JanusGraphDatabaseInfo(gremlinDatabaseInfo, storageBackend
                , indexBackend, indexServerUrl, mode);
        return janusGraphDatabaseInfo;
    }

    @Override
    public JanusGraphClient initDbConnect(JanusGraphDatabaseInfo databaseConf, GremlinTableInfo tableConf) {
        String mode = databaseConf.getMode();
        if (StringUtils.isNotBlank(mode) && StringUtils.equalsIgnoreCase(mode, JanusGraphDatabaseInfo.LOCAL_MODE)) {
            JanusGraphClient janusGraphClient = new JanusGraphClient(JanusGraphDatabaseInfo.LOCAL_MODE
                    , databaseConf.getStorageBackend()
                    , CollectionUtil.join(databaseConf.getServerUrl(), ",")
                    , databaseConf.getAlias()
                    , databaseConf.getPort()
                    , databaseConf.getIndexBackend()
                    , databaseConf.getIndexServerUrl());
            return janusGraphClient;
        } else {
            GremlinClient gremlinClient = super.initDbConnect(databaseConf, tableConf);
            return new JanusGraphClient(gremlinClient.getCluster(), gremlinClient.getClient(), gremlinClient.getKerberosUniformLogin(), JanusGraphDatabaseInfo.REMOTE_MODE);
        }
    }
}
