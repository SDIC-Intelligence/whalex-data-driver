package com.meiya.whalex.graph.driver;


import com.meiya.whalex.db.entity.DatabaseSetting;
import com.meiya.whalex.starter.InitDbModuleStartupRunner;

/**
 * 图数据库集群对象
 *
 * 用于创建实际的 DAT 图数据库环境
 *
 * @author 黄河森
 * @date 2023/2/9
 * @package com.meiya.whalex.graph.driver
 * @project whalex-data-driver
 */
public class GraphCluster {

    private DatabaseSetting conf;

    private GraphCluster(DatabaseSetting conf) {
        this.conf = conf;
    }

    public static GraphCluster open(final DatabaseSetting conf) {
        return new GraphCluster(conf);
    }

    /**
     * 初始化环境
     *
     * @return
     * @throws Exception
     */
    public GraphClient connect() throws Exception {
        InitDbModuleStartupRunner.init();
        return new GraphClient(conf);
    }

}
