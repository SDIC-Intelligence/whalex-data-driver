package com.meiya.whalex.db.entity.ani;

import lombok.Data;

/**
 * @author 黄河森
 * @date 2022/11/24
 * @package com.meiya.whalex.db.entity.ani
 * @project whalex-data-driver
 */
@Data
public class KingBaseEsDatabaseInfo extends BasePostGreDatabaseInfo {

    private String slaveUrl;

    private String slavePort;

    private String nodeList;

    public KingBaseEsDatabaseInfo() {
        super();
    }

    public KingBaseEsDatabaseInfo(String userName, String password, String serviceUrl, String port, String database, String schema, boolean ignoreCase, String slaveUrl, String slavePort, String nodeList) {
        super(userName, password, serviceUrl, port, database, schema, ignoreCase);
        this.slaveUrl = slaveUrl;
        this.slavePort = slavePort;
        this.nodeList = nodeList;
    }
}
