package com.meiya.whalex.db.entity.ani;

import lombok.Data;

/**
 * @author 黄河森
 * @date 2022/11/24
 * @package com.meiya.whalex.db.entity.ani
 * @project whalex-data-driver
 */
@Data
public class HighGoDatabaseInfo extends BasePostGreDatabaseInfo {

    public HighGoDatabaseInfo() {
        super();
    }

    public HighGoDatabaseInfo(String userName, String password, String serviceUrl, String port, String database, String schema, boolean ignoreCase) {
        super(userName, password, serviceUrl, port, database, schema, ignoreCase);
    }
}
