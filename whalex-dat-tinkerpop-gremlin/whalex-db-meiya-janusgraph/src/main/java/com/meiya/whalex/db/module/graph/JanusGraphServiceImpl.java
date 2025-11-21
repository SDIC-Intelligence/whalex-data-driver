package com.meiya.whalex.db.module.graph;

import com.meiya.whalex.annotation.DbService;
import com.meiya.whalex.db.entity.AbstractCursorCache;
import com.meiya.whalex.db.entity.graph.*;
import com.meiya.whalex.interior.db.constant.CloudVendorsEnum;
import com.meiya.whalex.interior.db.constant.DbResourceEnum;
import com.meiya.whalex.interior.db.constant.DbVersionEnum;

/**
 * @author 黄河森
 * @date 2023/4/19
 * @package com.meiya.whalex.db.module.graph
 * @project whalex-data-driver
 */
@DbService(dbType = DbResourceEnum.janusgraph, version = DbVersionEnum.JANUSGRAPH_0_2_0, cloudVendors = CloudVendorsEnum.OPEN)
public class JanusGraphServiceImpl extends BaseGremlinServiceImpl<JanusGraphClient, GremlinHandler, GremlinDatabaseInfo, GremlinTableInfo, AbstractCursorCache> {
}
