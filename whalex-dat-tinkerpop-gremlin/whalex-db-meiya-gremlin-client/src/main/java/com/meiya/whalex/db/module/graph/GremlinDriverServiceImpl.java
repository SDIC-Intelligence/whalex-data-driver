package com.meiya.whalex.db.module.graph;

import com.meiya.whalex.annotation.DbService;
import com.meiya.whalex.db.entity.AbstractCursorCache;
import com.meiya.whalex.db.entity.graph.*;
import com.meiya.whalex.interior.db.constant.CloudVendorsEnum;
import com.meiya.whalex.interior.db.constant.DbResourceEnum;
import com.meiya.whalex.interior.db.constant.DbVersionEnum;

/**
 * @author 黄河森
 * @date 2025/7/2
 * @package com.meiya.whalex.db.module.graph
 * @project whalex-data-driver-2.4.1
 * @description GremlinDriverServiceImpl
 */
@DbService(dbType = DbResourceEnum.gremlin, version = DbVersionEnum.GREMLIN_3_7_3, cloudVendors = CloudVendorsEnum.OPEN)
public class GremlinDriverServiceImpl extends BaseGremlinServiceImpl<GremlinDriverClient, GremlinHandler, GremlinDatabaseInfo, GremlinTableInfo, AbstractCursorCache>{
}
