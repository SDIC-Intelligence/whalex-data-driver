package com.meiya.whalex.db.util.param.impl.graph;

import com.meiya.whalex.annotation.DbParamUtil;
import com.meiya.whalex.interior.db.constant.CloudVendorsEnum;
import com.meiya.whalex.interior.db.constant.DbResourceEnum;
import com.meiya.whalex.interior.db.constant.DbVersionEnum;

/**
 * @author 黄河森
 * @date 2025/7/2
 * @package com.meiya.whalex.db.util.param.impl.graph
 * @project whalex-data-driver-2.4.1
 * @description GremlinDriverParamUtil
 */
@DbParamUtil(dbType = DbResourceEnum.gremlin, version = DbVersionEnum.GREMLIN_3_7_3, cloudVendors = CloudVendorsEnum.OPEN)
public class GremlinDriverParamUtil extends BaseGraphParamUtil {
}
