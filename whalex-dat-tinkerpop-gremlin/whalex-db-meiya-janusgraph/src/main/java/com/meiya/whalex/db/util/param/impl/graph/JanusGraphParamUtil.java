package com.meiya.whalex.db.util.param.impl.graph;

import com.meiya.whalex.annotation.DbParamUtil;
import com.meiya.whalex.interior.db.constant.CloudVendorsEnum;
import com.meiya.whalex.interior.db.constant.DbResourceEnum;
import com.meiya.whalex.interior.db.constant.DbVersionEnum;

/**
 * @author 黄河森
 * @date 2023/4/19
 * @package com.meiya.whalex.db.util.param.impl.graph
 * @project whalex-data-driver
 */
@DbParamUtil(dbType = DbResourceEnum.janusgraph, version = DbVersionEnum.JANUSGRAPH_0_2_0, cloudVendors = CloudVendorsEnum.OPEN)
public class JanusGraphParamUtil extends BaseGraphParamUtil {
}
