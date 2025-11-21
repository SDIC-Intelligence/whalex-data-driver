package com.meiya.whalex.db.util.param.impl.document;

import com.meiya.whalex.annotation.DbParamUtil;
import com.meiya.whalex.interior.db.constant.CloudVendorsEnum;
import com.meiya.whalex.interior.db.constant.DbResourceEnum;
import com.meiya.whalex.interior.db.constant.DbVersionEnum;
import lombok.extern.slf4j.Slf4j;

/**
 * @author 黄河森
 * @date 2022/10/25
 * @package com.meiya.whalex.db.util.param.impl.document
 * @project whalex-data-driver
 */
@DbParamUtil(dbType = DbResourceEnum.mongodb, version = DbVersionEnum.MONGO_4_0_5, cloudVendors = CloudVendorsEnum.OPEN)
@Slf4j
public class MongoParamUtil extends BaseMongoParamUtil {
}
