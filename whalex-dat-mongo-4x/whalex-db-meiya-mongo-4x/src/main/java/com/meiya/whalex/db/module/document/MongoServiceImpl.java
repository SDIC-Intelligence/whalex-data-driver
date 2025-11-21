package com.meiya.whalex.db.module.document;

import com.meiya.whalex.annotation.DbService;
import com.meiya.whalex.interior.db.constant.CloudVendorsEnum;
import com.meiya.whalex.interior.db.constant.DbResourceEnum;
import com.meiya.whalex.interior.db.constant.DbVersionEnum;
import lombok.extern.slf4j.Slf4j;

/**
 * @author 黄河森
 * @date 2022/10/25
 * @package com.meiya.whalex.db.module.document
 * @project whalex-data-driver
 */
@DbService(dbType = DbResourceEnum.mongodb, version = DbVersionEnum.MONGO_4_0_5, cloudVendors = CloudVendorsEnum.OPEN)
@Slf4j
public class MongoServiceImpl extends BaseMongoServiceImpl {
}
