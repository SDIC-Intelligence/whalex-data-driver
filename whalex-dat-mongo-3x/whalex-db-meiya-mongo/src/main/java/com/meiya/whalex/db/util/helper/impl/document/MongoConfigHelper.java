package com.meiya.whalex.db.util.helper.impl.document;

import com.meiya.whalex.annotation.DbHelper;
import com.meiya.whalex.interior.db.constant.CloudVendorsEnum;
import com.meiya.whalex.interior.db.constant.DbResourceEnum;
import com.meiya.whalex.interior.db.constant.DbVersionEnum;
import lombok.extern.slf4j.Slf4j;

/**
 * @author 黄河森
 * @date 2022/10/25
 * @package com.meiya.whalex.db.util.helper.impl.document
 * @project whalex-data-driver
 */
@DbHelper(dbType = DbResourceEnum.mongodb, version = DbVersionEnum.MONGO_3_0_1, cloudVendors = CloudVendorsEnum.OPEN)
@Slf4j
public class MongoConfigHelper extends BaseMongoConfigHelper {
}
