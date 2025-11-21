package com.meiya.whalex.db.util.helper.impl.stream;

import com.meiya.whalex.annotation.DbHelper;
import com.meiya.whalex.interior.db.constant.CloudVendorsEnum;
import com.meiya.whalex.interior.db.constant.DbResourceEnum;
import com.meiya.whalex.interior.db.constant.DbVersionEnum;

/**
 * @author 黄河森
 * @date 2020/12/21
 * @project whalex-data-driver
 */
@DbHelper(dbType = DbResourceEnum.kafka, version = DbVersionEnum.KAFKA_1_1_0, cloudVendors = CloudVendorsEnum.OPEN, needCheckDataSource = false)
public class KafkaConfigHelper extends BaseKafkaConfigHelper {
}
