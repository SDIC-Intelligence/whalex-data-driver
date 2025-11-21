package com.meiya.whalex.db.util.param.impl.stream;

import com.meiya.whalex.annotation.DbParamUtil;
import com.meiya.whalex.interior.db.constant.CloudVendorsEnum;
import com.meiya.whalex.interior.db.constant.DbResourceEnum;
import com.meiya.whalex.interior.db.constant.DbVersionEnum;

/**
 * @author 黄河森
 * @date 2020/12/21
 * @project whalex-data-driver
 */
@DbParamUtil(dbType = DbResourceEnum.kafka, version = DbVersionEnum.KAFKA_1_1_0, cloudVendors = CloudVendorsEnum.OPEN)
public class KafkaParamUtil extends BaseKafkaParamUtil {
}
