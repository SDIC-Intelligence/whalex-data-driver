package com.meiya.whalex.db.util.helper.impl.lucene;

import com.meiya.whalex.annotation.DbHelper;
import com.meiya.whalex.interior.db.constant.CloudVendorsEnum;
import com.meiya.whalex.interior.db.constant.DbResourceEnum;
import com.meiya.whalex.interior.db.constant.DbVersionEnum;
import lombok.extern.slf4j.Slf4j;

/**
 * @author 黄河森
 * @date 2020/10/3
 * @project whalex-data-driver
 */
@DbHelper(dbType = DbResourceEnum.es, version = DbVersionEnum.ES_7, cloudVendors = CloudVendorsEnum.OPEN)
@Slf4j
public class EsConfigHelper extends BaseEsConfigHelper {
}
