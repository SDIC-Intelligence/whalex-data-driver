package com.meiya.whalex.db.util.param.impl.lucene;

import com.meiya.whalex.annotation.DbParamUtil;
import com.meiya.whalex.interior.db.constant.CloudVendorsEnum;
import com.meiya.whalex.interior.db.constant.DbResourceEnum;
import com.meiya.whalex.interior.db.constant.DbVersionEnum;
import lombok.extern.slf4j.Slf4j;

/**
 * @author 黄河森
 * @date 2020/10/3
 * @project whalex-data-driver
 */
@DbParamUtil(cloudVendors = CloudVendorsEnum.OPEN, dbType = DbResourceEnum.es, version = DbVersionEnum.ES_6)
@Slf4j
public class EsParamUtil extends BaseEsParamUtil {
}
