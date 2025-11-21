package com.meiya.whalex.db.util.param.impl.ani;

import com.meiya.whalex.annotation.DbParamUtil;
import com.meiya.whalex.interior.db.constant.CloudVendorsEnum;
import com.meiya.whalex.interior.db.constant.DbResourceEnum;
import com.meiya.whalex.interior.db.constant.DbVersionEnum;

/**
 * MySql 参数转换工具类
 *
 * @author 黄河森
 * @date 2019/12/23
 * @project whale-cloud-platformX
 */
@DbParamUtil(dbType = DbResourceEnum.doris, version = DbVersionEnum.DORIS_0_14, cloudVendors = CloudVendorsEnum.OPEN)
public class DorisParamUtil extends BaseDorisParamUtil {
}
