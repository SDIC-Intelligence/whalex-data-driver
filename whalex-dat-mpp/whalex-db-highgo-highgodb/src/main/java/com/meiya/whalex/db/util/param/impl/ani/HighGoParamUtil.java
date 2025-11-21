package com.meiya.whalex.db.util.param.impl.ani;

import com.meiya.whalex.annotation.DbParamUtil;
import com.meiya.whalex.db.entity.ani.AniHandler;
import com.meiya.whalex.db.entity.ani.BasePostGreTableInfo;
import com.meiya.whalex.db.entity.ani.HighGoDatabaseInfo;
import com.meiya.whalex.interior.db.constant.CloudVendorsEnum;
import com.meiya.whalex.interior.db.constant.DbResourceEnum;
import com.meiya.whalex.interior.db.constant.DbVersionEnum;
import lombok.extern.slf4j.Slf4j;

/**
 * Gauss 参数转换工具类
 *
 * @author 黄河森
 * @date 2019/12/23
 * @project whale-cloud-platformX
 */
@DbParamUtil(dbType = DbResourceEnum.highgo, version = DbVersionEnum.HIGHGO_1_2, cloudVendors = CloudVendorsEnum.HighGo)
@Slf4j
public class HighGoParamUtil extends BasePostGreParamUtil<AniHandler, HighGoDatabaseInfo, BasePostGreTableInfo> {

}
