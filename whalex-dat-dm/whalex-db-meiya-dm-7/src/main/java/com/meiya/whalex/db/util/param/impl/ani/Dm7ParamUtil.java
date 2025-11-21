package com.meiya.whalex.db.util.param.impl.ani;

import com.meiya.whalex.annotation.DbParamUtil;
import com.meiya.whalex.db.entity.ani.AniHandler;
import com.meiya.whalex.db.entity.ani.BaseDmDatabaseInfo;
import com.meiya.whalex.db.entity.ani.BaseDmTableInfo;
import com.meiya.whalex.interior.db.constant.CloudVendorsEnum;
import com.meiya.whalex.interior.db.constant.DbResourceEnum;
import com.meiya.whalex.interior.db.constant.DbVersionEnum;
import lombok.extern.slf4j.Slf4j;

/**
 * @author 黄河森
 * @date 2022/5/19
 * @package com.meiya.whalex.db.util.helper.impl.ani
 * @project whalex-data-driver
 */
@DbParamUtil(dbType = DbResourceEnum.dm, version = DbVersionEnum.DM_7, cloudVendors = CloudVendorsEnum.OPEN)
@Slf4j
public class Dm7ParamUtil extends BaseDmParamUtil<AniHandler, BaseDmDatabaseInfo, BaseDmTableInfo> {
}
