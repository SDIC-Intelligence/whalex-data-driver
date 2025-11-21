package com.meiya.whalex.db.util.helper.impl.ani;

import com.meiya.whalex.annotation.DbHelper;
import com.meiya.whalex.interior.db.constant.CloudVendorsEnum;
import com.meiya.whalex.interior.db.constant.DbResourceEnum;
import com.meiya.whalex.interior.db.constant.DbVersionEnum;


/**
 * MySql 配置管理工具
 *
 * @author 黄河森
 * @date 2019/12/23
 * @project whale-cloud-platformX
 */

@DbHelper(dbType = DbResourceEnum.doris, version = DbVersionEnum.DORIS_0_14, cloudVendors = CloudVendorsEnum.OPEN)
public class DorisConfigHelper extends BaseDorisConfigHelper {
}
