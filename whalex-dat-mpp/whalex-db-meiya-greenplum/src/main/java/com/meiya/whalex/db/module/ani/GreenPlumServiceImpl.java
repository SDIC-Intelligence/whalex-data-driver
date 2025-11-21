package com.meiya.whalex.db.module.ani;

import com.meiya.whalex.annotation.DbService;
import com.meiya.whalex.interior.db.constant.CloudVendorsEnum;
import com.meiya.whalex.interior.db.constant.DbResourceEnum;
import com.meiya.whalex.interior.db.constant.DbVersionEnum;
import lombok.extern.slf4j.Slf4j;

/**
 * @author 黄河森
 * @date 2019/12/28
 * @project whale-cloud-platformX
 */
@DbService(dbType = DbResourceEnum.greenplum, version = DbVersionEnum.POSTGRE_9_3, cloudVendors = CloudVendorsEnum.OPEN)
@Slf4j
public class GreenPlumServiceImpl extends BasePostGreServiceImpl {
}
