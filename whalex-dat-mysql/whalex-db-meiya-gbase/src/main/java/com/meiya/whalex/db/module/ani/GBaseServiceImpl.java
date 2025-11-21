package com.meiya.whalex.db.module.ani;

import com.meiya.whalex.annotation.DbService;
import com.meiya.whalex.interior.db.constant.CloudVendorsEnum;
import com.meiya.whalex.interior.db.constant.DbResourceEnum;
import com.meiya.whalex.interior.db.constant.DbVersionEnum;
import lombok.extern.slf4j.Slf4j;

/**
 * GBase 服务
 *
 * @author 蔡荣桂
 * @date 2022/05/23
 * @project whale-cloud-platformX
 */
@DbService(dbType = DbResourceEnum.gbase, version = DbVersionEnum.GBASE_8_6_2_43, cloudVendors = CloudVendorsEnum.OPEN)
@Slf4j
public class GBaseServiceImpl extends BaseMySqlServiceImpl {
}
