package com.meiya.whalex.db.util.helper.impl.ani;

import com.meiya.whalex.annotation.DbHelper;
import com.meiya.whalex.interior.db.constant.CloudVendorsEnum;
import com.meiya.whalex.interior.db.constant.DbResourceEnum;
import com.meiya.whalex.interior.db.constant.DbVersionEnum;
import lombok.extern.slf4j.Slf4j;

/**
 * MySql 配置管理工具
 *
 * @author 黄河森
 * @date 2019/12/23
 * @project whale-cloud-platformX
 */

@DbHelper(dbType = DbResourceEnum.mysql, version = DbVersionEnum.MYSQL_8_0_17, cloudVendors = CloudVendorsEnum.OPEN)
@Slf4j
public class MySqlConfigHelper extends BaseMySqlConfigHelper {
}
