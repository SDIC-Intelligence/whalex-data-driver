package com.meiya.whalex.db.util.helper.impl.dwh;

import com.meiya.whalex.annotation.DbHelper;
import com.meiya.whalex.interior.db.constant.CloudVendorsEnum;
import com.meiya.whalex.interior.db.constant.DbResourceEnum;
import com.meiya.whalex.interior.db.constant.DbVersionEnum;
import lombok.extern.slf4j.Slf4j;

/**
 * @author 黄河森
 * @date 2020/9/28
 * @project whalex-data-driver
 */
@DbHelper(dbType = DbResourceEnum.hive, version = DbVersionEnum.HIVE_2_1_1, cloudVendors = CloudVendorsEnum.OPEN)
@Slf4j
public class HiveConfigHelper extends BaseHiveConfigHelper {
}
