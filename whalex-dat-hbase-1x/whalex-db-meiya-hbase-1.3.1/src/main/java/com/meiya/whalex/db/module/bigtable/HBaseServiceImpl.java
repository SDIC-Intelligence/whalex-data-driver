package com.meiya.whalex.db.module.bigtable;

import com.meiya.whalex.annotation.DbService;
import com.meiya.whalex.interior.db.constant.CloudVendorsEnum;
import com.meiya.whalex.interior.db.constant.DbResourceEnum;
import com.meiya.whalex.interior.db.constant.DbVersionEnum;
import lombok.extern.slf4j.Slf4j;

/**
 * @author 黄河森
 * @date 2020/9/28
 * @project whalex-data-driver
 */
@DbService(dbType = DbResourceEnum.hbase, version = DbVersionEnum.HBASE_1_3_1, cloudVendors = CloudVendorsEnum.OPEN)
@Slf4j
public class HBaseServiceImpl extends BaseHBaseServiceImpl {
}
