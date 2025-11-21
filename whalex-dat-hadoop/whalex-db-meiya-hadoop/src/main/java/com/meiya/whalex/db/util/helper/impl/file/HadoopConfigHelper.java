package com.meiya.whalex.db.util.helper.impl.file;

import com.meiya.whalex.annotation.DbHelper;
import com.meiya.whalex.interior.db.constant.CloudVendorsEnum;
import com.meiya.whalex.interior.db.constant.DbResourceEnum;
import com.meiya.whalex.interior.db.constant.DbVersionEnum;
import lombok.extern.slf4j.Slf4j;

/**
 * @author 黄河森
 * @date 2020/10/15
 * @project whalex-data-driver
 */
@DbHelper(dbType = DbResourceEnum.hadoop, version = DbVersionEnum.HADOOP_3_1_0, cloudVendors = CloudVendorsEnum.OPEN)
@Slf4j
public class HadoopConfigHelper extends BaseHadoopConfigHelper {
}
