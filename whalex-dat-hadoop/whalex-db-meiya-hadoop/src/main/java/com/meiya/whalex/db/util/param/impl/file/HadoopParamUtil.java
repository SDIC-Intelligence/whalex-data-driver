package com.meiya.whalex.db.util.param.impl.file;

import com.meiya.whalex.annotation.DbParamUtil;
import com.meiya.whalex.interior.db.constant.CloudVendorsEnum;
import com.meiya.whalex.interior.db.constant.DbResourceEnum;
import com.meiya.whalex.interior.db.constant.DbVersionEnum;
import lombok.extern.slf4j.Slf4j;

/**
 * @author 黄河森
 * @date 2020/10/15
 * @project whalex-data-driver
 */
@DbParamUtil(dbType = DbResourceEnum.hadoop, version = DbVersionEnum.HADOOP_3_1_0, cloudVendors = CloudVendorsEnum.OPEN)
@Slf4j
public class HadoopParamUtil extends BaseHadoopParamUtil {
}
