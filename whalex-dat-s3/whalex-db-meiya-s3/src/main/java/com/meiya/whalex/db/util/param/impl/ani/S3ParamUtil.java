package com.meiya.whalex.db.util.param.impl.ani;

import com.meiya.whalex.annotation.DbParamUtil;
import com.meiya.whalex.interior.db.constant.CloudVendorsEnum;
import com.meiya.whalex.interior.db.constant.DbResourceEnum;
import com.meiya.whalex.interior.db.constant.DbVersionEnum;
import lombok.extern.slf4j.Slf4j;

/**
 * S3 参数转换工具类
 *
 * @author 黄河森
 * @date 2019/12/23
 * @project whale-cloud-platformX
 *
 */
@DbParamUtil(dbType = DbResourceEnum.s3, version = DbVersionEnum.S3_1_11_415, cloudVendors = CloudVendorsEnum.MY)
@Slf4j
public class S3ParamUtil extends BaseS3ParamUtil {
}
