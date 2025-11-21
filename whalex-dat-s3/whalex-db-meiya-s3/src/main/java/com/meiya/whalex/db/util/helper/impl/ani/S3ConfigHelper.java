package com.meiya.whalex.db.util.helper.impl.ani;


import com.meiya.whalex.annotation.DbHelper;
import com.meiya.whalex.interior.db.constant.CloudVendorsEnum;
import com.meiya.whalex.interior.db.constant.DbResourceEnum;
import com.meiya.whalex.interior.db.constant.DbVersionEnum;
import lombok.extern.slf4j.Slf4j;

/**
 * S3 配置管理工具
 *
 * @author xult
 * @date 2020/3/27
 * @project whale-cloud-platformX
 */
@DbHelper(dbType = DbResourceEnum.s3, version = DbVersionEnum.S3_1_11_415, cloudVendors = CloudVendorsEnum.MY)
@Slf4j
public class S3ConfigHelper extends BaseS3ConfigHelper {
}
