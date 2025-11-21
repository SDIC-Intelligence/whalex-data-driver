package com.meiya.whalex.db.module.file;

import com.meiya.whalex.annotation.DbService;
import com.meiya.whalex.interior.db.constant.CloudVendorsEnum;
import com.meiya.whalex.interior.db.constant.DbResourceEnum;
import lombok.extern.slf4j.Slf4j;

/**
 * Hadoop 服务
 *
 * @author 黄河森
 * @date 2019/12/30
 * @project whale-cloud-platformX
 */
@DbService(dbType = DbResourceEnum.ftp,  cloudVendors = CloudVendorsEnum.OPEN)
@Slf4j
public class FtpServiceImpl extends BaseFtpServiceImpl {

}
