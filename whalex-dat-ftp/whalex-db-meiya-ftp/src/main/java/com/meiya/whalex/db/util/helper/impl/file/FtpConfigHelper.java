package com.meiya.whalex.db.util.helper.impl.file;

import com.meiya.whalex.annotation.DbHelper;
import com.meiya.whalex.interior.db.constant.CloudVendorsEnum;
import com.meiya.whalex.interior.db.constant.DbResourceEnum;
import lombok.extern.slf4j.Slf4j;

/**
 * @author wangkm1
 * @date 2025/04/01
 * @project whalex-data-driver
 */
@DbHelper(dbType = DbResourceEnum.ftp, cloudVendors = CloudVendorsEnum.OPEN)
@Slf4j
public class FtpConfigHelper extends BaseFtpConfigHelper {
}
