package com.meiya.whalex.db.module.ani;

import com.meiya.whalex.annotation.DbService;
import com.meiya.whalex.db.entity.ani.BasePostGreDatabaseInfo;
import com.meiya.whalex.db.util.param.impl.ani.VerticaRowProcessor;
import com.meiya.whalex.interior.db.constant.CloudVendorsEnum;
import com.meiya.whalex.interior.db.constant.DbResourceEnum;
import com.meiya.whalex.interior.db.constant.DbVersionEnum;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.dbutils.BasicRowProcessor;

/**
 * @author 黄河森
 * @date 2020/9/28
 * @project whalex-data-driver
 */
@DbService(dbType = DbResourceEnum.vertica, version = DbVersionEnum.VERTICA_10_0_1, cloudVendors = CloudVendorsEnum.H3C)
@Slf4j
public class VerticaServiceImpl extends BasePostGreServiceImpl {

    @Override
    protected BasicRowProcessor getRowProcessor(BasePostGreDatabaseInfo postGreDatabaseInfo) {
        return new VerticaRowProcessor();
    }
}
