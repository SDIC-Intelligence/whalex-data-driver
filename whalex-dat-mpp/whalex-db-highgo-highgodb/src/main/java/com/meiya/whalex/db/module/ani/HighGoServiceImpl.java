package com.meiya.whalex.db.module.ani;

import com.meiya.whalex.annotation.DbService;
import com.meiya.whalex.db.entity.ani.AniHandler;
import com.meiya.whalex.db.entity.ani.BasePostGreTableInfo;
import com.meiya.whalex.db.entity.ani.HighGoDatabaseInfo;
import com.meiya.whalex.db.entity.ani.HighGoSqlParseHandler;
import com.meiya.whalex.db.util.param.impl.ani.HighGoRowProcessor;
import com.meiya.whalex.interior.db.constant.CloudVendorsEnum;
import com.meiya.whalex.interior.db.constant.DbResourceEnum;
import com.meiya.whalex.interior.db.constant.DbVersionEnum;
import com.meiya.whalex.sql.ani.RdbmsCursorCache;
import com.meiya.whalex.sql.module.SqlParseHandler;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.dbutils.QueryRunner;

/**
 * @author 黄河森
 * @date 2020/9/28
 * @project whalex-data-driver
 */
@DbService(dbType = DbResourceEnum.highgo, version = DbVersionEnum.HIGHGO_1_2, cloudVendors = CloudVendorsEnum.HighGo)
@Slf4j
public class HighGoServiceImpl extends BasePostGreServiceImpl<QueryRunner,
        AniHandler,
        HighGoDatabaseInfo,
        BasePostGreTableInfo,
        RdbmsCursorCache> {

    @Override
    protected HighGoRowProcessor getRowProcessor(HighGoDatabaseInfo databaseInfo) {
        return new HighGoRowProcessor();
    }

    @Override
    protected SqlParseHandler getSqlParseHandler(HighGoDatabaseInfo database) {
        return new HighGoSqlParseHandler(database);
    }
}
