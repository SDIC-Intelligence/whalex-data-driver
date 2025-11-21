package com.meiya.whalex.db.module.ani;

import com.meiya.whalex.annotation.DbService;
import com.meiya.whalex.db.entity.ani.AniHandler;
import com.meiya.whalex.db.entity.ani.BasePostGreTableInfo;
import com.meiya.whalex.db.entity.ani.KingBaseEsDatabaseInfo;
import com.meiya.whalex.db.entity.ani.KingBaseSqlParseHandler;
import com.meiya.whalex.db.util.param.impl.ani.KingBaseEsRowProcessor;
import com.meiya.whalex.interior.db.constant.CloudVendorsEnum;
import com.meiya.whalex.interior.db.constant.DbResourceEnum;
import com.meiya.whalex.interior.db.constant.DbVersionEnum;
import com.meiya.whalex.sql.ani.RdbmsCursorCache;
import com.meiya.whalex.sql.module.SqlParseHandler;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.dbutils.BasicRowProcessor;
import org.apache.commons.dbutils.QueryRunner;

/**
 * @author 黄河森
 * @date 2020/9/28
 * @project whalex-data-driver
 */
@DbService(dbType = DbResourceEnum.kingbasees, version = DbVersionEnum.KINGBASEES_8_6_0, cloudVendors = CloudVendorsEnum.KingBase)
@Slf4j
public class KingBaseEsServiceImpl extends BasePostGreServiceImpl<QueryRunner,
        AniHandler,
        KingBaseEsDatabaseInfo,
        BasePostGreTableInfo,
        RdbmsCursorCache> {

    @Override
    protected BasicRowProcessor getRowProcessor(KingBaseEsDatabaseInfo databaseInfo) {
        return new KingBaseEsRowProcessor();
    }

    @Override
    protected SqlParseHandler getSqlParseHandler(KingBaseEsDatabaseInfo database) {
        return new KingBaseSqlParseHandler(database);
    }
}
