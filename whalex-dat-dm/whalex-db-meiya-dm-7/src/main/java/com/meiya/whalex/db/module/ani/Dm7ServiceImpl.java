package com.meiya.whalex.db.module.ani;

import com.meiya.whalex.annotation.DbService;
import com.meiya.whalex.db.entity.ani.AniHandler;
import com.meiya.whalex.db.entity.ani.BaseDmDatabaseInfo;
import com.meiya.whalex.db.entity.ani.BaseDmTableInfo;
import com.meiya.whalex.interior.db.constant.CloudVendorsEnum;
import com.meiya.whalex.interior.db.constant.DbResourceEnum;
import com.meiya.whalex.interior.db.constant.DbVersionEnum;
import com.meiya.whalex.sql.ani.RdbmsCursorCache;
import com.meiya.whalex.sql.module.SqlParseHandler;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.dbutils.QueryRunner;

/**
 * @author 黄河森
 * @date 2022/5/19
 * @package com.meiya.whalex.db.module.ani
 * @project whalex-data-driver
 */
@DbService(dbType = DbResourceEnum.dm, version = DbVersionEnum.DM_7, cloudVendors = CloudVendorsEnum.OPEN)
@Slf4j
public class Dm7ServiceImpl extends BaseDmServiceImpl<QueryRunner, AniHandler, BaseDmDatabaseInfo, BaseDmTableInfo, RdbmsCursorCache> {

    @Override
    protected SqlParseHandler getSqlParseHandler(BaseDmDatabaseInfo database) {
        return new Dm7SqlParseHandler(database);
    }
}
