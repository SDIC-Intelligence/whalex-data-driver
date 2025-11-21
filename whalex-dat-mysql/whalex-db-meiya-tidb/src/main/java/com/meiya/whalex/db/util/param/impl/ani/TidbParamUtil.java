package com.meiya.whalex.db.util.param.impl.ani;

import com.meiya.whalex.annotation.DbParamUtil;
import com.meiya.whalex.db.entity.ani.AniHandler;
import com.meiya.whalex.db.entity.ani.BaseMySqlDatabaseInfo;
import com.meiya.whalex.db.entity.ani.BaseMySqlTableInfo;
import com.meiya.whalex.interior.db.constant.CloudVendorsEnum;
import com.meiya.whalex.interior.db.constant.DbResourceEnum;
import com.meiya.whalex.interior.db.constant.DbVersionEnum;
import com.meiya.whalex.interior.db.operation.in.CreateTableParamCondition;
import lombok.extern.slf4j.Slf4j;

/**
 * Tidb 参数转换工具类
 *
 * @author 黄河森
 * @date 2019/12/23
 * @project whale-cloud-platformX
 */
@DbParamUtil(dbType = DbResourceEnum.tidb, version = DbVersionEnum.MYSQL_8_0_17, cloudVendors = CloudVendorsEnum.OPEN)
@Slf4j
public class TidbParamUtil extends BaseMySqlParamUtil {
    BaseMySqlParserUtil baseMySqlParserUtil = new TidbParserUtil();

    @Override
    protected BaseMySqlParserUtil getBaseMysqlParserUtil() {
        return baseMySqlParserUtil;
    }

    @Override
    protected AniHandler transitionCreateTableParam(CreateTableParamCondition createTableParamCondition, BaseMySqlDatabaseInfo databaseConf, BaseMySqlTableInfo tableConf) {
        AniHandler aniHandler = new AniHandler();
        aniHandler.setAniCreateTable(getBaseMysqlParserUtil().parserCreateTableSql(createTableParamCondition, databaseConf, tableConf));
        return aniHandler;
    }
}
