package com.meiya.whalex.db.util.param.impl.ani;

import com.meiya.whalex.db.entity.ani.AniHandler;
import com.meiya.whalex.db.entity.ani.BaseMySqlTableInfo;
import com.meiya.whalex.db.entity.ani.DorisDatabaseInfo;
import lombok.extern.slf4j.Slf4j;

/**
 * MySql 参数转换工具类
 *
 * @author 黄河森
 * @date 2019/12/23
 * @project whale-cloud-platformX
 */
@Slf4j
public class BaseDorisParamUtil extends BaseMySqlParamUtil<AniHandler, DorisDatabaseInfo, BaseMySqlTableInfo> {

    DorisParserUtil baseDorisParserUtil = new DorisParserUtil();

    @Override
    protected BaseMySqlParserUtil getBaseMysqlParserUtil() {
        return baseDorisParserUtil;
    }
}
