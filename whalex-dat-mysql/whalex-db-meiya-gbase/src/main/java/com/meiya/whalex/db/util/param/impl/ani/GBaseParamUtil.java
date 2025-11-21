package com.meiya.whalex.db.util.param.impl.ani;

import com.meiya.whalex.annotation.DbParamUtil;
import com.meiya.whalex.interior.db.constant.CloudVendorsEnum;
import com.meiya.whalex.interior.db.constant.DbResourceEnum;
import com.meiya.whalex.interior.db.constant.DbVersionEnum;
import lombok.extern.slf4j.Slf4j;

/**
 * GBase 参数转换工具类
 *
 * @author 蔡荣桂
 * @date 2022/05/23
 * @project whale-cloud-platformX
 */
@DbParamUtil(dbType = DbResourceEnum.gbase, version = DbVersionEnum.GBASE_8_6_2_43, cloudVendors = CloudVendorsEnum.OPEN)
@Slf4j
public class GBaseParamUtil extends BaseMySqlParamUtil {

    GBaseParserUtil gBaseParserUtil = new GBaseParserUtil();

    @Override
    protected BaseMySqlParserUtil getBaseMysqlParserUtil() {
        return gBaseParserUtil;
    }
}
