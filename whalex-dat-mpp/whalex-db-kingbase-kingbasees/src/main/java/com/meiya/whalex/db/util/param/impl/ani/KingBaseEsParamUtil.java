package com.meiya.whalex.db.util.param.impl.ani;

import com.meiya.whalex.annotation.DbParamUtil;
import com.meiya.whalex.db.entity.ani.AniHandler;
import com.meiya.whalex.db.entity.ani.BasePostGreTableInfo;
import com.meiya.whalex.db.entity.ani.KingBaseEsDatabaseInfo;
import com.meiya.whalex.db.util.param.impl.ani.BasePostGreParamUtil;
import com.meiya.whalex.interior.db.constant.CloudVendorsEnum;
import com.meiya.whalex.interior.db.constant.DbResourceEnum;
import com.meiya.whalex.interior.db.constant.DbVersionEnum;
import lombok.extern.slf4j.Slf4j;

/**
 * Gauss 参数转换工具类
 *
 * @author 黄河森
 * @date 2019/12/23
 * @project whale-cloud-platformX
 */
@DbParamUtil(dbType = DbResourceEnum.kingbasees, version = DbVersionEnum.KINGBASEES_8_6_0, cloudVendors = CloudVendorsEnum.KingBase)
@Slf4j
public class KingBaseEsParamUtil extends BasePostGreParamUtil<AniHandler, KingBaseEsDatabaseInfo, BasePostGreTableInfo> {

    /**
     * like建表模板
     */
    protected final static String CREATE_TABLE_LIKE_TEMPLATE = "CREATE TABLE %s (LIKE %s INCLUDING ALL)";

    @Override
    protected String getCreateTableLikeSqlTmp() {
        return CREATE_TABLE_LIKE_TEMPLATE;
    }
}
