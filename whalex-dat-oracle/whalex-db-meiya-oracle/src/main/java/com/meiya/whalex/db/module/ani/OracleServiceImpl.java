package com.meiya.whalex.db.module.ani;

import com.meiya.whalex.annotation.DbService;
import com.meiya.whalex.interior.db.constant.CloudVendorsEnum;
import com.meiya.whalex.interior.db.constant.DbResourceEnum;
import com.meiya.whalex.interior.db.constant.DbVersionEnum;
import lombok.extern.slf4j.Slf4j;

/**
 * Oracle 服务
 *
 * @author 蔡荣桂
 * @date 2021/4/14
 * @project whale-cloud-platformX
 */
@DbService(dbType = DbResourceEnum.oracle, version = DbVersionEnum.ORACLE_11G, cloudVendors = CloudVendorsEnum.OPEN)
@Slf4j
public class OracleServiceImpl extends BaseOracleServiceImpl{
}
