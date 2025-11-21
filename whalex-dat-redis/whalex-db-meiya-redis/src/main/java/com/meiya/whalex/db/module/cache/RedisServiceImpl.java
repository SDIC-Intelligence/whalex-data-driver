package com.meiya.whalex.db.module.cache;

import com.meiya.whalex.annotation.DbService;

import com.meiya.whalex.interior.db.constant.CloudVendorsEnum;
import com.meiya.whalex.interior.db.constant.DbResourceEnum;
import com.meiya.whalex.interior.db.constant.DbVersionEnum;
import lombok.extern.slf4j.Slf4j;


/**
 * @author chenjp
 * @date 2020/9/8
 */
@Slf4j
@DbService(dbType = DbResourceEnum.redis, version = DbVersionEnum.REDIS_6, cloudVendors = CloudVendorsEnum.OPEN)
public class RedisServiceImpl extends BaseRedisServiceImpl{
}
