package com.meiya.whalex.db.util.param.impl.cache;

import com.meiya.whalex.annotation.DbParamUtil;
import com.meiya.whalex.interior.db.constant.CloudVendorsEnum;
import com.meiya.whalex.interior.db.constant.DbResourceEnum;
import com.meiya.whalex.interior.db.constant.DbVersionEnum;
import lombok.extern.slf4j.Slf4j;

/**
 * redis 组件参数转换工具类
 *
 * 重要：
 * 目前只支持简单的key-value形式，后面想办法屏蔽客户端使用复杂性
 * 且redis组件支持更多复杂存储方式!!!
 *
 * @author chenjp
 * @date 2020/9/7
 */
//  TODO 目前只是处理简单key-value形式存储，后续想办法兼容更多(屏蔽客户端使用的复杂性) by chenjp
@Slf4j
@DbParamUtil(dbType = DbResourceEnum.redis, version = DbVersionEnum.REDIS_6, cloudVendors = CloudVendorsEnum.OPEN)
public class RedisParamUtil extends BaseRedisParamUtil{
}
