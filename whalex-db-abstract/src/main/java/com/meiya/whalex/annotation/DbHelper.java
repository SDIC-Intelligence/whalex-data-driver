package com.meiya.whalex.annotation;

import com.meiya.whalex.interior.db.constant.CloudVendorsEnum;
import com.meiya.whalex.interior.db.constant.DbResourceEnum;
import com.meiya.whalex.interior.db.constant.DbVersionEnum;

import java.lang.annotation.*;
import java.util.concurrent.TimeUnit;

/**
 * 组件数据库信息管理类注解
 *
 * @author 黄河森
 * @date 2019/9/16
 * @project whale-cloud-platformX
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Documented
public @interface DbHelper {

    /**
     * 云厂商
     * @return
     */
    CloudVendorsEnum cloudVendors() default CloudVendorsEnum.undefine;

    /**
     * 组件类型
     *
     * @return
     */
    DbResourceEnum dbType();

    /**
     * 组件版本
     * @return
     */
    DbVersionEnum version() default DbVersionEnum.UNDEFINE;

    /**
     * 是否需要表信息才可以建立连接对象
     *
     * @return
     */
    boolean isTableConnect() default false;

    /**
     * 是否需要设置连接缓存过期
     *
     * @return
     */
    boolean isTTLConnectCache() default false;

    /**
     * 过期时间
     *
     * @return
     */
    long ttlTime() default 12;

    /**
     * 过期时间类型
     *
     * @return
     */
    TimeUnit ttlType() default TimeUnit.HOURS;

    /**
     * 是否需要定时巡检检测连接是否可用
     *
     * @return
     */
    boolean needCheckDataSource() default true;

    /**
     * 游标过期时间
     *
     * @return
     */
    long cursorTtlTime() default 5;

    /**
     * 游标过期时间类型
     *
     * @return
     */
    TimeUnit cursorTtlType() default TimeUnit.MINUTES;

}
