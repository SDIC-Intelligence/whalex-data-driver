package com.meiya.whalex.annotation;

import com.meiya.whalex.interior.db.constant.CloudVendorsEnum;
import com.meiya.whalex.interior.db.constant.DbResourceEnum;
import com.meiya.whalex.interior.db.constant.DbVersionEnum;

import java.lang.annotation.*;

/**
 * 组件服务注解
 *
 * @author 黄河森
 * @date 2019/9/16
 * @project whale-cloud-platformX
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Documented
public @interface DbService {

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
     * 是否表连接（需要根据表信息才能创建连接）
     *
     * @return
     */
    boolean isTableConnect() default false;
}
