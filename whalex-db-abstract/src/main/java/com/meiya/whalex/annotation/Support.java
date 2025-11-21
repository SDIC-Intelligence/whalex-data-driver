package com.meiya.whalex.annotation;

import com.meiya.whalex.db.constant.SupportPower;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 组件支持能务注解
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Documented
public @interface  Support {
    SupportPower[] value() default {};
}
