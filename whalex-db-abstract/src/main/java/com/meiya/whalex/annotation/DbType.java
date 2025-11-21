package com.meiya.whalex.annotation;


import com.meiya.whalex.interior.db.constant.DbResourceEnum;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 组件类型注解
 *
 * @author 蔡荣桂
 * @date 2022/06/28
 * @project whale-cloud-platformX
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Documented
public @interface DbType {

    DbResourceEnum[] value();
}
