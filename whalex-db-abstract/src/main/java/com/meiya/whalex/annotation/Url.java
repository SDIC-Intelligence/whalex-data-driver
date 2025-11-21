package com.meiya.whalex.annotation;


import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 组件属性注解
 *
 * @author 蔡荣桂
 * @date 2022/06/28
 * @project whale-cloud-platformX
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface Url {
}
