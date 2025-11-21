package com.meiya.whalex.sql.annotation;

import java.lang.annotation.*;

/**
 * @author 黄河森
 * @date 2022/8/5
 * @package com.meiya.whalex.annotation
 * @project whalex-data-driver
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Documented
public @interface SqlFunction {

    /**
     * 函数名称
     *
     * @return
     */
    String functionName();

}
