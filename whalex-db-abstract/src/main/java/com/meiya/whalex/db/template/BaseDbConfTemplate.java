package com.meiya.whalex.db.template;

import com.meiya.whalex.annotation.ExtendField;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author 黄河森
 * @date 2021/6/29
 * @project whalex-data-driver-back
 */
@NoArgsConstructor
@AllArgsConstructor
@Data
public class BaseDbConfTemplate {

    @ExtendField(value = "initialSize")
    public Integer initialSize = 1;
    @ExtendField(value = "minIdle")
    public Integer minIdle = 32;
    @ExtendField(value = "maxActive")
    public Integer maxActive = 64;
    @ExtendField(value = "timeOut")
    public Integer timeOut = 60;
}
