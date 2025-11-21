package com.meiya.whalex.db.entity;

import lombok.Data;

import java.io.Serializable;

/**
 * raw statement 接口统一返回
 *
 * @author 蔡荣桂
 * @date 2024/01/16
 * @project whale-cloud-platformX
 */
@Data
public class DataResult<T> implements Serializable {

    private Boolean success = true;
    private int code = 0;
    private String message = "成功";
    private Integer total;
    private T data;
}
