package com.meiya.whalex.interior.base;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import java.io.Serializable;

/**
 * 基础请求报文
 *
 * @author 黄河森
 * @date 2019/9/26
 * @project whale-cloud-platformX
 */
@ApiModel(value = "云平台请求基础报文")
public class BaseQuery<SubClass extends BaseQuery> implements Serializable {

    /**
     * 角色
     */
    @ApiModelProperty(value = "角色")
    protected String role;

    /**
     * 令牌
     */
    @ApiModelProperty(value = "令牌")
    protected String token;

    public String getRole() {
        return role;
    }

    public SubClass setRole(String role) {
        this.role = role;
        return (SubClass) this;
    }

    public String getToken() {
        return token;
    }

    public SubClass setToken(String token) {
        this.token = token;
        return (SubClass) this;
    }

    public BaseQuery(String role) {
        this.role = role;
    }

    public BaseQuery() {
    }

    public BaseQuery(String role, String token) {
        this.role = role;
        this.token = token;
    }
}
