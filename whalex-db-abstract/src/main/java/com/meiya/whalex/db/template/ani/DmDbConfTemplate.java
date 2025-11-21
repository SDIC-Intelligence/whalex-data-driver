package com.meiya.whalex.db.template.ani;

import com.meiya.whalex.annotation.*;
import com.meiya.whalex.db.template.BaseDbConfTemplate;
import com.meiya.whalex.interior.db.constant.DbResourceEnum;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Mysql 模板配置
 *
 * @author 黄河森
 * @date 2019/12/24
 * @project whale-cloud-platformX
 */
@Builder
@Data
@NoArgsConstructor
@AllArgsConstructor
@DbType(value = {DbResourceEnum.dm})
public class DmDbConfTemplate extends BaseDbConfTemplate {

    /**
     * ip
     */
    @Host
    private String serviceUrl;
    /**
     * 端口
     */
    @Port
    private String port;
    /**
     * 数据库名
     */
    @DatabaseName
    private String dbaseName;
    /**
     * 用户名
     */
    @UserName
    private String userName;
    /**
     * 密码
     */
    @Password
    private String password;

    @ExtendField(value = "schema")
    private String schema;

    @ExtendField(value = "ignoreCase")
    private Boolean ignoreCase;
}
