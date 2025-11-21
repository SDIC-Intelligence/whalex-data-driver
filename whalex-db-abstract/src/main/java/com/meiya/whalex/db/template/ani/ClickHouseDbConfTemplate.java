package com.meiya.whalex.db.template.ani;

import com.meiya.whalex.annotation.*;
import com.meiya.whalex.db.template.BaseDbConfTemplate;
import com.meiya.whalex.interior.db.constant.DbResourceEnum;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

/**
 * PostGre 模板配置
 *
 * @author 黄河森
 * @date 2019/12/24
 * @project whale-cloud-platformX
 */
@Builder
@DbType(DbResourceEnum.clickhouse)
@AllArgsConstructor
@Data
public class ClickHouseDbConfTemplate extends BaseDbConfTemplate {

    /**
     * ip:port
     */
    @Url
    private String serviceUrl;
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

    @ExtendField("clusterName")
    private String clusterName;

    public ClickHouseDbConfTemplate() {
    }

    public ClickHouseDbConfTemplate(String serviceUrl, String dbaseName, String userName, String password) {
        this.serviceUrl = serviceUrl;
        this.dbaseName = dbaseName;
        this.userName = userName;
        this.password = password;
    }


}
