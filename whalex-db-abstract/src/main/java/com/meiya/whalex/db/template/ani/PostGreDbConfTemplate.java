package com.meiya.whalex.db.template.ani;

import com.meiya.whalex.annotation.DatabaseName;
import com.meiya.whalex.annotation.DbType;
import com.meiya.whalex.annotation.ExtendField;
import com.meiya.whalex.annotation.Host;
import com.meiya.whalex.annotation.Password;
import com.meiya.whalex.annotation.Port;
import com.meiya.whalex.annotation.UserName;
import com.meiya.whalex.db.template.BaseDbConfTemplate;
import com.meiya.whalex.interior.db.constant.DbResourceEnum;
import lombok.Builder;

/**
 * PostGre 模板配置
 *
 * @author 黄河森
 * @date 2019/12/24
 * @project whale-cloud-platformX
 */
@Builder
@DbType(value = {DbResourceEnum.postgre, DbResourceEnum.kingbasees, DbResourceEnum.highgo})
public class PostGreDbConfTemplate extends BaseDbConfTemplate {

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

    /**
     * 模式
     */
    @ExtendField(value = "schema")
    private String schema;

    @ExtendField(value = "ignoreCase")
    private Boolean ignoreCase;

    @ExtendField(value = "tinyInt1isBit")
    private Boolean tinyInt1isBit;

    @ExtendField(value = "likeToiLike")
    private Boolean likeToiLike;

    public Boolean getTinyInt1isBit() {
        return tinyInt1isBit;
    }

    public void setTinyInt1isBit(Boolean tinyInt1isBit) {
        this.tinyInt1isBit = tinyInt1isBit;
    }

    public String getServiceUrl() {
        return serviceUrl;
    }

    public void setServiceUrl(String serviceUrl) {
        this.serviceUrl = serviceUrl;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public String getDbaseName() {
        return dbaseName;
    }

    public void setDbaseName(String dbaseName) {
        this.dbaseName = dbaseName;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public Boolean getIgnoreCase() {
        return ignoreCase;
    }

    public void setIgnoreCase(Boolean ignoreCase) {
        this.ignoreCase = ignoreCase;
    }

    public Boolean getLikeToiLike() {
        return likeToiLike;
    }

    public void setLikeToiLike(Boolean likeToiLike) {
        this.likeToiLike = likeToiLike;
    }

    public PostGreDbConfTemplate() {
    }

    public PostGreDbConfTemplate(String serviceUrl, String port, String dbaseName, String userName, String password, String schema) {
        this.serviceUrl = serviceUrl;
        this.port = port;
        this.dbaseName = dbaseName;
        this.userName = userName;
        this.password = password;
        this.schema = schema;
    }

    public PostGreDbConfTemplate(String serviceUrl, String port, String dbaseName, String userName, String password, String schema, Boolean ignoreCase, Boolean tinyInt1isBit) {
        this.serviceUrl = serviceUrl;
        this.port = port;
        this.dbaseName = dbaseName;
        this.userName = userName;
        this.password = password;
        this.schema = schema;
        this.ignoreCase = ignoreCase;
        this.tinyInt1isBit = tinyInt1isBit;
    }

    public PostGreDbConfTemplate(String serviceUrl, String port, String dbaseName, String userName, String password, String schema, Boolean ignoreCase, Boolean tinyInt1isBit, Boolean likeToiLike) {
        this.serviceUrl = serviceUrl;
        this.port = port;
        this.dbaseName = dbaseName;
        this.userName = userName;
        this.password = password;
        this.schema = schema;
        this.ignoreCase = ignoreCase;
        this.tinyInt1isBit = tinyInt1isBit;
        this.likeToiLike = likeToiLike;
    }
}
