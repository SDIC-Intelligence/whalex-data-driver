package com.meiya.whalex.db.template.ani;

import com.meiya.whalex.annotation.*;
import com.meiya.whalex.db.template.BaseDbConfTemplate;
import com.meiya.whalex.interior.db.constant.DbResourceEnum;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

/**
 * Mysql 模板配置
 *
 * @author 黄河森
 * @date 2019/12/24
 * @project whale-cloud-platformX
 */
@Builder
@DbType(value = {DbResourceEnum.doris})
@AllArgsConstructor
@Data
public class DorisDbConfTemplate extends BaseDbConfTemplate {

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
     * 字符编码
     */
    @ExtendField(value = "characterEncoding")
    private String characterEncoding;

    /**
     * 目录
     */
    @ExtendField(value = "catalog")
    private String catalog;

    /**
     * 是否开启 SSL
     */
    @ExtendField(value = "useSSL")
    private Boolean useSSL;

    @ExtendField(value = "fePort")
    private String fePort;

    /**
     * HTTP 连接超时
     */
    private Long httpConnectTimeout;

    /**
     * HTTP 读取超时
     */
    private Long httpReadTimeout;

    /**
     * HTTP 最大连接数
     */
    private Integer httpMaxIdleConnections;

    public String getCatalog() {
        return catalog;
    }

    public void setCatalog(String catalog) {
        this.catalog = catalog;
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

    public String getCharacterEncoding() {
        return characterEncoding;
    }

    public void setCharacterEncoding(String characterEncoding) {
        this.characterEncoding = characterEncoding;
    }

    public Boolean getUseSSL() {
        return useSSL;
    }

    public void setUseSSL(Boolean useSSL) {
        this.useSSL = useSSL;
    }

    public String getFePort() {
        return fePort;
    }

    public void setFePort(String fePort) {
        this.fePort = fePort;
    }

    public DorisDbConfTemplate() {
    }

    public DorisDbConfTemplate(String serviceUrl, String port, String dbaseName, String userName, String password) {
        this.serviceUrl = serviceUrl;
        this.port = port;
        this.dbaseName = dbaseName;
        this.userName = userName;
        this.password = password;
    }

    public DorisDbConfTemplate(String serviceUrl, String port, String dbaseName, String userName, String password, String characterEncoding, Boolean useSSL) {
        this.serviceUrl = serviceUrl;
        this.port = port;
        this.dbaseName = dbaseName;
        this.userName = userName;
        this.password = password;
        this.characterEncoding = characterEncoding;
        this.useSSL = useSSL;
    }
}
