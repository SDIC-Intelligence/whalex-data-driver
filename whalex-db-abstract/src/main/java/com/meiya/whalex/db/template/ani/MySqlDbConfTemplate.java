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
import lombok.AllArgsConstructor;
import lombok.Builder;

/**
 * Mysql 模板配置
 *
 * @author 黄河森
 * @date 2019/12/24
 * @project whale-cloud-platformX
 */
@Builder
@DbType(value = {DbResourceEnum.mysql})
@AllArgsConstructor
public class MySqlDbConfTemplate extends BaseDbConfTemplate {

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
     * 是否开启 SSL
     */
    @ExtendField(value = "useSSL")
    private Boolean useSSL;

    /**
     * 是否开启 tinyInt1isBit
     */
    @ExtendField(value = "tinyInt1isBit")
    private Boolean tinyInt1isBit;

    /**
     * 时区
     */
    @ExtendField(value = "serverTimezone")
    private String serverTimezone;

    /**
     * bit(1) 查询数据时是否转为 boolean 值，默认true
     * false 为 bit(1)输出为int类型
     */
    @ExtendField(value = "bit1isBoolean")
    private Boolean bit1isBoolean;

    /**
     * 是否将 json 字段转为 map 或 list，默认为 false：输出字符串
     */
    @ExtendField(value = "jsonToObject")
    private Boolean jsonToObject;


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

    public String getServerTimezone() {
        return serverTimezone;
    }

    public void setServerTimezone(String serverTimezone) {
        this.serverTimezone = serverTimezone;
    }

    public Boolean getBit1isBoolean() {
        return bit1isBoolean;
    }

    public void setBit1isBoolean(Boolean bit1isBoolean) {
        this.bit1isBoolean = bit1isBoolean;
    }

    public Boolean getJsonToObject() {
        return jsonToObject;
    }

    public void setJsonToObject(Boolean jsonToObject) {
        this.jsonToObject = jsonToObject;
    }

    public MySqlDbConfTemplate() {
    }

    public MySqlDbConfTemplate(String serviceUrl, String port, String dbaseName, String userName, String password) {
        this.serviceUrl = serviceUrl;
        this.port = port;
        this.dbaseName = dbaseName;
        this.userName = userName;
        this.password = password;
    }

    public MySqlDbConfTemplate(String serviceUrl, String port, String dbaseName, String userName, String password, String characterEncoding, Boolean useSSL) {
        this.serviceUrl = serviceUrl;
        this.port = port;
        this.dbaseName = dbaseName;
        this.userName = userName;
        this.password = password;
        this.characterEncoding = characterEncoding;
        this.useSSL = useSSL;
    }
}
