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
 * Oracle 模板配置
 *
 * @author 蔡荣桂
 * @date 2021/4/14
 * @project whale-cloud-platformX
 */
@Builder
@DbType(value = DbResourceEnum.oracle)
public class OracleDbConfTemplate extends BaseDbConfTemplate {

    public static final String CONNECTION_TYPE_SID = "sid";
    public static final String CONNECTION_TYPE_SERVICE_NAME = "service_name";

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
     * 连接方式
     */
    @ExtendField(value = "connectionType")
    private String connectionType;

    @ExtendField(value = "ignoreCase")
    private Boolean ignoreCase;

    /**
     * 模式
     */
    @ExtendField(value = "schema")
    private String schema;

    public Boolean getIgnoreCase() {
        return ignoreCase;
    }

    public void setIgnoreCase(Boolean ignoreCase) {
        this.ignoreCase = ignoreCase;
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

    public String getConnectionType() {
        return connectionType;
    }

    public void setConnectionType(String connectionType) {
        this.connectionType = connectionType;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public OracleDbConfTemplate() {
    }

    public OracleDbConfTemplate(String serviceUrl, String port, String dbaseName, String userName, String password, String connectionType, boolean ignoreCase, String schema) {
        this.serviceUrl = serviceUrl;
        this.port = port;
        this.dbaseName = dbaseName;
        this.userName = userName;
        this.password = password;
        this.connectionType = connectionType;
        this.ignoreCase = ignoreCase;
        this.schema = schema;
    }

}
