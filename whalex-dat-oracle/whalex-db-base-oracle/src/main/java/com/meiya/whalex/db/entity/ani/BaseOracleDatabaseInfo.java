package com.meiya.whalex.db.entity.ani;

import com.meiya.whalex.db.entity.AbstractDatabaseInfo;
import com.meiya.whalex.db.template.ani.OracleDbConfTemplate;

/**
 * oracle 组件数据库配置信息
 *
 * @author 蔡荣桂
 * @date 2021/4/14
 * @project whale-cloud-platformX
 */
public class BaseOracleDatabaseInfo extends AbstractDatabaseInfo {

    /**
     * 连接地址模板
     */
    public static String SERVICE_NAME_URL_TEMPLATE = "jdbc:oracle:thin:@//%s:%s/%s";
    public static String SID_URL_TEMPLATE = "jdbc:oracle:thin:@%s:%s:%s";
    /**
     * 驱动类
     */
    public static String DRIVER_CLASS_NAME = "oracle.jdbc.driver.OracleDriver";

    /**
     * 连接校验
     */
    public static String VALIDATION_QUERY = "select 1 from dual";

    /**
     * poolPreparedStatements
     */
    public static boolean POOL_PREPARED_STATEMENTS = false;

    /**
     * 测试连接
     */
    public static boolean TEST_WHILE_IDLE = true;

    /**
     * 用户名
     */
    private String userName;

    /**
     * 密码
     */
    private String password;

    /**
     * 初始化连接数
     */
    private int initialSize = 1;

    /**
     * 最小连接数
     */
    private int minIdle = 1;

    /**
     * 最大连接数
     */
    private int maxActive = 64;

    /**
     * ip
     */
    private String serviceUrl;

    /**
     * 端口
     */
    private String port;

    /**
     * 库名
     */
    private String databaseName;


    private String schema;
    /**
     * 连接类型
     */
    private String connectionType;

    private boolean ignoreCase = Boolean.TRUE;

    public BaseOracleDatabaseInfo() {
    }

    public BaseOracleDatabaseInfo(String userName, String password, String serviceUrl, String port, String databaseName, String connectionType) {
        this.userName = userName;
        this.password = password;
        this.serviceUrl = serviceUrl;
        this.port = port;
        this.databaseName = databaseName;
        this.connectionType = connectionType;
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

    public int getInitialSize() {
        return initialSize;
    }

    public void setInitialSize(int initialSize) {
        this.initialSize = initialSize;
    }

    public int getMinIdle() {
        return minIdle;
    }

    public void setMinIdle(int minIdle) {
        this.minIdle = minIdle;
    }

    public int getMaxActive() {
        return maxActive;
    }

    public void setMaxActive(int maxActive) {
        this.maxActive = maxActive;
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

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public boolean isIgnoreCase() {
        return ignoreCase;
    }

    public void setIgnoreCase(boolean ignoreCase) {
        this.ignoreCase = ignoreCase;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    @Override
    public String getServerAddr() {
        return String.format(SERVER_ADDR_TEMP, serviceUrl, port);
    }

    @Override
    public String getDbName() {
        return databaseName;
    }

    public String getUrlTemplate() {
        if(OracleDbConfTemplate.CONNECTION_TYPE_SERVICE_NAME.equals(connectionType)){
            return SERVICE_NAME_URL_TEMPLATE;
        }
        return SID_URL_TEMPLATE;
    }
}
