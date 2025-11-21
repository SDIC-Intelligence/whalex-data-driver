package com.meiya.whalex.db.entity.ani;

import com.meiya.whalex.db.entity.AbstractDatabaseInfo;

/**
 * MySql 组件数据库配置信息
 *
 * @author 黄河森
 * @date 2019/12/23
 * @project whale-cloud-platformX
 */
public class BaseMySqlDatabaseInfo extends AbstractDatabaseInfo {

    /**
     * 驱动类
     */
    public static String DRIVER_CLASS_NAME = "com.mysql.jdbc.Driver";

    /**
     * 连接校验
     */
    public static String VALIDATION_QUERY = "select 1";

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
     * 超时时间
     */
    private long timeOut = 60L * 1000L;

    private long leakDetectionThreshold;

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

    /**
     * 字符编码
     */
    private String characterEncoding = "UTF-8";

    /**
     * 是否开启 SSL
     */
    private Boolean useSSL;

    /**
     * tinyInt(1) 是否转为 boolean，默认为 true
     * false 为原值
     */
    private Boolean tinyInt1isBit;

    /**
     * 时区
     */
    private String serverTimezone;

    /**
     * bit(1) 查询数据时是否转为 boolean 值，默认true
     * false 为 bit(1)输出为int类型
     */
    private Boolean bit1isBoolean;

    /**
     * 是否将 json 字段转为 map 或 list，默认为 false：输出字符串
     */
    private Boolean jsonToObject;

    public BaseMySqlDatabaseInfo() {
    }

    public BaseMySqlDatabaseInfo(String userName, String password, String serviceUrl, String port, String databaseName) {
        this.userName = userName;
        this.password = password;
        this.serviceUrl = serviceUrl;
        this.port = port;
        this.databaseName = databaseName;
    }

    public BaseMySqlDatabaseInfo(String userName, String password, String serviceUrl, String port, String databaseName, String characterEncoding, Boolean useSSL) {
        this.userName = userName;
        this.password = password;
        this.serviceUrl = serviceUrl;
        this.port = port;
        this.databaseName = databaseName;
        this.characterEncoding = characterEncoding;
        this.useSSL = useSSL;
    }

    public BaseMySqlDatabaseInfo(String userName, String password, String serviceUrl, String port, String databaseName, String characterEncoding, Boolean useSSL, Boolean tinyInt1isBit, String serverTimezone, Boolean bit1isBoolean, Boolean jsonToObject) {
        this.userName = userName;
        this.password = password;
        this.serviceUrl = serviceUrl;
        this.port = port;
        this.databaseName = databaseName;
        this.characterEncoding = characterEncoding;
        this.useSSL = useSSL;
        this.tinyInt1isBit = tinyInt1isBit;
        this.serverTimezone = serverTimezone;
        this.bit1isBoolean = bit1isBoolean;
        this.jsonToObject = jsonToObject;
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

    public long getTimeOut() {
        return timeOut;
    }

    public void setTimeOut(long timeOut) {
        this.timeOut = timeOut;
    }

    public String getServerTimezone() {
        return serverTimezone;
    }

    public void setServerTimezone(String serverTimezone) {
        this.serverTimezone = serverTimezone;
    }

    public Boolean getTinyInt1isBit() {
        return tinyInt1isBit;
    }

    public void setTinyInt1isBit(Boolean tinyInt1isBit) {
        this.tinyInt1isBit = tinyInt1isBit;
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

    @Override
    public String getServerAddr() {
        return String.format(SERVER_ADDR_TEMP, serviceUrl, port);
    }

    @Override
    public String getDbName() {
        return databaseName;
    }

    public long getLeakDetectionThreshold() {
        return leakDetectionThreshold;
    }

    public void setLeakDetectionThreshold(long leakDetectionThreshold) {
        this.leakDetectionThreshold = leakDetectionThreshold;
    }
}
