package com.meiya.whalex.db.entity.ani;

import com.meiya.whalex.db.entity.AbstractDatabaseInfo;
import lombok.Data;

/**
 * PostGre 组件数据库配置信息
 *
 * @author 黄河森
 * @date 2019/12/23
 * @project whale-cloud-platformX
 */
@Data
public class BasePostGreDatabaseInfo extends AbstractDatabaseInfo {

    /**
     * 连接地址模板
     */
    public static String URL_TEMPLATE = "jdbc:postgresql://%s:%s/%s";

    /**
     * 驱动类
     */
    public static String DRIVER_CLASS_NAME = "org.postgresql.Driver";

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
    private String database;

    /**
     * 结构
     */
    private String schema;

    /**
     * 是否忽略大小写
     */
    private boolean ignoreCase = Boolean.TRUE;

    /**
     * boolean是否转成int类型 (false 是转， true不转)
     */
    private boolean tinyInt1isBit = Boolean.FALSE;

    private boolean likeToiLike = Boolean.FALSE;

    public BasePostGreDatabaseInfo() {
    }

    public BasePostGreDatabaseInfo(String userName, String password, String serviceUrl, String port, String database, String schema, boolean ignoreCase) {
        this.userName = userName;
        this.password = password;
        this.serviceUrl = serviceUrl;
        this.port = port;
        this.database = database;
        this.schema = schema;
        this.ignoreCase = ignoreCase;
    }

    @Override
    public String getServerAddr() {
        return String.format(SERVER_ADDR_TEMP, serviceUrl, port);
    }

    @Override
    public String getDbName() {
        return String.format(SERVER_ADDR_TEMP, database, schema);
    }
}
