package com.meiya.whalex.db.entity.ani;

import com.meiya.whalex.db.entity.AbstractDatabaseInfo;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author 黄河森
 * @date 2022/7/6
 * @package com.meiya.whalex.db.entity.ani
 * @project whalex-data-driver
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class ClickHouseDatabaseInfo extends AbstractDatabaseInfo {

    /**
     * 连接地址模板
     */
    public static String URL_TEMPLATE = "jdbc:clickhouse://%s/%s";

    /**
     * 驱动类
     */
    public static String DRIVER_CLASS_NAME = "ru.yandex.clickhouse.ClickHouseDriver";

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
     * ip:port
     */
    private String serviceUrl;

    /**
     * 库名
     */
    private String database;

    /**
     * 集群名称
     */
    private String clusterName;

    public ClickHouseDatabaseInfo(String userName, String password, String serviceUrl, String database) {
        this.userName = userName;
        this.password = password;
        this.serviceUrl = serviceUrl;
        this.database = database;
    }

    @Override
    public String getServerAddr() {
        return serviceUrl;
    }

    @Override
    public String getDbName() {
        return database;
    }
}
