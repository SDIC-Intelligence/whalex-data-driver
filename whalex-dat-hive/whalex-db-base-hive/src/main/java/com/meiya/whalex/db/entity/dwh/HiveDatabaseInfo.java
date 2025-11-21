package com.meiya.whalex.db.entity.dwh;

import com.meiya.whalex.db.entity.AbstractDatabaseInfo;
import lombok.Data;

/**
 * Hive 数据库配置信息实体
 *
 * @author 黄河森
 * @date 2019/12/26
 * @project whale-cloud-platformX
 */
@Data
public class HiveDatabaseInfo extends AbstractDatabaseInfo {

    /**
     * hive server连接地址模板
     */
    public static String HIVE_SERVER_URL_TEMPLATE = "jdbc:hive2://%s/%s";

    public static String HIVE_SERVER_URL_TEMPLATE2 = "jdbc:hive2://%s";

    /**
     * hive server连接地址模板
     */
    public static String HIVE_AUTH_SERVER_URL_TEMPLATE = "jdbc:hive2://%s/%s;principal=%s;kerberosAuthType=kerberos";

    public static String HIVE_AUTH_SERVER_URL_TEMPLATE2 = "jdbc:hive2://%s;principal=%s;kerberosAuthType=kerberos";

    /**
     * zk 连接方式
     */
    public static String HIVE_ZK_URL_TEMPLATE = "jdbc:hive2://%s/%s;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=%s";

    public static String HIVE_ZK_URL_TEMPLATE2 = "jdbc:hive2://%s;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=%s";

    /**
     * 认证方式连接地址模板
     */
    public static String AUTH_ZK_URL_TEMPLATE = "jdbc:hive2://%s/%s;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=%s;principal=%s";

    public static String AUTH_ZK_URL_TEMPLATE2 = "jdbc:hive2://%s;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=%s;principal=%s";
    /**
     * 驱动类
     */
    public static String DRIVER_CLASS_NAME = "org.apache.hive.jdbc.HiveDriver";

    /**
     * zk 连接类型
     */
    public static String ZK_LINK_TYPE = "zk";

    /**
     * hive server 连接类型
     */
    public static String HIVE_SERVER_LINK_TYPE = "hiveserver";

    /**
     * poolPreparedStatements
     */
    public static boolean POOL_PREPARED_STATEMENTS = false;

    /**
     * 任务引擎
     */
    public static String TEZ_JOB = "tez";

    public static String MR_JOB = "mr";

    /**
     * mapreduce
     */
    public static String YARN_MAP_MEMORY = "set mapreduce.map.memory.mb=%s";
    public static String YARN_REDUCE_MEMORY = "set mapreduce.reduce.memory.mb=%s";

    public static String TEZ_CONTAINER_SIZE = "set hive.tez.container.size=%s";
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
    private String databaseName;

    /**
     * 认证证书
     */
    private String certificateId;
    /**
     * krb5 地址
     */
    private String krb5Path;
    /**
     * userKeytab 地址
     */
    private String userKeytabPath;

    /**
     * 认证类型
     */
    private String authType;

    /**
     * kerberos.principal
     */
    private String kerberosPrincipal;

    /**
     * zk 命名空间
     */
    private String zooKeeperNamespace;

    /**
     * 队列名称
     */
    private String queueName;

    /**
     * 连接方式
     * zk，hiveserver
     */
    private String linkType;

    /**
     * 任务引擎
     */
    private String jobEngine;

    /**
     * 从数据库下载证书时，保存的父级目录
     */
    private String parentDir;

    private String certificateBase64Str;

    private String metaStoreUrl;

    private int metaStoreMaxConnectionCount;

    private boolean metaStoreExecuteSetugi;

    @Override
    public String getServerAddr() {
        return serviceUrl;
    }

    @Override
    public String getDbName() {
        return databaseName;
    }
}
