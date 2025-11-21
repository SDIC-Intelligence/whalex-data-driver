package com.meiya.whalex.db.entity.ani;

import lombok.Data;

import java.util.List;

/**
 * MySql 组件数据库配置信息
 *
 * @author 黄河森
 * @date 2019/12/23
 * @project whale-cloud-platformX
 */
@Data
public class DorisDatabaseInfo extends BaseMySqlDatabaseInfo {
    /**
     * 连接测试模板
     */
    public static String TEST_TEMPLATE = "http://%s:%s/api/meta/namespaces/default_cluster/databases/default_cluster:%s/tables";

    List<IpPort> ipPorts;

    /**
     * HTTP 端口
     */
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

    private String catalog;

    public DorisDatabaseInfo(String userName, String password, String serviceUrl, String port, String databaseName, String fePort) {
        super(userName, password, serviceUrl, port, databaseName);
        this.fePort = fePort;
    }

    public DorisDatabaseInfo() {
    }

    public DorisDatabaseInfo(String userName, String password, String serviceUrl, String port, String databaseName, String characterEncoding, Boolean useSSL, String fePort) {
        super(userName, password, serviceUrl, port, databaseName, characterEncoding, useSSL);
        this.fePort = fePort;
    }

    public DorisDatabaseInfo(String userName, String password, String serviceUrl, String port,
                             String databaseName, String characterEncoding, Boolean useSSL, Boolean tinyInt1isBit,
                             String serverTimezone, Boolean bit1isBoolean, Boolean jsonToObject, String fePort) {
        super(userName, password, serviceUrl, port, databaseName, characterEncoding, useSSL, tinyInt1isBit, serverTimezone, bit1isBoolean, jsonToObject);
        this.fePort = fePort;
    }


    @Data
    public static class IpPort{
        private String ip;
        private String port;
        private String fePort;
    }

}
