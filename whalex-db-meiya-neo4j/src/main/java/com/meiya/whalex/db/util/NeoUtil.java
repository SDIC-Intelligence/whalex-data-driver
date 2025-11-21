package com.meiya.whalex.db.util;

import com.meiya.whalex.db.template.graph.Neo4jDbTemplate;
import org.neo4j.driver.*;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Neo4j Util
 *
 * @author chenjp
 * @date 2020/9/27
 */
public class NeoUtil {


    private static final Map<String, Object> driverMap = new ConcurrentHashMap<>();
    private static final Config config;
    public static Driver driver = null;
    private static final String BOLT = "bolt://";

    static {
        config = Config.builder()
                //  允许由连接池管理的每个主机的最大连接总数
                .withMaxConnectionPoolSize(50)
                //  会话从连接池请求时等待的最长时间
                .withConnectionAcquisitionTimeout(2, TimeUnit.MINUTES)
                //  托管事务在失败之前重试的最长时间
                .withMaxTransactionRetryTime(10L, TimeUnit.SECONDS)
                //  从池中删除之前，驱动程序将保持连接的最长时间
                .withMaxConnectionLifetime(30, TimeUnit.SECONDS)
                //  等待建立TCP连接的最长时间
                .withConnectionTimeout(60, TimeUnit.SECONDS)
                .build();
    }

    public static void saveDriver(Neo4jDbTemplate connect, Driver driver) {
        driverMap.put(connect.getServiceUrl(), driver);
    }

    /**
     * 获取Neo4j驱动
     */
    public static Driver getDriver(String key) {
        return driverMap.containsKey(key) ? (Driver) NeoUtil.driverMap.get(key) : null;
    }

    /**
     * 获取Neo4j驱动
     */
    public static Driver getDriver(String server, String username, String password) {
        driver = GraphDatabase.driver(BOLT + server, AuthTokens.basic(username, password), config);
        return driver;
    }

    /**
     * 获取Neo4j驱动
     */
    public static Driver getDriver(Neo4jDbTemplate connect) {
        Driver driver = NeoUtil.driver;
        if (driver == null) {
            driver = getDriver(connect.getServiceUrl()) != null ? getDriver(connect.getServiceUrl()) :
                    getDriver(connect.getServiceUrl(), connect.getUsername(), connect.getPassword());
        }
        if (!driverMap.containsKey(connect.getServiceUrl())) {
            saveDriver(connect, driver);
        }
        return driver;
    }

    /**
     * 获取Session
     */
    public static Session getSession(Driver driver) {
        return driver.session();
    }

    /**
     * 开启事务
     */
    public static Transaction beginTx(Session session) {
        return session.beginTransaction();
    }
}
