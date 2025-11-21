package com.meiya.whalex.db.entity.cache;

import com.meiya.whalex.exception.BusinessException;
import lombok.extern.slf4j.Slf4j;

import static org.apache.commons.lang3.StringUtils.isNotBlank;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.redisson.config.ClusterServersConfig;
import org.redisson.config.Config;
import org.redisson.config.SingleServerConfig;
import redis.clients.jedis.*;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Redis Config
 *
 * @author chenjp
 * @date 2020/9/10
 */
@Slf4j
public class RedisConfig {

    public static final GenericObjectPoolConfig jedisPoolConfig;
    public static final Integer TIMEOUT = 5000;
    public static final Integer SO_TIMEOUT = 2000;
    public static final Integer MAX_ATTEMPTS = 3;

    static {
        jedisPoolConfig = new JedisPoolConfig();
        //  若设置-1，则表示不受限制
        jedisPoolConfig.setMaxTotal(20);
        //  控制一个pool最多有多少个状态为idle的jedis实例
        jedisPoolConfig.setMaxIdle(20);
        //  控制一个pool最少空闲的连接数
        jedisPoolConfig.setMinIdle(0);
        //  当资源池满时，调用者是否要等待。只有为true时，maxWaitMillis才能生效
        jedisPoolConfig.setBlockWhenExhausted(true);
        //  最大的等待时间，默认60s
        jedisPoolConfig.setMaxWaitMillis(60000);
        //  向资源池借用连接时是否做连接有效性检测(ping)，业务量大的时候建议false，不然会多一次ping的开销
        jedisPoolConfig.setTestOnBorrow(false);
        //  是否提前进行有效性检查
        jedisPoolConfig.setTestOnReturn(false);
        //  是否开启jmx监控
        jedisPoolConfig.setJmxEnabled(true);
        //  是否开启空闲资源监测
        jedisPoolConfig.setTestWhileIdle(true);
        //  空闲资源的检测周期
        //jedisPoolConfig.setTimeBetweenEvictionRunsMillis(-1);
    }


    public static RedissonClient getRedissonClient(List<String> servers, String password, Integer dbIndex) {

        Config config = new Config();
        config.setCodec(new StringCodec());

        if(servers.size() == 1) {
            SingleServerConfig singleServerConfig = config.useSingleServer();
            singleServerConfig.setAddress("redis://" + servers.get(0));
            if (isNotBlank(password)) {
                singleServerConfig.setPassword(password);
            }
            if (dbIndex != null) {
                singleServerConfig.setDatabase(dbIndex);
            }
        }else {
            ClusterServersConfig clusterServersConfig = config.useClusterServers();
            for (String server : servers) {
                clusterServersConfig.addNodeAddress("redis://" + server);
            }
            if (isNotBlank(password)) {
                clusterServersConfig.setPassword(password);
            }
        }

        return Redisson.create(config);
    }

    /**
     * 单机模式，使用 Jedis
     */
    public static Jedis getJedis(String server, String username, String password, Integer dbIndex) {
        String[] ss = split(server);
        String host = ss[0].split(":")[0];
        int port = Integer.parseInt(ss[0].split(":")[1]);
        Jedis jedis = new Jedis(host, port);
        if (isNotBlank(username) && isNotBlank(password)) {
            jedis.auth(username, password);
        } else if (isNotBlank(password)) {
            jedis.auth(password);
        }
        if (dbIndex != null) {
            jedis.select(dbIndex);
        }
        return jedis;
    }

    /**
     * 真集群模式，使用 JedisCluster
     */
    public static JedisClusterExt getJedisClient(List<String> servers, String username, String password, Integer dbIndex) {
        Set<HostAndPort> hostAndPorts = new HashSet<>();
        servers.forEach(server -> {
            String[] ss = split(server);
            String host = ss[0].split(":")[0];
            int port = Integer.parseInt(ss[0].split(":")[1]);
            HostAndPort hostAndPort = new HostAndPort(host, port);
            hostAndPorts.add(hostAndPort);
        });
        return isNotBlank(password) ?
                (isNotBlank(username) ?
                        new JedisClusterExt(hostAndPorts, TIMEOUT, SO_TIMEOUT, MAX_ATTEMPTS, username, password, null, jedisPoolConfig) : new JedisClusterExt(hostAndPorts, TIMEOUT, SO_TIMEOUT, MAX_ATTEMPTS, password, null, jedisPoolConfig)) :
                new JedisClusterExt(hostAndPorts, TIMEOUT, SO_TIMEOUT, MAX_ATTEMPTS, jedisPoolConfig);
    }


    /**
     * 假集群模式，使用 ShardedJedis
     */
    public static ShardedJedisPool getShardedJedis(List<String> servers, String username, String password, Integer dbIndex) {
        List<JedisShardInfo> shards = new ArrayList<>();
        servers.forEach(server -> {
            String[] ss = split(server);
            String shardName = ss[1];
            String host = ss[0].split(":")[0];
            int port = Integer.parseInt(ss[0].split(":")[1]);
            JedisShardInfo shardInfo = new JedisShardInfo(host, port, "SHARD-" + shardName + "-NODE");
            if (dbIndex != null) {
                try {
                    Field db = JedisShardInfo.class.getDeclaredField("db");
                    db.setAccessible(true);
                    db.set(shardInfo, dbIndex);
                } catch (Exception e) {
                    throw new BusinessException("Shard Redis 设置 DB 失败!", e);
                }
            }
            shards.add(shardInfo);
        });
        log.info("redis server: " + shards);
        return initShardedPool(shards, true, username, password);
    }

    public static ShardedJedisPool initShardedPool(List<JedisShardInfo> shards, boolean checkRedisConfig, String username, String password) {
        for (JedisShardInfo shardInfo : shards) {
            shardInfo.setSoTimeout(5);
            if (isNotBlank(username)) {
                shardInfo.setUser(username);
            }
            if (isNotBlank(password)) {
                shardInfo.setPassword(password);
            }
        }
        GenericObjectPoolConfig<ShardedJedis> config = jedisPoolConfig;
        if (!checkRedisConfig) {
            config.setMaxTotal(100);
            config.setMaxIdle(1000);
            config.setTestOnBorrow(false);
            config.setTestOnReturn(false);
        }
        return new ShardedJedisPool(config, shards);
    }

    private static String[] split(String serverUrl) {
        return serverUrl.split("_");
    }

}
