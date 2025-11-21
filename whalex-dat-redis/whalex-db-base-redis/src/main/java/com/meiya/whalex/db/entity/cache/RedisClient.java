package com.meiya.whalex.db.entity.cache;

import com.meiya.whalex.exception.BusinessException;
import lombok.Data;

import org.redisson.api.RBatch;
import org.redisson.api.RBuckets;
import org.redisson.api.RLock;
import org.redisson.api.RReadWriteLock;
import org.redisson.api.RTransaction;
import org.redisson.api.RedissonClient;
import org.redisson.api.TransactionOptions;
import redis.clients.jedis.*;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

/**
 * @author chenjp
 * @date 2020/9/8
 */
@Data
public class RedisClient {

    public static final int TYPE_0 = 0;
    public static final int TYPE_1 = 1;
    public static final int TYPE_2 = 2;

    /**
     * 真集群
     */
    private JedisClusterExt jedisCluster;

    /**
     * 真集群用到的host及port
     */
    private Set<HostAndPort> hosts;

    /**
     * 单机模式
     */
    private Jedis jedis;

    /**
     * 伪集群
     */
    private ShardedJedisPool shardedJedisPool;

    /**
     * 密码
     */
    private String password;

    private RedissonClient redissonClient;

    /**
     * type （指当前RedisClient是单机模式、伪集群模式或真集群模式）
     * type 默认 0  ---- 单机
     * type=1 --- 伪集群
     * type=2 --- 真集群
     */
    private int type;

    public RedisClient(Jedis jedis, int type) {
        this.jedis = jedis;
        this.type = type;
    }

    public RedisClient(Jedis jedis,String password, int type) {
        this.jedis = jedis;
        this.password = password;
        this.jedis.auth(password);
        this.type = type;
    }

    public RedisClient(ShardedJedisPool shardedJedisPool, int type) {
        this.shardedJedisPool = shardedJedisPool;
        this.type = type;
    }

    public RedisClient(ShardedJedisPool shardedJedisPool,String password, int type) {
        this.shardedJedisPool = shardedJedisPool;
        this.password = password;

        this.type = type;
    }

    public RedisClient(JedisClusterExt jedisCluster, int type) {
        this.jedisCluster = jedisCluster;
        this.type = type;
    }

    public RedisClient(JedisClusterExt jedisCluster,String password, int type) {
        this.jedisCluster = jedisCluster;
        this.password = password;
        this.type = type;
    }


    public String get(String key) {
        switch (type) {
            case RedisClient.TYPE_0:
                return jedis.get(key);
            case RedisClient.TYPE_1:
                return shardedJedisPool.getResource().get(key);
            case RedisClient.TYPE_2:
                return jedisCluster.get(key);
            default:
                throw new BusinessException("请先配置redis客户端");
        }
    }


    public String hget(String key, String field) {
        switch (type) {
            case RedisClient.TYPE_0:
                return jedis.hget(key, field);
            case RedisClient.TYPE_1:
                return shardedJedisPool.getResource().hget(key, field);
            case RedisClient.TYPE_2:
                return jedisCluster.hget(key, field);
            default:
                throw new BusinessException("请先配置redis客户端");
        }

    }


    public long ttl(String key) {
        switch (type) {
            case RedisClient.TYPE_0:
                return jedis.ttl(key);
            case RedisClient.TYPE_1:
                return shardedJedisPool.getResource().ttl(key);
            case RedisClient.TYPE_2:
                return jedisCluster.ttl(key);
            default:
                throw new BusinessException("请先配置redis客户端");
        }

    }


    public Set<String> hkeys(String key) {
        switch (type) {
            case RedisClient.TYPE_0:
                return jedis.hkeys(key);
            case RedisClient.TYPE_1:
                return shardedJedisPool.getResource().hkeys(key);
            case RedisClient.TYPE_2:
                return jedisCluster.hkeys(key);
            default:
                throw new BusinessException("请先配置redis客户端");
        }
    }

    public List<String> hmget(String key, String ...field) {
        switch (type) {
            case RedisClient.TYPE_0:
                return jedis.hmget(key, field);
            case RedisClient.TYPE_1:
                return shardedJedisPool.getResource().hmget(key, field);
            case RedisClient.TYPE_2:
                return jedisCluster.hmget(key, field);
            default:
                throw new BusinessException("请先配置redis客户端");
        }
    }

    public List<String> hvals(String key) {
        switch (type) {
            case RedisClient.TYPE_0:
                return jedis.hvals(key);
            case RedisClient.TYPE_1:
                return shardedJedisPool.getResource().hvals(key);
            case RedisClient.TYPE_2:
                return jedisCluster.hvals(key);
            default:
                throw new BusinessException("请先配置redis客户端");
        }
    }

    public Long del(String ...key) {
        switch (type) {
            case RedisClient.TYPE_0:
                return jedis.del(key);
            case RedisClient.TYPE_1:
                return shardedJedisPool.getResource().getShard(key[0]).del(key);
            case RedisClient.TYPE_2:
                return jedisCluster.del(key);
            default:
                throw new BusinessException("请先配置redis客户端");
        }
    }

    public Long hdel(String key, String ...field) {
        switch (type) {
            case RedisClient.TYPE_0:
                return jedis.hdel(key, field);
            case RedisClient.TYPE_1:
                return shardedJedisPool.getResource().hdel(key, field);
            case RedisClient.TYPE_2:
                return jedisCluster.hdel(key, field);
            default:
                throw new BusinessException("请先配置redis客户端");
        }
    }

    public Long hset(String key, String field, Object value, Long expire) {
        switch (type) {
            case RedisClient.TYPE_0:
                Long hset = jedis.hset(key, field, String.valueOf(value));
                if (expire != null) {
                    jedis.expire(key, Math.toIntExact(expire));
                }
                return hset;
            case RedisClient.TYPE_1:
                Long hset1 = shardedJedisPool.getResource().hset(key, field, String.valueOf(value));
                if (expire != null) {
                    shardedJedisPool.getResource().expire(key, Math.toIntExact(expire));
                }
                return hset1;
            case RedisClient.TYPE_2:
                Long hset2 = jedisCluster.hset(key, field, String.valueOf(value));
                if (expire != null) {
                    jedisCluster.expire(key, Math.toIntExact(expire));
                }
                return hset2;
            default:
                throw new BusinessException("请先配置redis客户端");
        }
    }

    public String set(String key, Object value, Long expire) {
        switch (type) {
            case RedisClient.TYPE_0:
                String set = jedis.set(key, String.valueOf(value));
                if (expire != null) {
                    jedis.expire(key, Math.toIntExact(expire));
                }
                return set;
            case RedisClient.TYPE_1:
                String set1 = shardedJedisPool.getResource().set(key, String.valueOf(value));
                if (expire != null) {
                    shardedJedisPool.getResource().expire(key, Math.toIntExact(expire));
                }
                return set1;
            case RedisClient.TYPE_2:
                String set2 = jedisCluster.set(key, String.valueOf(value));
                if (expire != null) {
                    jedisCluster.expire(key, Math.toIntExact(expire));
                }
                return set2;
            default:
                throw new BusinessException("请先配置redis客户端");
        }
    }

    public Set<String> keys(String key) {
        switch (type) {
            case RedisClient.TYPE_0:
                return jedis.keys(key);
            case RedisClient.TYPE_1:
                return shardedJedisPool.getResource().getShard(key).keys(key);
            case RedisClient.TYPE_2:
                return jedisCluster.keys(key);
            default:
                throw new BusinessException("请先配置redis客户端");
        }
    }

    public Long dbSize() {
        switch (type) {
            case RedisClient.TYPE_0:
                return jedis.dbSize();
            case RedisClient.TYPE_1:
                return shardedJedisPool.getResource().getShard("").dbSize();
            case RedisClient.TYPE_2:
                return 1L;
            default:
                throw new BusinessException("请先配置redis客户端");
        }
    }


    public Long exists(String ...keys) {
        switch (type) {
            case RedisClient.TYPE_0:
                return jedis.exists(keys);
            case RedisClient.TYPE_1:
                return shardedJedisPool.getResource().getShard(keys[0]).exists(keys);
            case RedisClient.TYPE_2:
                return jedisCluster.exists(keys);
            default:
                throw new BusinessException("请先配置redis客户端");
        }
    }

    public List<String> mget(String ...keys) {
        switch (type) {
            case RedisClient.TYPE_0:
                return jedis.mget(keys);
            case RedisClient.TYPE_1:
                return shardedJedisPool.getResource().getShard(keys[0]).mget(keys);
            case RedisClient.TYPE_2:
                return jedisCluster.mget(keys);
            default:
                throw new BusinessException("请先配置redis客户端");
        }
    }

    public String mset(String ...keysValues) {
        switch (type) {
            case RedisClient.TYPE_0:
                return jedis.mset(keysValues);
            case RedisClient.TYPE_1:
                return shardedJedisPool.getResource().getShard(keysValues[0]).mset(keysValues);
            case RedisClient.TYPE_2:
                return jedisCluster.mset(keysValues);
            default:
                throw new BusinessException("请先配置redis客户端");
        }
    }

    public Long sadd(String key, String ...value) {
        switch (type) {
            case RedisClient.TYPE_0:
                return jedis.sadd(key, value);
            case RedisClient.TYPE_1:
                return shardedJedisPool.getResource().getShard(key).sadd(key, value);
            case RedisClient.TYPE_2:
                return jedisCluster.sadd(key, value);
            default:
                throw new BusinessException("请先配置redis客户端");
        }
    }

    public Set<String> smembers(String key) {
        switch (type) {
            case RedisClient.TYPE_0:
                return jedis.smembers(key);
            case RedisClient.TYPE_1:
                return shardedJedisPool.getResource().getShard(key).smembers(key);
            case RedisClient.TYPE_2:
                return jedisCluster.smembers(key);
            default:
                throw new BusinessException("请先配置redis客户端");
        }
    }

    public Boolean sismember(String key, String value) {
        switch (type) {
            case RedisClient.TYPE_0:
                return jedis.sismember(key, value);
            case RedisClient.TYPE_1:
                return shardedJedisPool.getResource().getShard(key).sismember(key, value);
            case RedisClient.TYPE_2:
                return jedisCluster.sismember(key, value);
            default:
                throw new BusinessException("请先配置redis客户端");
        }
    }

    public Long srem(String key, String ...members) {
        switch (type) {
            case RedisClient.TYPE_0:
                return jedis.srem(key, members);
            case RedisClient.TYPE_1:
                return shardedJedisPool.getResource().getShard(key).srem(key, members);
            case RedisClient.TYPE_2:
                return jedisCluster.srem(key, members);
            default:
                throw new BusinessException("请先配置redis客户端");
        }
    }


    public Boolean hexists(String key, String field) {
        switch (type) {
            case RedisClient.TYPE_0:
                return jedis.hexists(key, field);
            case RedisClient.TYPE_1:
                return shardedJedisPool.getResource().getShard(key).hexists(key, field);
            case RedisClient.TYPE_2:
                return jedisCluster.hexists(key, field);
            default:
                throw new BusinessException("请先配置redis客户端");
        }
    }

    public String hmset(String key, Map<String, String> fieldValue) {
        switch (type) {
            case RedisClient.TYPE_0:
                return jedis.hmset(key, fieldValue);
            case RedisClient.TYPE_1:
                return shardedJedisPool.getResource().getShard(key).hmset(key, fieldValue);
            case RedisClient.TYPE_2:
                return jedisCluster.hmset(key, fieldValue);
            default:
                throw new BusinessException("请先配置redis客户端");
        }
    }

    public Map<String, String> hgetAll(String key) {
        switch (type) {
            case RedisClient.TYPE_0:
                return jedis.hgetAll(key);
            case RedisClient.TYPE_1:
                return shardedJedisPool.getResource().getShard(key).hgetAll(key);
            case RedisClient.TYPE_2:
                return jedisCluster.hgetAll(key);
            default:
                throw new BusinessException("请先配置redis客户端");
        }
    }

    public Long lpush(String key, String ...value) {
        switch (type) {
            case RedisClient.TYPE_0:
                return jedis.lpush(key, value);
            case RedisClient.TYPE_1:
                return shardedJedisPool.getResource().getShard(key).lpush(key, value);
            case RedisClient.TYPE_2:
                return jedisCluster.lpush(key, value);
            default:
                throw new BusinessException("请先配置redis客户端");
        }
    }

    public List<String> lrange(String key, long start, long stop) {
        switch (type) {
            case RedisClient.TYPE_0:
                return jedis.lrange(key, start, stop);
            case RedisClient.TYPE_1:
                return shardedJedisPool.getResource().getShard(key).lrange(key, start, stop);
            case RedisClient.TYPE_2:
                return jedisCluster.lrange(key, start, stop);
            default:
                throw new BusinessException("请先配置redis客户端");
        }
    }

    public RReadWriteLock getReadWriteLock(String key) {
        if(redissonClient == null) {
            throw new RuntimeException("锁功能未开启，请配置useLock = true");
        }

        RReadWriteLock readWriteLock = redissonClient.getReadWriteLock(key);
        return readWriteLock;
    }

    public RLock getLock(String key) {
        if(redissonClient == null) {
            throw new RuntimeException("锁功能未开启，请配置useLock = true");
        }

        return redissonClient.getLock(key);
    }

    public RTransaction createTransaction() {
        if(redissonClient == null) {
            throw new RuntimeException("锁功能未开启，请配置useLock = true");
        }

        RTransaction transaction = redissonClient.createTransaction(TransactionOptions.defaults());
        return transaction;
    }

    public RBatch createBatch() {
        if(redissonClient == null) {
            throw new RuntimeException("锁功能未开启，请配置useLock = true");
        }

        RBatch rBatch = redissonClient.createBatch();
        return rBatch;
    }

    public RBuckets getBuckets() {
        if(redissonClient == null) {
            throw new RuntimeException("锁功能未开启，请配置useLock = true");
        }

        RBuckets buckets = redissonClient.getBuckets();
        return buckets;
    }



    public Object eval(String script, String key) {
        switch (type) {
            case RedisClient.TYPE_0:
                return jedis.eval(script);
            case RedisClient.TYPE_1:
                return shardedJedisPool.getResource().getShard(key).eval(script);
            case RedisClient.TYPE_2:
                return jedisCluster.eval(script, key);
            default:
                throw new BusinessException("请先配置redis客户端");
        }
    }

    public Object eval(String script, List<String> keys, List<String> args) {
        switch (type) {
            case RedisClient.TYPE_0:
                return jedis.eval(script, keys, args);
            case RedisClient.TYPE_1:
                return shardedJedisPool.getResource().getShard("").eval(script, keys, args);
            case RedisClient.TYPE_2:
                return jedisCluster.eval(script, keys, args);
            default:
                throw new BusinessException("请先配置redis客户端");
        }
    }


    public Long publish(String channel, String message) {
        switch (type) {
            case RedisClient.TYPE_0:
                return jedis.publish(channel, message);
            case RedisClient.TYPE_1:
                return shardedJedisPool.getResource().getShard(channel).publish(channel, message);
            case RedisClient.TYPE_2:
                return jedisCluster.publish(channel, message);
            default:
                throw new BusinessException("请先配置redis客户端");
        }
    }


    public void subscribe(String channel, Consumer<Object> consumer) {
        JedisPubSub jedisPubSub = new JedisPubSub() {

            @Override
            public void onMessage(String channel, String message) {
                if(channel.equals(channel)) {
                    consumer.accept(message);
                }
            }
        };

        switch (type) {
            case RedisClient.TYPE_0:
                jedis.subscribe(jedisPubSub, channel);
            case RedisClient.TYPE_1:
                shardedJedisPool.getResource().getShard(channel).subscribe(jedisPubSub, channel);
            case RedisClient.TYPE_2:
                jedisCluster.subscribe(jedisPubSub, channel);
            default:
                throw new BusinessException("请先配置redis客户端");
        }
    }


    public Long publish(byte[] channel, byte[] message) {
        switch (type) {
            case RedisClient.TYPE_0:
                return jedis.publish(channel, message);
            case RedisClient.TYPE_1:
                return shardedJedisPool.getResource().getShard(channel).publish(channel, message);
            case RedisClient.TYPE_2:
                return jedisCluster.publish(channel, message);
            default:
                throw new BusinessException("请先配置redis客户端");
        }
    }








}
