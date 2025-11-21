package com.meiya.whalex.cache.module;

import com.meiya.whalex.db.entity.DatabaseSetting;
import com.meiya.whalex.db.entity.TableSetting;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface CacheService {

    //获取key
    Set<String> keys(DatabaseSetting databaseSetting, TableSetting tableSetting, String pattern) throws Exception;

    //获取键总数
    Long dbSize(DatabaseSetting databaseSetting, TableSetting tableSetting) throws Exception;

    //key是否存在
    Long exists(DatabaseSetting databaseSetting, TableSetting tableSetting, String ...key) throws Exception;

    //删除key
    Long del(DatabaseSetting databaseSetting, TableSetting tableSetting, String ...key) throws Exception;

    //key类型
    void type(DatabaseSetting databaseSetting, TableSetting tableSetting, String key);

    //移动key
    void move(DatabaseSetting databaseSetting, TableSetting tableSetting, String key, String db);

    //key的生命周期
    void ttl(DatabaseSetting databaseSetting, TableSetting tableSetting, String key);

    //设置过期时间
    void expire(DatabaseSetting databaseSetting, TableSetting tableSetting, String key, int seconds);
    void pexpire(DatabaseSetting databaseSetting, TableSetting tableSetting, String key, int milliseconds);

    //设置永不过期
    void persist(DatabaseSetting databaseSetting, TableSetting tableSetting, String key);

    //更改键名称
    void rename(DatabaseSetting databaseSetting, TableSetting tableSetting, String key, String newKey);

    //存放键值
    String set(DatabaseSetting databaseSetting, TableSetting tableSetting, String key, String value) throws Exception;

    //获取键值
    String get(DatabaseSetting databaseSetting, TableSetting tableSetting, String key) throws Exception;

    //值递增
    void incr(DatabaseSetting databaseSetting, TableSetting tableSetting, String key);

    //值递减
    void decr(DatabaseSetting databaseSetting, TableSetting tableSetting, String key);

    //批量存放键值
    String mset(DatabaseSetting databaseSetting, TableSetting tableSetting, String ...keysValues) throws Exception;

    //批量获取键值
    List<String> mget(DatabaseSetting databaseSetting, TableSetting tableSetting, String ...key) throws Exception;

    //获取值长度
    void strlen(DatabaseSetting databaseSetting, TableSetting tableSetting, String key);

    //追加肉容
    void append(DatabaseSetting databaseSetting, TableSetting tableSetting, String key, String value);

    //获取部分字符
    void getrange(DatabaseSetting databaseSetting, TableSetting tableSetting, String String, int start ,int end);

    //存储值
    Long sadd(DatabaseSetting databaseSetting, TableSetting tableSetting, String key, String ...member) throws Exception;

    List<String> hmget(DatabaseSetting databaseSetting, TableSetting tableSetting, String key, String... fields) throws Exception;

    //获取元素
    Set<String> smembers(DatabaseSetting databaseSetting, TableSetting tableSetting, String key) throws Exception;

    //随机获取元素
    void srandmember(DatabaseSetting databaseSetting, TableSetting tableSetting, String key, int count);

    //集合是否存在元素
    Boolean sismember(DatabaseSetting databaseSetting, TableSetting tableSetting, String key, String member) throws Exception;

    //获取集合元素个数
    void scard(DatabaseSetting databaseSetting, TableSetting tableSetting, String key);

    //删除集合元素
    Long srem(DatabaseSetting databaseSetting, TableSetting tableSetting, String key, String ...member) throws Exception;

    //弹出元素
    void spop(DatabaseSetting databaseSetting, TableSetting tableSetting, String key, int count);

    //有序集合
    void zadd(DatabaseSetting databaseSetting, TableSetting tableSetting, String key, Map<Integer, Object> scoreMambers);

    //获取元素分数
    void zscore(DatabaseSetting databaseSetting, TableSetting tableSetting, String key, String member);

    //获取元素
    void zrange(DatabaseSetting databaseSetting, TableSetting tableSetting, String key, int start, int stop);

    //获取指定分数范围元素
    void zrangebyscore(DatabaseSetting databaseSetting, TableSetting tableSetting, String key, int min, int max);

    //增加指定元素分数
    void zincrby(DatabaseSetting databaseSetting, TableSetting tableSetting, String key, int increment, String member);

    //获取指定范围分数个数
    void zcount(DatabaseSetting databaseSetting, TableSetting tableSetting, String key, int min, int max);

    //删除指定元素
    void zrem(DatabaseSetting databaseSetting, TableSetting tableSetting, String key, String ...member);

    //获取元素排名
    void zrank(DatabaseSetting databaseSetting, TableSetting tableSetting, String key, String member);

    //存储元素
    Long lpush(DatabaseSetting databaseSetting, TableSetting tableSetting, String key, String ...value) throws Exception;

    //存储元素
    void rpush(DatabaseSetting databaseSetting, TableSetting tableSetting, String key, String ...value);

    //存储元素
    void lset(DatabaseSetting databaseSetting, TableSetting tableSetting, String key, int index, String value);

    //取出元素
    void lpop(DatabaseSetting databaseSetting, TableSetting tableSetting, String key);

    //取出元素
    void rpop(DatabaseSetting databaseSetting, TableSetting tableSetting, String key);

    //列表长度
    void llen(DatabaseSetting databaseSetting, TableSetting tableSetting, String key);

    //根据范围获取元素
    List<String> lrange(DatabaseSetting databaseSetting, TableSetting tableSetting, String key, int start, int stop) throws Exception;

    //根据索引获取元素
    void lindex(DatabaseSetting databaseSetting, TableSetting tableSetting, String key, int index);

    //删除元素
    void lrem(DatabaseSetting databaseSetting, TableSetting tableSetting, String key, int count, String value);

    //范围删除元素
    void ltrim(DatabaseSetting databaseSetting, TableSetting tableSetting, String key, int start, int stop);

    //存放键值
    Long hset(DatabaseSetting databaseSetting, TableSetting tableSetting, String key, String field, String value) throws Exception;

    //存放键值
    String hmset(DatabaseSetting databaseSetting, TableSetting tableSetting, String key, Map<String, String> fieldValues) throws Exception;

    //存放键值
    void hsetnx(DatabaseSetting databaseSetting, TableSetting tableSetting, String key, String field, String value);

    //获取字段值
    String hget(DatabaseSetting databaseSetting, TableSetting tableSetting, String key, String field) throws Exception;

    //获取键与值
    Map<String, String>  hgetall(DatabaseSetting databaseSetting, TableSetting tableSetting, String key) throws Exception;

    //获取所有的字段
    Set<String> hkeys(DatabaseSetting databaseSetting, TableSetting tableSetting, String key) throws Exception;

    //获取所有的值
    List<String> hvals(DatabaseSetting databaseSetting, TableSetting tableSetting, String key) throws Exception;

    //字段是否存在
    Boolean hexists(DatabaseSetting databaseSetting, TableSetting tableSetting, String key, String field) throws Exception;

    //获取字段数量
    void hlen(DatabaseSetting databaseSetting, TableSetting tableSetting, String key);

    //递增/减
    void hincrby(DatabaseSetting databaseSetting, TableSetting tableSetting, String key, String field, int increment);

    //删除字段
    Long hdel(DatabaseSetting databaseSetting, TableSetting tableSetting, String key, String ...field) throws Exception;

    Object eval(DatabaseSetting databaseSetting, TableSetting tableSetting, String script, List<String> keys, List<String> args) throws Exception;

    <Z> Z getReadWriteLock(DatabaseSetting databaseSetting, TableSetting tableSetting, String key) throws Exception;

    <Z> Z getLock(DatabaseSetting databaseSetting, TableSetting tableSetting, String key) throws Exception;

    <Z> Z createTransaction(DatabaseSetting databaseSetting, TableSetting tableSetting) throws Exception;

    <Z> Z createBatch(DatabaseSetting databaseSetting, TableSetting tableSetting) throws Exception;

    <Z> Z getBuckets(DatabaseSetting databaseSetting, TableSetting tableSetting) throws Exception;


}
