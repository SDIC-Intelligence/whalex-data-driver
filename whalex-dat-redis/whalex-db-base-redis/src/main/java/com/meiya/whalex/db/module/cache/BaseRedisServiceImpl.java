package com.meiya.whalex.db.module.cache;

import com.meiya.whalex.annotation.Support;
import com.meiya.whalex.cache.entity.CacheEntity;
import com.meiya.whalex.cache.module.AbstractCacheBaseService;
import com.meiya.whalex.db.constant.SupportPower;
import com.meiya.whalex.db.entity.AbstractCursorCache;
import com.meiya.whalex.db.entity.QueryCursorMethodResult;
import com.meiya.whalex.db.entity.QueryMethodResult;
import com.meiya.whalex.db.entity.cache.*;
import com.meiya.whalex.db.module.DatabaseExecuteStatementLog;
import com.meiya.whalex.exception.BusinessException;
import com.meiya.whalex.exception.ExceptionCode;
import com.meiya.whalex.interior.db.search.condition.Rel;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.redisson.api.RBatch;
import org.redisson.api.RBuckets;
import org.redisson.api.RLock;
import org.redisson.api.RReadWriteLock;
import org.redisson.api.RTransaction;
import redis.clients.jedis.exceptions.JedisAccessControlException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

/**
 * @author chenjp
 * @date 2020/9/8
 */
@Slf4j
@Support(value = {
        SupportPower.TEST_CONNECTION,
        SupportPower.CREATE,
        SupportPower.UPDATE,
        SupportPower.DELETE,
        SupportPower.SEARCH
})
public class BaseRedisServiceImpl extends AbstractCacheBaseService<RedisClient, RedisHandler, RedisDatabaseInfo, RedisTableInfo, AbstractCursorCache> {


    @Override
    protected QueryMethodResult publishMethod(RedisClient connect, RedisHandler queryEntity, RedisDatabaseInfo databaseConf, RedisTableInfo tableConf) {
        String tableName = tableConf.getTableName();
        RedisHandler.RedisPublishData redisPublishData = queryEntity.getRedisPublishData();
        Object value = redisPublishData.getValue();
        QueryMethodResult queryMethodResult = new QueryMethodResult();
        if(value instanceof String) {
            Long publish = connect.publish(tableName, (String) value);
            queryMethodResult.setTotal(publish == null ? 0 : publish);
            return queryMethodResult;
        }else if(value instanceof byte[]) {
            Long publish = connect.publish(tableName.getBytes(), (byte[]) value);
            queryMethodResult.setTotal(publish == null ? 0 : publish);
            return queryMethodResult;
        }
        throw new RuntimeException("redis消息体只支持字符串或字节数组");
    }

    @Override
    protected QueryMethodResult subscribeMethod(RedisClient connect, RedisHandler queryEntity, RedisDatabaseInfo databaseConf, RedisTableInfo tableConf) {
        String tableName = tableConf.getTableName();
        RedisHandler.RedisSubscribeData redisSubscribeData = queryEntity.getRedisSubscribeData();
        boolean async = redisSubscribeData.isAsync();
        Consumer<Object> consumer = redisSubscribeData.getConsumer();
        if(async) {
            new Thread(()->{
                connect.subscribe(tableName, consumer);}
                ).start();
        }else{
            connect.subscribe(tableName, consumer);
        }
        return new QueryMethodResult();
    }

    @Override
    protected QueryMethodResult queryMethod(RedisClient redisClient, RedisHandler redisHandler, RedisDatabaseInfo databaseConf, RedisTableInfo tableConf) throws Exception {
        QueryMethodResult queryMethodResult = new QueryMethodResult();
        RedisHandler.RedisQuery redisQuery = redisHandler.getRedisQuery();


        Object result = "";

        String key = "";
        if(Rel.MGET.equals(redisQuery.getRel())) {
            if(StringUtils.isBlank(redisQuery.getKey())
                    || CollectionUtils.isEmpty(redisQuery.getFields())) {
                throw new BusinessException("redis mget 请求 key["+redisQuery.getKey()+"]或fields为空");
            }

            key = redisQuery.getKey();
            DatabaseExecuteStatementLog.set("mget " + redisQuery.getKey() + " " + redisQuery.getField());
            List<String> resultList = redisClient.mget(redisQuery.getFields().toArray(new String[0]));
            List<String> fields = redisQuery.getFields();
            Map<String, Object> field2value = new LinkedHashMap<>();
            for (int i = 0; i < fields.size(); i++) {
                field2value.put(fields.get(i), resultList.get(i));
            }
            result = field2value;
        }else if(Rel.HMGET.equals(redisQuery.getRel())) {
            if(StringUtils.isBlank(redisQuery.getKey())
                    || CollectionUtils.isEmpty(redisQuery.getFields())) {
                throw new BusinessException("redis hmget 请求 key["+redisQuery.getKey()+"]或fields为空");
            }

            key = redisQuery.getKey();
            DatabaseExecuteStatementLog.set("hmget " + redisQuery.getKey() + " " + redisQuery.getField());
            List<String> resultList = redisClient.hmget(redisQuery.getKey(), redisQuery.getFields().toArray(new String[0]));
            List<String> fields = redisQuery.getFields();
            Map<String, Object> field2value = new LinkedHashMap<>();
            for (int i = 0; i < fields.size(); i++) {
                field2value.put(fields.get(i), resultList.get(i));
            }
            result = field2value;
        }else if(Rel.HGET.equals(redisQuery.getRel())) {

            if(StringUtils.isBlank(redisQuery.getKey())
                    || StringUtils.isBlank(redisQuery.getField())) {
                throw new BusinessException("redis hget 请求 key["+redisQuery.getKey()+"]或field["+redisQuery.getField()+"]为空");
            }

            key = redisQuery.getField();
            DatabaseExecuteStatementLog.set("hget " + redisQuery.getKey() + " " + redisQuery.getField());
            result = redisClient.hget(redisQuery.getKey(), redisQuery.getField());
        }else if(Rel.HKEYS.equals(redisQuery.getRel())){

            if(StringUtils.isBlank(redisQuery.getKey())) {
                throw new BusinessException("redis hkeys 请求 key["+redisQuery.getKey()+"]为空");
            }

            key = redisQuery.getKey();
            DatabaseExecuteStatementLog.set("hkeys " + redisQuery.getKey());
            result = redisClient.hkeys(redisQuery.getKey());
        } else if(Rel.KEYS.equals(redisQuery.getRel())){

            if(StringUtils.isBlank(redisQuery.getKey())) {
                throw new BusinessException("redis keys 请求 key["+redisQuery.getKey()+"]为空");
            }

            key = redisQuery.getKey();
            DatabaseExecuteStatementLog.set("keys " + redisQuery.getKey());
            result = redisClient.keys(redisQuery.getKey());

        }else if(Rel.TTL.equals(redisQuery.getRel())){
            if(StringUtils.isBlank(redisQuery.getKey())) {
                throw new BusinessException("redis ttl 请求 key["+redisQuery.getKey()+"]为空");
            }

            key = redisQuery.getKey();
            DatabaseExecuteStatementLog.set("ttl " + redisQuery.getKey());
            result = redisClient.ttl(redisQuery.getKey());
        }else {
            if(StringUtils.isBlank(redisQuery.getKey())) {
                throw new BusinessException("redis get 请求 key["+redisQuery.getKey()+"]为空");
            }

            key = redisQuery.getKey();
            DatabaseExecuteStatementLog.set("get " + redisQuery.getKey());
            result = redisClient.get(redisQuery.getKey());
        }


        Map<String, Object> map = new HashMap<>();
        List<Map<String, Object>> list = new ArrayList<>();
        map.put(key, result);
        list.add(map);
        queryMethodResult.setRows(list);
        return queryMethodResult;
    }

    @Override
    protected QueryMethodResult countMethod(RedisClient redisClient, RedisHandler redisHandler, RedisDatabaseInfo databaseConf, RedisTableInfo tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "BaseRedisServiceImpl.countMethod");
    }

    @Override
    protected QueryMethodResult testConnectMethod(RedisClient redisClient, RedisDatabaseInfo databaseConf) throws Exception {
        DatabaseExecuteStatementLog.set("keys test");
        try {
            redisClient.get("test");
        } catch (JedisAccessControlException ignored) {
        }
        return new QueryMethodResult();
    }

    @Override
    protected QueryMethodResult showTablesMethod(RedisClient redisClient, RedisHandler redisHandler, RedisDatabaseInfo databaseConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "BaseRedisServiceImpl.showTablesMethod");
    }

    @Override
    protected QueryMethodResult getIndexesMethod(RedisClient redisClient, RedisDatabaseInfo databaseConf, RedisTableInfo tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "BaseRedisServiceImpl.getIndexesMethod");
    }

    @Override
    protected QueryMethodResult createTableMethod(RedisClient redisClient, RedisHandler redisHandler, RedisDatabaseInfo databaseConf, RedisTableInfo tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "BaseRedisServiceImpl.createTableMethod");
    }

    @Override
    protected QueryMethodResult dropTableMethod(RedisClient redisClient, RedisHandler redisHandler, RedisDatabaseInfo databaseConf, RedisTableInfo tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "BaseRedisServiceImpl.dropTableMethod");
    }

    @Override
    protected QueryMethodResult deleteIndexMethod(RedisClient redisClient, RedisHandler redisHandler, RedisDatabaseInfo databaseConf, RedisTableInfo tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "BaseRedisServiceImpl.deleteIndexMethod");
    }

    @Override
    protected QueryMethodResult createIndexMethod(RedisClient redisClient, RedisHandler redisHandler, RedisDatabaseInfo databaseConf, RedisTableInfo tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "BaseRedisServiceImpl.createIndexMethod");
    }

    @Override
    protected QueryMethodResult insertMethod(RedisClient redisClient, RedisHandler redisHandler, RedisDatabaseInfo databaseConf, RedisTableInfo tableConf) throws Exception {

        List<RedisHandler.RedisInsert> redisInsertList = redisHandler.getRedisInsertList();

        for (RedisHandler.RedisInsert redisInsert : redisInsertList) {

            if(Rel.HSET.equals(redisInsert.getRel())) {

                if(StringUtils.isBlank(redisInsert.getKey())
                        || StringUtils.isBlank(redisInsert.getField())) {
                    throw new BusinessException("redis hset 请求 key["+redisInsert.getKey()+"]或field["+redisInsert.getField()+"]为空");
                }
                DatabaseExecuteStatementLog.set("hset " + redisInsert.getKey() + " " + redisInsert.getField() + " " + redisInsert.getValue());
                redisClient.hset(redisInsert.getKey(), redisInsert.getField(), redisInsert.getValue(), redisInsert.getExpire());

            }else {

                if(StringUtils.isBlank(redisInsert.getKey())) {
                    throw new BusinessException("redis set 请求 key["+redisInsert.getKey()+"]为空");
                }
                DatabaseExecuteStatementLog.set("set " + redisInsert.getKey() + " " + redisInsert.getValue());
                redisClient.set(redisInsert.getKey(), redisInsert.getValue(), redisInsert.getExpire());

            }

        }

        return new QueryMethodResult();
    }

    @Override
    protected QueryMethodResult updateMethod(RedisClient redisClient, RedisHandler redisHandler, RedisDatabaseInfo databaseConf, RedisTableInfo tableConf) throws Exception {
        RedisHandler.RedisUpdate redisUpdate = redisHandler.getRedisUpdate();
        DatabaseExecuteStatementLog.set("set " + redisUpdate.getKey() + " " + redisUpdate.getValue());
        switch (redisClient.getType()) {
            case RedisClient.TYPE_0:
                redisClient.getJedis().set(redisUpdate.getKey(), String.valueOf(redisUpdate.getValue()));
                break;
            case RedisClient.TYPE_1:
                redisClient.getShardedJedisPool().getResource().set(redisUpdate.getKey(), String.valueOf(redisUpdate.getValue()));
                break;
            case RedisClient.TYPE_2:
                redisClient.getJedisCluster().set(redisUpdate.getKey(), String.valueOf(redisUpdate.getValue()));
                break;
            default:
                throw new BusinessException("请先配置redis客户端");
        }
        return new QueryMethodResult();
    }

    @Override
    protected QueryMethodResult delMethod(RedisClient redisClient, RedisHandler redisHandler, RedisDatabaseInfo databaseConf, RedisTableInfo tableConf) throws Exception {
        RedisHandler.RedisDel redisDel = redisHandler.getRedisDel();

        if(Rel.HDEL.equals(redisDel.getRel())) {

            if(StringUtils.isBlank(redisDel.getKey())
                    || CollectionUtils.isEmpty(redisDel.getFields())) {
                throw new BusinessException("redis hdel 请求 key["+redisDel.getKey()+"]或fields为空");
            }
            DatabaseExecuteStatementLog.set("hdel " + redisDel.getKey() + " " + redisDel.getField());
            redisClient.hdel(redisDel.getKey(), redisDel.getFields().toArray(new String[0]));

        }else {

            if(StringUtils.isBlank(redisDel.getKey())) {
                throw new BusinessException("redis del 请求 key["+redisDel.getKey()+"]为空");
            }
            DatabaseExecuteStatementLog.set("del " + redisDel.getKey());
            redisClient.del(redisDel.getKey());
        }


        return new QueryMethodResult();
    }

    @Override
    protected QueryMethodResult monitorStatusMethod(RedisClient redisClient, RedisDatabaseInfo databaseConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "BaseRedisServiceImpl.monitorStatusMethod");
    }

    @Override
    protected QueryMethodResult querySchemaMethod(RedisClient redisClient, RedisDatabaseInfo databaseConf, RedisTableInfo tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "BaseRedisServiceImpl.querySchemaMethod");
    }

    @Override
    protected QueryMethodResult queryDatabaseSchemaMethod(RedisClient redisClient, RedisDatabaseInfo databaseConf, RedisTableInfo tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "BaseRedisServiceImpl.queryDatabaseSchemaMethod");
    }

    @Override
    protected QueryCursorMethodResult queryCursorMethod(RedisClient connect, RedisHandler queryEntity, RedisDatabaseInfo databaseConf, RedisTableInfo tableConf, AbstractCursorCache cursorCache, Consumer<Map<String, Object>> consumer, Boolean rollAllData) throws Exception {
        return null;
    }

    @Override
    protected QueryMethodResult saveOrUpdateMethod(RedisClient redisClient, RedisHandler redisHandler, RedisDatabaseInfo databaseConf, RedisTableInfo tableConf) throws Exception {
        return null;
    }

    @Override
    protected QueryMethodResult saveOrUpdateBatchMethod(RedisClient connect, RedisHandler queryEntity, RedisDatabaseInfo databaseConf, RedisTableInfo tableConf) throws Exception {
        return null;
    }

    @Override
    protected QueryMethodResult alterTableMethod(RedisClient connect, RedisHandler queryEntity, RedisDatabaseInfo databaseConf, RedisTableInfo tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "BaseRedisServiceImpl.alterTableMethod");
    }

    @Override
    protected QueryMethodResult tableExistsMethod(RedisClient connect, RedisDatabaseInfo databaseConf, RedisTableInfo tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "BaseRedisServiceImpl.tableExistsMethod");
    }

    @Override
    protected QueryMethodResult queryTableInformationMethod(RedisClient connect, RedisDatabaseInfo databaseConf, RedisTableInfo tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + ".queryTableInformationMethod");
    }

    @Override
    protected QueryMethodResult queryListDatabaseMethod(RedisClient connect, RedisHandler queryEntity, RedisDatabaseInfo databaseConf, RedisTableInfo tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + ".queryListDatabaseMethod");
    }

    @Override
    protected QueryMethodResult queryDatabaseInformationMethod(RedisClient connect, RedisHandler queryEntity, RedisDatabaseInfo databaseConf, RedisTableInfo tableConf) {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + ".queryDatabaseInformationMethod");
    }

    @Override
    protected Set<String> keys(RedisClient connect, CacheEntity cacheEntity, RedisDatabaseInfo database, RedisTableInfo table) throws Exception {
        Set<String> keys = connect.keys(cacheEntity.getKey());
        return keys;
    }

    @Override
    protected Long dbSize(RedisClient connect, RedisDatabaseInfo database, RedisTableInfo table) throws Exception {
        return connect.dbSize();
    }

    @Override
    protected Long exists(RedisClient connect, CacheEntity cacheEntity, RedisDatabaseInfo database, RedisTableInfo table) throws Exception {
        return connect.exists(cacheEntity.getKeys());
    }

    @Override
    protected List<String> mget(RedisClient connect, CacheEntity cacheEntity, RedisDatabaseInfo database, RedisTableInfo table) throws Exception {
        return connect.mget(cacheEntity.getKeys());
    }

    @Override
    protected String set(RedisClient connect, CacheEntity cacheEntity, RedisDatabaseInfo database, RedisTableInfo table) throws Exception {
        return connect.set(cacheEntity.getKey(), cacheEntity.getValue(), cacheEntity.getExpire());
    }

    @Override
    protected String get(RedisClient connect, CacheEntity cacheEntity, RedisDatabaseInfo database, RedisTableInfo table) throws Exception {
        return connect.get(cacheEntity.getKey());
    }

    @Override
    protected String mset(RedisClient connect, CacheEntity cacheEntity, RedisDatabaseInfo database, RedisTableInfo table) throws Exception {
        return connect.mset(cacheEntity.getKeysValues());
    }

    @Override
    protected Long sadd(RedisClient connect, CacheEntity cacheEntity, RedisDatabaseInfo database, RedisTableInfo table) throws Exception {
        return connect.sadd(cacheEntity.getKey(), cacheEntity.getValues());
    }

    @Override
    protected Set<String> smembers(RedisClient connect, CacheEntity cacheEntity, RedisDatabaseInfo database, RedisTableInfo table) throws Exception {
        return connect.smembers(cacheEntity.getKey());
    }

    @Override
    protected Boolean sismember(RedisClient connect, CacheEntity cacheEntity, RedisDatabaseInfo database, RedisTableInfo table) throws Exception {
        return connect.sismember(cacheEntity.getKey(), (String)cacheEntity.getValue());
    }

    @Override
    protected Long srem(RedisClient connect, CacheEntity cacheEntity, RedisDatabaseInfo database, RedisTableInfo table) throws Exception {
        return connect.srem(cacheEntity.getKey(), cacheEntity.getValues());
    }

    @Override
    protected Long hset(RedisClient connect, CacheEntity cacheEntity, RedisDatabaseInfo database, RedisTableInfo table) throws Exception {
        return connect.hset(cacheEntity.getKey(), cacheEntity.getField(), cacheEntity.getValue(), null);
    }

    @Override
    protected Long hdel(RedisClient connect, CacheEntity cacheEntity, RedisDatabaseInfo database, RedisTableInfo table) throws Exception {
        return connect.hdel(cacheEntity.getKey(), cacheEntity.getFields());
    }

    @Override
    protected String hget(RedisClient connect, CacheEntity cacheEntity, RedisDatabaseInfo database, RedisTableInfo table) throws Exception {
        return connect.hget(cacheEntity.getKey(), cacheEntity.getField());
    }

    @Override
    protected Set<String> hkeys(RedisClient connect, CacheEntity cacheEntity, RedisDatabaseInfo database, RedisTableInfo table) throws Exception {
        return connect.hkeys(cacheEntity.getKey());
    }

    @Override
    protected List<String> hmget(RedisClient connect, CacheEntity cacheEntity, RedisDatabaseInfo database, RedisTableInfo table) throws Exception {
        return connect.hmget(cacheEntity.getKey(), cacheEntity.getFields());
    }

    @Override
    protected List<String> hvals(RedisClient connect, CacheEntity cacheEntity, RedisDatabaseInfo database, RedisTableInfo table) throws Exception {
        return connect.hvals(cacheEntity.getKey());
    }

    @Override
    protected Boolean hexists(RedisClient connect, CacheEntity cacheEntity, RedisDatabaseInfo database, RedisTableInfo table) throws Exception {
        return connect.hexists(cacheEntity.getKey(), cacheEntity.getField());
    }

    @Override
    protected String hmset(RedisClient connect, CacheEntity cacheEntity, RedisDatabaseInfo database, RedisTableInfo table) throws Exception {
        return connect.hmset(cacheEntity.getKey(), cacheEntity.getFieldsValues());
    }

    @Override
    protected Long del(RedisClient connect, CacheEntity cacheEntity, RedisDatabaseInfo database, RedisTableInfo table) throws Exception {
        return connect.del(cacheEntity.getKeys());
    }

    @Override
    protected Long lpush(RedisClient connect, CacheEntity cacheEntity, RedisDatabaseInfo database, RedisTableInfo table) throws Exception {
        return connect.lpush(cacheEntity.getKey(), cacheEntity.getValues());
    }

    @Override
    protected Object eval(RedisClient connect, CacheEntity cacheEntity, RedisDatabaseInfo database, RedisTableInfo table) throws Exception {
        return connect.eval(cacheEntity.getScript(), Arrays.asList(cacheEntity.getKeys()), Arrays.asList(cacheEntity.getValues()));
    }

    @Override
    protected List<String> lrange(RedisClient connect, CacheEntity cacheEntity, RedisDatabaseInfo database, RedisTableInfo table) throws Exception {
        return connect.lrange(cacheEntity.getKey(), cacheEntity.getStart(), cacheEntity.getStop());
    }

    @Override
    protected Map<String, String> hgetall(RedisClient connect, CacheEntity cacheEntity, RedisDatabaseInfo database, RedisTableInfo table) throws Exception {
        return connect.hgetAll(cacheEntity.getKey());
    }

    @Override
    protected RBuckets getBuckets(RedisClient connect, CacheEntity cacheEntity, RedisDatabaseInfo database, RedisTableInfo table) throws Exception {
        return connect.getBuckets();
    }

    @Override
    protected RBatch createBatch(RedisClient connect, CacheEntity cacheEntity, RedisDatabaseInfo database, RedisTableInfo table) throws Exception {
        return connect.createBatch();
    }

    @Override
    protected RTransaction createTransaction(RedisClient connect, CacheEntity cacheEntity, RedisDatabaseInfo database, RedisTableInfo table) throws Exception {
        return connect.createTransaction();
    }

    @Override
    protected RReadWriteLock getReadWriteLock(RedisClient connect, CacheEntity cacheEntity, RedisDatabaseInfo database, RedisTableInfo table) throws Exception {
        return connect.getReadWriteLock(cacheEntity.getKey());
    }

    @Override
    protected RLock getLock(RedisClient connect, CacheEntity cacheEntity, RedisDatabaseInfo database, RedisTableInfo table) throws Exception {
        return connect.getLock(cacheEntity.getKey());
    }


}
