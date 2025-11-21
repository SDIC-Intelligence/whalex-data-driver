package com.meiya.whalex.cache.module;

import com.meiya.whalex.annotation.DbService;
import com.meiya.whalex.business.entity.DatabaseConf;
import com.meiya.whalex.cache.entity.CacheEntity;
import com.meiya.whalex.db.entity.AbstractCursorCache;
import com.meiya.whalex.db.entity.AbstractDatabaseInfo;
import com.meiya.whalex.db.entity.AbstractDbHandler;
import com.meiya.whalex.db.entity.AbstractDbTableInfo;
import com.meiya.whalex.db.entity.DatabaseSetting;
import com.meiya.whalex.db.entity.TableSetting;
import com.meiya.whalex.db.module.AbstractDbModuleBaseService;
import com.meiya.whalex.db.module.DatabaseExecuteStatementLog;
import com.meiya.whalex.exception.BusinessException;
import com.meiya.whalex.exception.ExceptionCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.skywalking.apm.toolkit.trace.Trace;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 缓存统一抽象接口
 *
 * @author 蔡荣桂
 * @date 2023/10/31
 * @package com.meiya.whalex.cache.module
 * @project whalex-data-driver
 * @description CacheService
 */
@Slf4j
public abstract class AbstractCacheBaseService<S,
        Q extends AbstractDbHandler,
        D extends AbstractDatabaseInfo,
        T extends AbstractDbTableInfo,
        C extends AbstractCursorCache> extends AbstractDbModuleBaseService<S, Q, D, T, C> implements CacheService {

    /**
     * 回调方法
     *
     * @param <S>
     * @param <CE>
     * @param <D>
     * @param <T>
     */
    public interface DbCacheModuleCallback<S, CE extends CacheEntity, D extends AbstractDatabaseInfo, T extends AbstractDbTableInfo, R> {

        /**
         * 回调方法
         *
         * @param connect
         * @param cacheEntity
         * @param databaseConf
         * @param tableConf
         * @return
         * @throws Exception
         */
        @Trace
        R doWithExecute(S connect, CE cacheEntity, D databaseConf, T tableConf) throws Exception;
    }


    @Override
    public Set<String> keys(DatabaseSetting databaseSetting, TableSetting tableSetting, String pattern) throws Exception {

        CacheEntity  ce = new CacheEntity();
        ce.setKey(pattern);

        return interiorCacheExecute(databaseSetting, tableSetting, ce, new DbCacheModuleCallback<S, CacheEntity, D, T, Set<String>>() {
            @Override
            public Set<String> doWithExecute(S connect, CacheEntity cacheEntity, D databaseConf, T tableConf) throws Exception {
                return keys(connect, cacheEntity, databaseConf, tableConf);
            }
        });
    }


    @Override
    public Long dbSize(DatabaseSetting databaseSetting, TableSetting tableSetting) throws Exception {
        return interiorCacheExecute(databaseSetting, tableSetting, null, new DbCacheModuleCallback<S, CacheEntity, D, T, Long>() {
            @Override
            public Long doWithExecute(S connect, CacheEntity cacheEntity, D databaseConf, T tableConf) throws Exception {
                return dbSize(connect, databaseConf, tableConf);
            }
        });
    }

    @Override
    public Long exists(DatabaseSetting databaseSetting, TableSetting tableSetting, String... key) throws Exception {
        CacheEntity  ce = new CacheEntity();
        ce.setKeys(key);

        return interiorCacheExecute(databaseSetting, tableSetting, ce, new DbCacheModuleCallback<S, CacheEntity, D, T, Long>() {
            @Override
            public Long doWithExecute(S connect, CacheEntity cacheEntity, D databaseConf, T tableConf) throws Exception {
                return exists(connect, cacheEntity, databaseConf, tableConf);
            }
        });
    }

    @Override
    public Long del(DatabaseSetting databaseSetting, TableSetting tableSetting, String... key) throws Exception {
        CacheEntity  ce = new CacheEntity();
        ce.setKeys(key);

        return interiorCacheExecute(databaseSetting, tableSetting, ce, new DbCacheModuleCallback<S, CacheEntity, D, T, Long>() {
            @Override
            public Long doWithExecute(S connect, CacheEntity cacheEntity, D databaseConf, T tableConf) throws Exception {
                return del(connect, cacheEntity, databaseConf, tableConf);
            }
        });
    }

    @Override
    public void type(DatabaseSetting databaseSetting, TableSetting tableSetting, String key) {
        throw new RuntimeException("接口未实现...");
    }

    @Override
    public void move(DatabaseSetting databaseSetting, TableSetting tableSetting, String key, String db) {
        throw new RuntimeException("接口未实现...");
    }

    @Override
    public void ttl(DatabaseSetting databaseSetting, TableSetting tableSetting, String key) {
        throw new RuntimeException("接口未实现...");
    }

    @Override
    public void expire(DatabaseSetting databaseSetting, TableSetting tableSetting, String key, int seconds) {
        throw new RuntimeException("接口未实现...");
    }

    @Override
    public void pexpire(DatabaseSetting databaseSetting, TableSetting tableSetting, String key, int milliseconds) {
        throw new RuntimeException("接口未实现...");
    }

    @Override
    public void persist(DatabaseSetting databaseSetting, TableSetting tableSetting, String key) {
        throw new RuntimeException("接口未实现...");
    }

    @Override
    public void rename(DatabaseSetting databaseSetting, TableSetting tableSetting, String key, String newKey) {
        throw new RuntimeException("接口未实现...");
    }

    @Override
    public String set(DatabaseSetting databaseSetting, TableSetting tableSetting, String key, String value) throws Exception {
        CacheEntity  ce = new CacheEntity();
        ce.setKey(key);
        ce.setValue(value);

        return interiorCacheExecute(databaseSetting, tableSetting, ce, new DbCacheModuleCallback<S, CacheEntity, D, T, String>() {
            @Override
            public String doWithExecute(S connect, CacheEntity cacheEntity, D databaseConf, T tableConf) throws Exception {
                return set(connect, cacheEntity, databaseConf, tableConf);
            }
        });
    }

    @Override
    public String get(DatabaseSetting databaseSetting, TableSetting tableSetting, String key) throws Exception {
        CacheEntity  ce = new CacheEntity();
        ce.setKey(key);

        return interiorCacheExecute(databaseSetting, tableSetting, ce, new DbCacheModuleCallback<S, CacheEntity, D, T, String>() {
            @Override
            public String doWithExecute(S connect, CacheEntity cacheEntity, D databaseConf, T tableConf) throws Exception {
                return get(connect, cacheEntity, databaseConf, tableConf);
            }
        });
    }

    @Override
    public void incr(DatabaseSetting databaseSetting, TableSetting tableSetting, String key) {
        throw new RuntimeException("接口未实现...");
    }

    @Override
    public void decr(DatabaseSetting databaseSetting, TableSetting tableSetting, String key) {
        throw new RuntimeException("接口未实现...");
    }

    @Override
    public String mset(DatabaseSetting databaseSetting, TableSetting tableSetting, String ...keysValues) throws Exception {
        CacheEntity  ce = new CacheEntity();
        ce.setKeysValues(keysValues);

        return interiorCacheExecute(databaseSetting, tableSetting, ce, new DbCacheModuleCallback<S, CacheEntity, D, T, String>() {
            @Override
            public String doWithExecute(S connect, CacheEntity cacheEntity, D databaseConf, T tableConf) throws Exception {
                return mset(connect, cacheEntity, databaseConf, tableConf);
            }
        });
    }

    @Override
    public List<String> mget(DatabaseSetting databaseSetting, TableSetting tableSetting, String... key) throws Exception {
        CacheEntity  ce = new CacheEntity();
        ce.setKeys(key);

        return interiorCacheExecute(databaseSetting, tableSetting, ce, new DbCacheModuleCallback<S, CacheEntity, D, T, List<String>>() {
            @Override
            public List<String> doWithExecute(S connect, CacheEntity cacheEntity, D databaseConf, T tableConf) throws Exception {
                return mget(connect, cacheEntity, databaseConf, tableConf);
            }
        });
    }

    @Override
    public void strlen(DatabaseSetting databaseSetting, TableSetting tableSetting, String key) {
        throw new RuntimeException("接口未实现...");
    }

    @Override
    public void append(DatabaseSetting databaseSetting, TableSetting tableSetting, String key, String value) {
        throw new RuntimeException("接口未实现...");
    }

    @Override
    public void getrange(DatabaseSetting databaseSetting, TableSetting tableSetting, String String, int start, int end) {
        throw new RuntimeException("接口未实现...");
    }

    @Override
    public Long sadd(DatabaseSetting databaseSetting, TableSetting tableSetting, String key, String... member) throws Exception {
        CacheEntity  ce = new CacheEntity();
        ce.setKey(key);
        ce.setValues(member);

        return interiorCacheExecute(databaseSetting, tableSetting, ce, new DbCacheModuleCallback<S, CacheEntity, D, T, Long>() {
            @Override
            public Long doWithExecute(S connect, CacheEntity cacheEntity, D databaseConf, T tableConf) throws Exception {
                return sadd(connect, cacheEntity, databaseConf, tableConf);
            }
        });
    }

    @Override
    public List<String> hmget(DatabaseSetting databaseSetting, TableSetting tableSetting, String key, String... fields) throws Exception {
        CacheEntity  ce = new CacheEntity();
        ce.setKey(key);
        ce.setFields(fields);

        return interiorCacheExecute(databaseSetting, tableSetting, ce, new DbCacheModuleCallback<S, CacheEntity, D, T, List<String>>() {
            @Override
            public List<String> doWithExecute(S connect, CacheEntity cacheEntity, D databaseConf, T tableConf) throws Exception {
                return hmget(connect, cacheEntity, databaseConf, tableConf);
            }
        });
    }

    @Override
    public Set<String> smembers(DatabaseSetting databaseSetting, TableSetting tableSetting, String key) throws Exception {
        CacheEntity  ce = new CacheEntity();
        ce.setKey(key);

        return interiorCacheExecute(databaseSetting, tableSetting, ce, new DbCacheModuleCallback<S, CacheEntity, D, T, Set<String>>() {
            @Override
            public Set<String> doWithExecute(S connect, CacheEntity cacheEntity, D databaseConf, T tableConf) throws Exception {
                return smembers(connect, cacheEntity, databaseConf, tableConf);
            }
        });
    }

    @Override
    public void srandmember(DatabaseSetting databaseSetting, TableSetting tableSetting, String key, int count) {
        throw new RuntimeException("接口未实现...");
    }

    @Override
    public Boolean sismember(DatabaseSetting databaseSetting, TableSetting tableSetting, String key, String member) throws Exception {
        CacheEntity  ce = new CacheEntity();
        ce.setKey(key);
        ce.setValue(member);

        return interiorCacheExecute(databaseSetting, tableSetting, ce, new DbCacheModuleCallback<S, CacheEntity, D, T, Boolean>() {
            @Override
            public Boolean doWithExecute(S connect, CacheEntity cacheEntity, D databaseConf, T tableConf) throws Exception {
                return sismember(connect, cacheEntity, databaseConf, tableConf);
            }
        });
    }

    @Override
    public void scard(DatabaseSetting databaseSetting, TableSetting tableSetting, String key) {
        throw new RuntimeException("接口未实现...");
    }

    @Override
    public Long srem(DatabaseSetting databaseSetting, TableSetting tableSetting, String key, String... member) throws Exception {
        CacheEntity  ce = new CacheEntity();
        ce.setKey(key);
        ce.setValues(member);

        return interiorCacheExecute(databaseSetting, tableSetting, ce, new DbCacheModuleCallback<S, CacheEntity, D, T, Long>() {
            @Override
            public Long doWithExecute(S connect, CacheEntity cacheEntity, D databaseConf, T tableConf) throws Exception {
                return srem(connect, cacheEntity, databaseConf, tableConf);
            }
        });
    }

    @Override
    public void spop(DatabaseSetting databaseSetting, TableSetting tableSetting, String key, int count) {
        throw new RuntimeException("接口未实现...");
    }

    @Override
    public void zadd(DatabaseSetting databaseSetting, TableSetting tableSetting, String key, Map<Integer, Object> scoreMambers) {
        throw new RuntimeException("接口未实现...");
    }

    @Override
    public void zscore(DatabaseSetting databaseSetting, TableSetting tableSetting, String key, String member) {
        throw new RuntimeException("接口未实现...");
    }

    @Override
    public void zrange(DatabaseSetting databaseSetting, TableSetting tableSetting, String key, int start, int stop) {
        throw new RuntimeException("接口未实现...");
    }

    @Override
    public void zrangebyscore(DatabaseSetting databaseSetting, TableSetting tableSetting, String key, int min, int max) {
        throw new RuntimeException("接口未实现...");
    }

    @Override
    public void zincrby(DatabaseSetting databaseSetting, TableSetting tableSetting, String key, int increment, String member) {
        throw new RuntimeException("接口未实现...");
    }

    @Override
    public void zcount(DatabaseSetting databaseSetting, TableSetting tableSetting, String key, int min, int max) {
        throw new RuntimeException("接口未实现...");
    }

    @Override
    public void zrem(DatabaseSetting databaseSetting, TableSetting tableSetting, String key, String... member) {
        throw new RuntimeException("接口未实现...");
    }

    @Override
    public void zrank(DatabaseSetting databaseSetting, TableSetting tableSetting, String key, String member) {
        throw new RuntimeException("接口未实现...");
    }

    @Override
    public Long lpush(DatabaseSetting databaseSetting, TableSetting tableSetting, String key, String... value) throws Exception {
        CacheEntity  ce = new CacheEntity();
        ce.setKey(key);
        ce.setValues(value);

        return interiorCacheExecute(databaseSetting, tableSetting, ce, new DbCacheModuleCallback<S, CacheEntity, D, T, Long>() {
            @Override
            public Long doWithExecute(S connect, CacheEntity cacheEntity, D databaseConf, T tableConf) throws Exception {
                return lpush(connect, cacheEntity, databaseConf, tableConf);
            }
        });
    }

    @Override
    public void rpush(DatabaseSetting databaseSetting, TableSetting tableSetting, String key, String... value) {
        throw new RuntimeException("接口未实现...");
    }

    @Override
    public void lset(DatabaseSetting databaseSetting, TableSetting tableSetting, String key, int index, String value) {
        throw new RuntimeException("接口未实现...");
    }

    @Override
    public void lpop(DatabaseSetting databaseSetting, TableSetting tableSetting, String key) {
        throw new RuntimeException("接口未实现...");
    }

    @Override
    public void rpop(DatabaseSetting databaseSetting, TableSetting tableSetting, String key) {
        throw new RuntimeException("接口未实现...");
    }

    @Override
    public void llen(DatabaseSetting databaseSetting, TableSetting tableSetting, String key) {
        throw new RuntimeException("接口未实现...");
    }

    @Override
    public List<String> lrange(DatabaseSetting databaseSetting, TableSetting tableSetting, String key, int start, int stop) throws Exception {
        CacheEntity  ce = new CacheEntity();
        ce.setKey(key);
        ce.setStart(start);
        ce.setStop(stop);

        return interiorCacheExecute(databaseSetting, tableSetting, ce, new DbCacheModuleCallback<S, CacheEntity, D, T, List<String>>() {
            @Override
            public List<String> doWithExecute(S connect, CacheEntity cacheEntity, D databaseConf, T tableConf) throws Exception {
                return lrange(connect, cacheEntity, databaseConf, tableConf);
            }
        });
    }

    @Override
    public void lindex(DatabaseSetting databaseSetting, TableSetting tableSetting, String key, int index) {
        throw new RuntimeException("接口未实现...");
    }

    @Override
    public void lrem(DatabaseSetting databaseSetting, TableSetting tableSetting, String key, int count, String value) {
        throw new RuntimeException("接口未实现...");
    }

    @Override
    public void ltrim(DatabaseSetting databaseSetting, TableSetting tableSetting, String key, int start, int stop) {
        throw new RuntimeException("接口未实现...");
    }

    @Override
    public Long hset(DatabaseSetting databaseSetting, TableSetting tableSetting, String key, String field, String value) throws Exception {
        CacheEntity  ce = new CacheEntity();
        ce.setKey(key);
        ce.setField(field);
        ce.setValue(value);

        return interiorCacheExecute(databaseSetting, tableSetting, ce, new DbCacheModuleCallback<S, CacheEntity, D, T, Long>() {
            @Override
            public Long doWithExecute(S connect, CacheEntity cacheEntity, D databaseConf, T tableConf) throws Exception {
                return hset(connect, cacheEntity, databaseConf, tableConf);
            }
        });
    }

    @Override
    public String hmset(DatabaseSetting databaseSetting, TableSetting tableSetting, String key, Map<String, String> fieldValues) throws Exception {
        CacheEntity  ce = new CacheEntity();
        ce.setKey(key);
        ce.setFieldsValues(fieldValues);

        return interiorCacheExecute(databaseSetting, tableSetting, ce, new DbCacheModuleCallback<S, CacheEntity, D, T, String>() {
            @Override
            public String doWithExecute(S connect, CacheEntity cacheEntity, D databaseConf, T tableConf) throws Exception {
                return hmset(connect, cacheEntity, databaseConf, tableConf);
            }
        });
    }

    @Override
    public void hsetnx(DatabaseSetting databaseSetting, TableSetting tableSetting, String key, String field, String value) {
        throw new RuntimeException("接口未实现...");
    }

    @Override
    public String hget(DatabaseSetting databaseSetting, TableSetting tableSetting, String key, String field) throws Exception {
        CacheEntity  ce = new CacheEntity();
        ce.setKey(key);
        ce.setField(field);

        return interiorCacheExecute(databaseSetting, tableSetting, ce, new DbCacheModuleCallback<S, CacheEntity, D, T, String>() {
            @Override
            public String doWithExecute(S connect, CacheEntity cacheEntity, D databaseConf, T tableConf) throws Exception {
                return hget(connect, cacheEntity, databaseConf, tableConf);
            }
        });
    }

    @Override
    public Map<String, String> hgetall(DatabaseSetting databaseSetting, TableSetting tableSetting, String key) throws Exception {
        CacheEntity  ce = new CacheEntity();
        ce.setKey(key);

        return interiorCacheExecute(databaseSetting, tableSetting, ce, new DbCacheModuleCallback<S, CacheEntity, D, T, Map<String, String> >() {
            @Override
            public Map<String, String>  doWithExecute(S connect, CacheEntity cacheEntity, D databaseConf, T tableConf) throws Exception {
                return hgetall(connect, cacheEntity, databaseConf, tableConf);
            }
        });
    }

    @Override
    public Set<String> hkeys(DatabaseSetting databaseSetting, TableSetting tableSetting, String key) throws Exception {
        CacheEntity  ce = new CacheEntity();
        ce.setKey(key);

        return interiorCacheExecute(databaseSetting, tableSetting, ce, new DbCacheModuleCallback<S, CacheEntity, D, T, Set<String>>() {
            @Override
            public Set<String> doWithExecute(S connect, CacheEntity cacheEntity, D databaseConf, T tableConf) throws Exception {
                return hkeys(connect, cacheEntity, databaseConf, tableConf);
            }
        });
    }

    @Override
    public List<String> hvals(DatabaseSetting databaseSetting, TableSetting tableSetting, String key) throws Exception {
        CacheEntity  ce = new CacheEntity();
        ce.setKey(key);

        return interiorCacheExecute(databaseSetting, tableSetting, ce, new DbCacheModuleCallback<S, CacheEntity, D, T, List<String>>() {
            @Override
            public List<String> doWithExecute(S connect, CacheEntity cacheEntity, D databaseConf, T tableConf) throws Exception {
                return hvals(connect, cacheEntity, databaseConf, tableConf);
            }
        });
    }

    @Override
    public Boolean hexists(DatabaseSetting databaseSetting, TableSetting tableSetting, String key, String field) throws Exception {
        CacheEntity  ce = new CacheEntity();
        ce.setKey(key);
        ce.setField(field);

        return interiorCacheExecute(databaseSetting, tableSetting, ce, new DbCacheModuleCallback<S, CacheEntity, D, T, Boolean>() {
            @Override
            public Boolean doWithExecute(S connect, CacheEntity cacheEntity, D databaseConf, T tableConf) throws Exception {
                return hexists(connect, cacheEntity, databaseConf, tableConf);
            }
        });
    }

    @Override
    public void hlen(DatabaseSetting databaseSetting, TableSetting tableSetting, String key) {
        throw new RuntimeException("接口未实现...");
    }

    @Override
    public void hincrby(DatabaseSetting databaseSetting, TableSetting tableSetting, String key, String field, int increment) {
        throw new RuntimeException("接口未实现...");
    }

    @Override
    public Long hdel(DatabaseSetting databaseSetting, TableSetting tableSetting, String key, String... field) throws Exception {
        CacheEntity  ce = new CacheEntity();
        ce.setKey(key);
        ce.setFields(field);

        return interiorCacheExecute(databaseSetting, tableSetting, ce, new DbCacheModuleCallback<S, CacheEntity, D, T, Long>() {
            @Override
            public Long doWithExecute(S connect, CacheEntity cacheEntity, D databaseConf, T tableConf) throws Exception {
                return hdel(connect, cacheEntity, databaseConf, tableConf);
            }
        });
    }

    @Override
    public Object eval(DatabaseSetting databaseSetting, TableSetting tableSetting, String script, List<String> keys, List<String> args) throws Exception {
        CacheEntity  ce = new CacheEntity();
        if(CollectionUtils.isEmpty(keys)) {
            ce.setKeys(new String[0]);
        }else {
            ce.setKeys(keys.toArray(new String[0]));
        }

        if(CollectionUtils.isEmpty(args)) {
            ce.setValues(new String[0]);
        }else {
            ce.setValues(args.toArray(new String[0]));
        }

        ce.setScript(script);

        return interiorCacheExecute(databaseSetting, tableSetting, ce, new DbCacheModuleCallback<S, CacheEntity, D, T, Object>() {
            @Override
            public Object doWithExecute(S connect, CacheEntity cacheEntity, D databaseConf, T tableConf) throws Exception {
                return eval(connect, cacheEntity, databaseConf, tableConf);
            }
        });
    }

    @Override
    public <Z> Z getReadWriteLock(DatabaseSetting databaseSetting, TableSetting tableSetting, String key) throws Exception {
        CacheEntity  ce = new CacheEntity();
        ce.setKey(key);

        return interiorCacheExecute(databaseSetting, tableSetting, ce, new DbCacheModuleCallback<S, CacheEntity, D, T, Z>() {
            @Override
            public Z doWithExecute(S connect, CacheEntity cacheEntity, D databaseConf, T tableConf) throws Exception {
                return getReadWriteLock(connect, cacheEntity, databaseConf, tableConf);
            }
        });
    }

    @Override
    public <Z> Z getLock(DatabaseSetting databaseSetting, TableSetting tableSetting, String key) throws Exception {
        CacheEntity  ce = new CacheEntity();
        ce.setKey(key);

        return interiorCacheExecute(databaseSetting, tableSetting, ce, new DbCacheModuleCallback<S, CacheEntity, D, T, Z>() {
            @Override
            public Z doWithExecute(S connect, CacheEntity cacheEntity, D databaseConf, T tableConf) throws Exception {
                return getLock(connect, cacheEntity, databaseConf, tableConf);
            }
        });
    }

    @Override
    public <Z> Z createTransaction(DatabaseSetting databaseSetting, TableSetting tableSetting) throws Exception {
        return interiorCacheExecute(databaseSetting, tableSetting, null, new DbCacheModuleCallback<S, CacheEntity, D, T, Z>() {
            @Override
            public Z doWithExecute(S connect, CacheEntity cacheEntity, D databaseConf, T tableConf) throws Exception {
                return createTransaction(connect, cacheEntity, databaseConf, tableConf);
            }
        });
    }

    @Override
    public <Z> Z createBatch(DatabaseSetting databaseSetting, TableSetting tableSetting) throws Exception {
        return interiorCacheExecute(databaseSetting, tableSetting, null, new DbCacheModuleCallback<S, CacheEntity, D, T, Z>() {
            @Override
            public Z doWithExecute(S connect, CacheEntity cacheEntity, D databaseConf, T tableConf) throws Exception {
                return createBatch(connect, cacheEntity, databaseConf, tableConf);
            }
        });
    }

    @Override
    public <Z> Z getBuckets(DatabaseSetting databaseSetting, TableSetting tableSetting) throws Exception {
        return interiorCacheExecute(databaseSetting, tableSetting, null, new DbCacheModuleCallback<S, CacheEntity, D, T, Z>() {
            @Override
            public Z doWithExecute(S connect, CacheEntity cacheEntity, D databaseConf, T tableConf) throws Exception {
                return getBuckets(connect, cacheEntity, databaseConf, tableConf);
            }
        });
    }

    protected <P, R> R interiorCacheExecute(DatabaseSetting databaseSetting, TableSetting tableSetting, CacheEntity cacheEntity, DbCacheModuleCallback<S, CacheEntity, D, T, R> executeCallback) throws Exception {
        Q queryEntity = null;
        S dbConnect = null;
        D dataConf = null;
        T tableConf = null;
        Long databaseCost = 0L;
        Long tableSettingCost = 0L;
        Long paramConditionCost = 0L;
        Long getDbConnectCost = 0L;
        Long invokeDbCost = 0L;
        Long allCost = 0L;
        long allStart = System.currentTimeMillis();
        if (databaseSetting != null) {
            long databaseStart = System.currentTimeMillis();
            // 获取库连接
            if (StringUtils.isNotBlank(databaseSetting.getBigDataResourceId())) {
                dataConf = helper.getDbModuleConfig(databaseSetting.getBigDataResourceId());
            } else {
                DatabaseConf databaseConf = new DatabaseConf();
                databaseConf.setBigdataResourceId(databaseSetting.getBigDataResourceId());
                databaseConf.setConnSetting(databaseSetting.getConnSetting());
                databaseConf.setConnTypeId(databaseSetting.getConnTypeId());
                databaseConf.setResourceName(databaseSetting.getDbType());
                dataConf = helper.getDbModuleConfig(databaseConf);
            }
            if (dataConf == null) {
                throw new BusinessException(ExceptionCode.DATABASE_NULL_EXCEPTION, null);
            }
            long databaseEnd = System.currentTimeMillis();
            databaseCost = databaseEnd - databaseStart;

            String cacheKey = helper.getCacheKey(dataConf, tableConf);
            long start = 0;
            try {
                // 防止线程复用，清除  ThreadLocal
                DatabaseExecuteStatementLog.remove();
                // 连接使用计数
                this.helper.getUseConnectCount().incrementAndGet(cacheKey);
                // 获取连接
                long getDbConnectStart = System.currentTimeMillis();
                dbConnect = helper.getDbConnect(dataConf, tableConf, databaseSetting.getBigDataResourceId(), tableSetting == null ? null : tableSetting.getDbId());
                if (dbConnect == null || dataConf == null) {
                    throw new BusinessException(ExceptionCode.DATABASE_NULL_EXCEPTION, null);
                }
                long getDbConnectEnd = System.currentTimeMillis();
                getDbConnectCost = getDbConnectEnd - getDbConnectStart;
                start = System.currentTimeMillis();
                // 调用组件实现方法
                R queryMethodResult = executeCallback.doWithExecute(dbConnect, cacheEntity, dataConf, tableConf);
                return queryMethodResult;
            } finally {
                // 释放连接计数
                this.helper.getUseConnectCount().decrementAndGet(cacheKey);
                long end = System.currentTimeMillis();
                if (start != 0) {
                    invokeDbCost = end - start;
                }
                allCost = end - allStart;
                //记录原生sql
                recordInvokeMsg(queryEntity, dataConf, tableConf, invokeDbCost, databaseCost, tableSettingCost, paramConditionCost, getDbConnectCost, allCost);

                if (log.isDebugEnabled()) {
                    DbService annotation = this.getClass().getAnnotation(DbService.class);
                    log.debug("db query execute info dbType:[{}] - dbId:[{}] method:[{}] - param:[{}] - cost[{}]ms", annotation.dbType().name(), tableSetting == null ? null : tableSetting.getDbIdOrTableName(), null, queryEntity != null ? queryEntity.getQueryStr() : null, allCost);
                }
            }

        } else {
            throw new BusinessException(ExceptionCode.DB_INFO_NULL_EXCEPTION, null);
        }
    }

    protected abstract Set<String> keys(S connect, CacheEntity cacheEntity, D database, T table) throws Exception;

    protected abstract Long dbSize(S connect, D database, T table) throws Exception;

    protected abstract Long exists(S connect, CacheEntity cacheEntity, D database, T table) throws Exception;

    protected abstract Long sadd(S connect, CacheEntity cacheEntity, D database, T table) throws Exception;

    protected abstract Long srem(S connect, CacheEntity cacheEntity, D database, T table) throws Exception;

    protected abstract List<String> mget(S connect, CacheEntity cacheEntity, D database, T table) throws Exception;

    protected abstract String set(S connect, CacheEntity cacheEntity, D database, T table) throws Exception;

    protected abstract String mset(S connect, CacheEntity cacheEntity, D database, T table) throws Exception;

    protected abstract String get(S connect, CacheEntity cacheEntity, D database, T table) throws Exception;

    protected abstract Set<String> smembers(S connect, CacheEntity cacheEntity, D database, T table) throws Exception;

    protected abstract Boolean sismember(S connect, CacheEntity cacheEntity, D database, T table) throws Exception;

    protected abstract Long hset(S connect, CacheEntity cacheEntity, D database, T table) throws Exception;

    protected abstract Long hdel(S connect, CacheEntity cacheEntity, D database, T table) throws Exception;

    protected abstract String hget(S connect, CacheEntity cacheEntity, D database, T table) throws Exception;

    protected abstract Set<String> hkeys(S connect, CacheEntity cacheEntity, D database, T table) throws Exception;

    protected abstract List<String> hmget(S connect, CacheEntity cacheEntity,  D database, T table) throws Exception;

    protected abstract List<String> hvals(S connect, CacheEntity cacheEntity, D database, T table) throws Exception;

    protected abstract Boolean hexists(S connect, CacheEntity cacheEntity, D database, T table) throws Exception;

    protected abstract String hmset(S connect, CacheEntity cacheEntity, D database, T table) throws Exception;

    protected abstract Long del(S connect, CacheEntity cacheEntity, D database, T table) throws Exception;

    protected abstract Long lpush(S connect, CacheEntity cacheEntity, D database, T table) throws Exception;

    protected abstract Object eval(S connect, CacheEntity cacheEntity, D database, T table) throws Exception;

    protected abstract List<String> lrange(S connect, CacheEntity cacheEntity, D database, T table) throws Exception;

    protected abstract <Z> Z getReadWriteLock(S connect, CacheEntity cacheEntity, D database, T table) throws Exception;

    protected abstract <Z> Z getLock(S connect, CacheEntity cacheEntity, D database, T table) throws Exception;

    protected abstract <Z> Z createTransaction(S connect, CacheEntity cacheEntity, D database, T table) throws Exception;

    protected abstract <Z> Z createBatch(S connect, CacheEntity cacheEntity, D database, T table) throws Exception;

    protected abstract <Z> Z getBuckets(S connect, CacheEntity cacheEntity, D database, T table) throws Exception;

    protected abstract Map<String, String> hgetall(S connect, CacheEntity cacheEntity, D database, T table) throws Exception;
}
