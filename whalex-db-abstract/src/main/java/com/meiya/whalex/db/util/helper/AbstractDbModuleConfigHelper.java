package com.meiya.whalex.db.util.helper;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.util.ArrayUtil;
import cn.hutool.core.util.IdUtil;
import cn.hutool.crypto.digest.DigestUtil;
import com.github.benmanes.caffeine.cache.Cache;
import com.google.common.util.concurrent.AtomicLongMap;
import com.meiya.whalex.annotation.DatCode;
import com.meiya.whalex.annotation.DbHelper;
import com.meiya.whalex.business.entity.DatabaseConf;
import com.meiya.whalex.business.entity.TableConf;
import com.meiya.whalex.business.service.DatabaseConfService;
import com.meiya.whalex.business.service.TableConfService;
import com.meiya.whalex.cache.DatCaffeine;
import com.meiya.whalex.db.entity.*;
import com.meiya.whalex.exception.BusinessException;
import com.meiya.whalex.exception.ExceptionCode;
import com.meiya.whalex.interior.db.constant.CloudVendorsEnum;
import com.meiya.whalex.interior.db.constant.DbResourceEnum;
import com.meiya.whalex.interior.db.constant.DbVersionEnum;
import com.meiya.whalex.thread.entity.DbThreadBaseConfig;
import com.meiya.whalex.util.collection.MapUtil;
import com.meiya.whalex.util.concurrent.ThreadNamedFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 组件配置信息缓存工具类
 * <p>
 * S 泛型：数据库连接对象
 * D 泛型：数据库配置信息对象
 * T 泛型：表信息对象
 *
 * @author 黄河森
 * @date 2019/9/16
 * @project whale-cloud-platformX
 */
@Slf4j
public abstract class AbstractDbModuleConfigHelper<S, D extends AbstractDatabaseInfo, T extends AbstractDbTableInfo, C extends AbstractCursorCache> implements DbModuleConfigHelper<S, D, T, C> {

    /**
     * 连接对象使用计数
     */
    private final AtomicLongMap<String> useConnectCount = AtomicLongMap.create();

    /**
     * 需要销毁的连接对象
     */
    private Map<String, Set<S>> destroyConnect = new ConcurrentHashMap<>();

    /**
     * 连接缓存销毁锁
     */
    private final ReentrantLock destroyLock = new ReentrantLock();

    /**
     * 连接对象缓存key和组件库标识符映射
     */
    private Map<String, Set<String>> connectCacheKeyToBigResourceId = new ConcurrentHashMap<>();

    /**
     * 最后一次从数据库加载库信息时，组件更新的最晚时间
     */
    private Date lastLoadTimeForDatabase;

    /**
     * 最后一次从数据库加载表信息时，组件更新的最晚时间
     */
    private Date lastLoadTimeForTable;

    /**
     * 组件类型
     */
    private DbResourceEnum dbResourceEnum;

    /**
     * 组件版本
     */
    private DbVersionEnum dbVersionEnum;

    /**
     * 云厂商
     */
    private CloudVendorsEnum cloudVendorsEnum;

    /**
     * 组件配置缓存（bigResourceId）
     */
    private Map<String, D> dbModuleConfigCache;

    /**
     * 表信息配置缓存（bigResourceId）
     */
    private Map<String, T> dbTableConfigCache;

    /**
     * 数据库配置缓存（db配置）
     */
    private Cache<Integer, D> dbConfigCache = DatCaffeine.newBuilder().expireAfterWrite(5, TimeUnit.MINUTES)
            .removalListener((k, v, removalCause) -> {}).build();

    /**
     * 表库配置缓存（db配置）
     */
    private Cache<Integer, T> tableConfigCache = DatCaffeine.newBuilder().expireAfterWrite(5, TimeUnit.MINUTES)
            .removalListener((k, v, removalCause) -> {}).build();

    private Map<Integer, String> keyCache = new ConcurrentHashMap<>();

    /**
     * 数据库链接缓存
     */
    private Cache<String, S> connectCache;

    /**
     * 失败次数计数
     */
    private Map<String, Integer> connectTestFailNumMap = new HashMap<>();

    /**
     * 库表游标查询游标对象缓存
     */
    private Cache<String, C> dbCursorCache;

    /**
     * 事务管理缓存池
     */
    private Cache<String, TransactionManager> transactionManagerCache;

    /**
     * 数据库定义信息查询服务
     */
    private DatabaseConfService databaseConfService;

    /**
     * 数据表定义信息查询服务
     */
    private TableConfService tableConfService;

    /**
     * 线程配置
     */
    protected DbThreadBaseConfig threadConfig;

    /**
     * 定时销毁废弃的连接对象调度池
     */
    private ScheduledThreadPoolExecutor destroyConnectExecutor;


    /**
     * 定时检查资源连接状态
     */
    private ScheduledThreadPoolExecutor checkDataSourceStatusExecutor;

    /**
     * 状态检查单线程池
     */
    private ThreadPoolExecutor checkDataSourceTaskThreadPoolExecutor;

    public void setThreadConfig(DbThreadBaseConfig threadConfig) {
        this.threadConfig = threadConfig;
    }

    public void setTableConfService(TableConfService tableConfService) {
        this.tableConfService = tableConfService;
    }

    public void setDbResourceEnum(DbResourceEnum dbResourceEnum) {
        this.dbResourceEnum = dbResourceEnum;
    }

    public void setDbVersionEnum(DbVersionEnum dbVersionEnum) {
        this.dbVersionEnum = dbVersionEnum;
    }

    public void setCloudVendorsEnum(CloudVendorsEnum cloudVendorsEnum) {
        this.cloudVendorsEnum = cloudVendorsEnum;
    }

    public void setDatabaseConfService(DatabaseConfService databaseConfService) {
        this.databaseConfService = databaseConfService;
    }

    public DatabaseConfService getDatabaseConfService() {
        return databaseConfService;
    }

    public TableConfService getTableConfService() {
        return tableConfService;
    }

    public Map<String, D> getDbModuleConfigCache() {
        return dbModuleConfigCache;
    }

    public AtomicLongMap<String> getUseConnectCount() {
        return useConnectCount;
    }

    /**
     * 初始化配置
     *
     * @param loadDbConf 是否读取数据库配置
     */
    @Override
    public void init(boolean loadDbConf) {

        // 判断当前组件连接对象缓存是否设置过期时间
        DbHelper annotation = this.getClass().getAnnotation(DbHelper.class);
        if (!annotation.isTTLConnectCache()) {
            connectCache = DatCaffeine.newBuilder().build();
        } else {
            // 缓存失效时间
            long ttlTime = annotation.ttlTime();
            // 缓存失效类型
            TimeUnit timeUnit = annotation.ttlType();
            connectCache = DatCaffeine.newBuilder().expireAfterWrite(ttlTime, timeUnit).removalListener((k, v, removalCause) -> {
                // 执行连接销毁
                String key = (String) k;
                S value = (S) v;
                try {
                    log.info("cacheKey: [{}] 组件连接缓存过期，从缓存中剔除，并加入到待销毁队列!", key);
                    destroyLock.lock();
                    Set<S> connectList = destroyConnect.get(key);
                    if (connectList != null) {
                        connectList.add(value);
                    } else {
                        destroyConnect.put(key, CollectionUtil.newHashSet(value));
                    }
                } finally {
                    destroyLock.unlock();
                }
            }).build();
        }

        // 初始化游标查询游标缓存( todo 不同组件的超时时间还是得确认如何设置)
        // 缓存失效时间
        long cursorTtlTime = annotation.cursorTtlTime();
        // 缓存失效类型
        TimeUnit cursorTimeUnit = annotation.cursorTtlType();
        dbCursorCache = DatCaffeine.newBuilder().expireAfterWrite(cursorTtlTime, cursorTimeUnit).removalListener((k, v, removalCause) -> {
            // 如果是过期销毁，则执行连接销毁
            if (removalCause.wasEvicted()) {
                C value = (C) v;
                try {
                    value.closeCursor();
                } catch (Exception e) {
                    log.error("dbType: [{}] 组件游标信息缓存过期，执行游标对象销毁动作失败!", e);
                }
            }
        }).build();

        // 事务管理缓存
        transactionManagerCache = DatCaffeine.newBuilder().expireAfterAccess(5, TimeUnit.MINUTES).removalListener((key, value, removalCause)->{
            if (removalCause.wasEvicted()) {
                TransactionManager transactionManager = (TransactionManager) value;
                try {
                    log.error("UUID：[{}] 对应的事务管理器已失效过期，执行超时方法!", key);
                    transactionManager.transactionTimeOut();
                } catch (Exception e) {
                    log.error("UUID: [{}] 事务管理过期，执行事务管理超时方法失败!", key, e);
                }
            }
        }).build();

        // 启动定时清除废弃连接缓存
        destroyConnectTask();


        if (annotation.needCheckDataSource()) {
            // 启动定时检测连接状态任务
            log.info("当前组件启动定时检测连接状态任务!");
            checkDataSourceStatusTask();
        } else {
            log.info("当前组件未启动定时检测连接状态任务!");
        }

        // 是否从数据库中加载配置信息
        if (loadDbConf) {
            loadResConfig();
        }
    }

    /**
     * 加载数据库配置
     */
    public void loadResConfig() {
        // 获取数据库最新更新时间
        lastLoadTimeForDatabase = queryLastTimeForDatabase();

        // 获取数据库表最新更新时间
        lastLoadTimeForTable = queryLastTimeForTable();

        // 初始化库信息
        Map<String, D> initDbModuleConfig = initDbModuleConfig();
        this.dbModuleConfigCache = initDbModuleConfig;

        // 初始化表信息
        Map<String, T> initTableConfig = initTableConfig();
        if (initTableConfig == null) {
            this.dbTableConfigCache = MapUtil.EMPTY_MAP;
        } else {
            this.dbTableConfigCache = initTableConfig;
        }
    }

    /**
     * 通过大数据组件编码获取组件配置信息
     *
     * @param bigDataResourceId
     * @return
     */
    @Override
    public D getDbModuleConfig(String bigDataResourceId) {
        if (StringUtils.isBlank(bigDataResourceId)) {
            return null;
        }
        return this.dbModuleConfigCache.get(bigDataResourceId);
    }

    /**
     * 通过表信息编码获取表配置信息
     *
     * @param tableId
     * @return
     */
    @Override
    public T getDbTableConfigCache(String tableId) {
        if (StringUtils.isBlank(tableId)) {
            return null;
        }
        return this.dbTableConfigCache.get(tableId);
    }

    @Override
    public S getDbConnect(DbHandleEntity dbHandleEntity) throws Exception {
        return getDbConnect(dbHandleEntity.getDatabaseSetting(), dbHandleEntity.getTableSetting());
    }

    @Override
    public S getDbConnect(DatabaseSetting databaseSetting, TableSetting tableSetting) throws Exception {
        String bigDataResourceId = databaseSetting.getBigDataResourceId();
        if (StringUtils.isNotBlank(bigDataResourceId)) {
            D dbModuleConfig = this.getDbModuleConfig(bigDataResourceId);
            if (dbModuleConfig == null) {
                throw new BusinessException(ExceptionCode.DATABASE_NULL_EXCEPTION, null);
            }
            String dbId = tableSetting != null ? tableSetting.getDbId() : null;
            T dbTableConfigCache = null;
            if (StringUtils.isNotBlank(dbId)) {
                dbTableConfigCache = this.getDbTableConfigCache(dbId);
            } else {
                if (tableSetting != null && tableSetting.getTableName() != null) {
                    TableConf tableConf = new TableConf();
                    tableConf.setTableName(tableSetting.getTableName());
                    tableConf.setTableJson(tableSetting.getTableJson());
                    dbTableConfigCache = getTableConfig(tableConf);
                }
            }
            return getDbConnect(dbModuleConfig, dbTableConfigCache, bigDataResourceId, dbId);
        } else {
            String connSetting = databaseSetting.getConnSetting();
            DatabaseConf databaseConf = new DatabaseConf();
            databaseConf.setConnSetting(connSetting);
            databaseConf.setResourceName(databaseSetting.getDbType());
            databaseConf.setConnTypeId(databaseSetting.getConnTypeId());
            databaseConf.setSecuritySoftwareOrgcode(databaseSetting.getCloudCode());
            D d = getDbModuleConfig(databaseConf);
            T t = null;
            if (tableSetting != null && tableSetting.getTableName() != null) {
                TableConf tableConf = new TableConf();
                tableConf.setTableName(tableSetting.getTableName());
                tableConf.setTableJson(tableSetting.getTableJson());
                t = getTableConfig(tableConf);
            }
            return getDbConnect(d, t, null, null);
        }
    }

    /**
     * 获取连接
     *
     * @param databaseConf
     * @param tableInfo
     * @param bigResourceId
     * @param dbId
     * @return
     */
    @Override
    public S getDbConnect(D databaseConf, T tableInfo, String bigResourceId, String dbId) throws Exception {
        String cacheKey = getCacheKey(databaseConf, tableInfo);
        if (StringUtils.isBlank(cacheKey)) {
            throw new BusinessException(ExceptionCode.CONNECT_CACHE_KEY_NULL_EXCEPTION, null);
        }
        S connect = connectCache.get(cacheKey, (k) -> {
            synchronized (this) {
                S ifPresent = connectCache.getIfPresent(cacheKey);
                if (ifPresent != null) {
                    return ifPresent;
                } else {
                    log.info("db createConnect bigResourceId :[{}] dbId : [{}]", bigResourceId, dbId);
                    S dbConnect = initDbConnect(databaseConf, tableInfo);
                    if (dbConnect == null) {
                        throw new BusinessException(ExceptionCode.CONNECT_NULL_EXCEPTION, null);
                    }
                    return dbConnect;
                }
            }
        });
        if (StringUtils.isNotBlank(bigResourceId)) {
            setConnectCacheKey(cacheKey, bigResourceId, dbId);
        }
        return connect;
    }

    /**
     * 通过缓存Key换错连接，不存在则返回null
     *
     * @param cacheKey
     * @return
     */
    @Override
    public S getDbConnect(String cacheKey) {
        return connectCache.getIfPresent(cacheKey);
    }

    /**
     * 数据库链接缓存key策略
     * 如果 数据库连接 与 表信息无关，即创建数据库连接对象，与具体哪个表的信息无关，则不需要关注 tableInfo
     *
     * @param dbDatabaseConf
     * @param tableInfo
     * @return
     */
    @Override
    public String getCacheKey(D dbDatabaseConf, T tableInfo) {
        Class<? extends AbstractDatabaseInfo> aClass = dbDatabaseConf.getClass();
        Field[] fields = aClass.getDeclaredFields();
        Class<?> superclass = aClass.getSuperclass();
        while (superclass != AbstractDatabaseInfo.class) {
            Field[] declaredFields = superclass.getDeclaredFields();
            if (declaredFields != null && declaredFields.length > 0) {
                fields = ArrayUtil.append(fields, declaredFields);
            }
            superclass = superclass.getSuperclass();
        }
        StringBuilder cacheKey = new StringBuilder();
        for (int i = 0; i < fields.length; i++) {
            Field field = fields[i];
            try {
                field.setAccessible(true);
                Object o = field.get(dbDatabaseConf);
                if (o != null) {
                    if (cacheKey.length() > 0) {
                        cacheKey.append("#");
                    }
                    DatCode datCode = field.getAnnotation(DatCode.class);
                    if(datCode != null) {
                        cacheKey.append(o.hashCode());
                    }else {
                        cacheKey.append(o);
                    }
                }
            } catch (Exception e) {
                log.error("当前获取数据配置参数转换为 cacheKey 时，获取参数值失败， field: [{}]", field.getName(), e);
            }
        }
        return cacheKey.toString();
    }

    /**
     * 获取数据库中库信息最晚更新时间
     *
     * @return
     */
    private Date queryLastTimeForDatabase() {
        return databaseConfService.queryLastUpdateTimeForDatabase(dbResourceEnum.getVal());
    }

    /**
     * 获取数据库中库表信息最晚更新时间
     *
     * @return
     */
    private Date queryLastTimeForTable() {
        return tableConfService.queryLastUpdateTimeForTable(dbResourceEnum.getVal());
    }

    /**
     * 初始化数据库配置
     *
     * @return
     */
    private Map<String, D> initDbModuleConfig() {
        List<DatabaseConf> deploymentConfList = databaseConfService.queryListByDbType(dbResourceEnum.getVal(), null);
        List<DatabaseConf> deploymentConfList1 = getDeploymentConfList(null);
        if (CollectionUtil.isNotEmpty(deploymentConfList1)) {
            deploymentConfList.addAll(deploymentConfList1);
        }
        Map<String, D> cache = new ConcurrentHashMap<>(deploymentConfList.size());
        if (CollectionUtils.isNotEmpty(deploymentConfList)) {
            deploymentConfList.forEach((value) -> {
                try {
                    D dbModuleConfig = getDbModuleConfig(value);
                    if (dbModuleConfig != null) {
                        cache.put(value.getBigdataResourceId(), dbModuleConfig);
                    }
                } catch (Exception e) {
                    log.error("数据库配置解析异常: bigResourceId: [{}] msg: [{}]", value.getBigdataResourceId(), e.getMessage());
                }
            });
        }
        return cache;
    }

    /**
     * 若 dbType 有申明以外的调用这个
     *
     * @return
     */
    protected List<DatabaseConf> getDeploymentConfList(Date updateTime) {
        return null;
    };

    /**
     * 初始化数据库配置
     *
     * @return
     */
    private Map<String, T> initTableConfig() {
        if (CollectionUtil.isEmpty(dbModuleConfigCache)) {
            return null;
        }
        List<TableConf> tableConfList = tableConfService.queryListByBigResourceId(new ArrayList<>(dbModuleConfigCache.keySet()));
        Map<String, T> tableCache = new ConcurrentHashMap<>(tableConfList.size());
        if (CollectionUtils.isNotEmpty(tableConfList)) {
            tableConfList.forEach((value) -> {
                try {
                    T tableConfig = getTableConfig(value);
                    if (tableConfig != null) {
                        tableCache.put(value.getId(), tableConfig);
                    }
                } catch (Exception e) {
                    log.error("数据库表配置解析异常: dbId: [{}] msg: [{}]", value.getId(), e.getMessage());
                }
            });
        }
        return tableCache;
    }

    /**
     * 刷新数据库缓存
     *
     * @param bigDataResourceId
     */
    @Override
    public void refreshDatabaseCache(String bigDataResourceId) {
        DbHelper annotation = this.getClass().getAnnotation(DbHelper.class);
        // 数据库连接与表配置信息相关
        if (annotation.isTableConnect()) {
            // 需要查询与数据库配置相关的所有表配置
            if (StringUtils.isNotBlank(bigDataResourceId)) {
                // 获取新的库配置信息
                DatabaseConf deploymentConf = databaseConfService.queryOneByBigResourceId(bigDataResourceId);
                if (deploymentConf == null) {
                    log.warn("库配置缓存刷新 bigResourceId: [{}] 不存在!", bigDataResourceId);
                    throw new BusinessException(ExceptionCode.DATABASE_NULL_EXCEPTION, null);
                }
                if (deploymentConf.getIsDel() == null || !deploymentConf.getIsDel().equals(1)) {
                    D newDatabaseConfig = getDbModuleConfig(deploymentConf);
                    // 获取旧的库配置
                    D oldDatabaseConfig = this.dbModuleConfigCache.get(bigDataResourceId);
                    checkChangeDatabase(bigDataResourceId, newDatabaseConfig, oldDatabaseConfig);
                    this.dbModuleConfigCache.put(bigDataResourceId, newDatabaseConfig);
                } else {
                    // 从缓存中将无用连接加入待关闭连接缓存中
                    List<TableConf> tableConfList = tableConfService.queryListByBigResourceId(bigDataResourceId);
                    for (int i = 0; i < tableConfList.size(); i++) {
                        TableConf tableConf = tableConfList.get(i);
                        destroyConnectToCache(deploymentConf.getBigdataResourceId(), tableConf.getId());
                        this.dbModuleConfigCache.remove(bigDataResourceId);
                        this.dbTableConfigCache.remove(tableConf.getId());
                    }
                }
            } else {
                // 获取新的库配置信息
                Date updateTime = this.lastLoadTimeForDatabase;
                this.lastLoadTimeForDatabase = queryLastTimeForDatabase();
                List<DatabaseConf> databaseConfList = databaseConfService.queryListByDbType(dbResourceEnum.getVal(), updateTime);
                List<DatabaseConf> deploymentConfList = getDeploymentConfList(updateTime);
                if (CollectionUtil.isNotEmpty(deploymentConfList)) {
                    databaseConfList.addAll(deploymentConfList);
                }
                if (CollectionUtils.isNotEmpty(databaseConfList)) {
                    for (int i = 0; i < databaseConfList.size(); i++) {
                        DatabaseConf deploymentConf = databaseConfList.get(i);
                        if (deploymentConf.getIsDel() == null || !deploymentConf.getIsDel().equals(1)) {
                            // 新库配置
                            D newDatabaseConfig = getDbModuleConfig(deploymentConf);
                            // 获取旧的库配置
                            D oldDatabaseConfig = this.dbModuleConfigCache.get(deploymentConf.getBigdataResourceId());
                            checkChangeDatabase(bigDataResourceId, newDatabaseConfig, oldDatabaseConfig);
                            this.dbModuleConfigCache.put(deploymentConf.getBigdataResourceId(), newDatabaseConfig);
                        } else {
                            // 从缓存中将无用连接加入待关闭连接缓存中
                            List<TableConf> tableConfList = tableConfService.queryListByBigResourceId(bigDataResourceId);
                            for (int t = 0; t < tableConfList.size(); t++) {
                                TableConf tableConf = tableConfList.get(t);
                                destroyConnectToCache(deploymentConf.getBigdataResourceId(), tableConf.getId());
                                this.dbModuleConfigCache.remove(bigDataResourceId);
                                this.dbTableConfigCache.remove(tableConf.getId());
                            }
                        }
                    }
                }
            }
        } else {
            // 数据库连接与表配置信息无相关
            if (StringUtils.isNotBlank(bigDataResourceId)) {
                // 获取新的库配置信息
                DatabaseConf deploymentConf = databaseConfService.queryOneByBigResourceId(bigDataResourceId);
                if (deploymentConf == null) {
                    log.warn("库配置缓存刷新 bigResourceId: [{}] 不存在!", bigDataResourceId);
                    throw new BusinessException(ExceptionCode.DATABASE_NULL_EXCEPTION, null);
                }
                if (deploymentConf.getIsDel() == null || !deploymentConf.getIsDel().equals(1)) {
                    D newDatabaseConfig = getDbModuleConfig(deploymentConf);
                    // 获取旧的库配置
                    D oldDatabaseConfig = this.dbModuleConfigCache.get(bigDataResourceId);
                    if (oldDatabaseConfig != null) {
                        // 校验是否需要重新创建连接对象
                        checkDatabaseCache(newDatabaseConfig, null, oldDatabaseConfig, null, bigDataResourceId, null);
                    }
                    this.dbModuleConfigCache.put(bigDataResourceId, newDatabaseConfig);
                } else {
                    // 从缓存中将无用连接加入待关闭连接缓存中
                    destroyConnectToCache(deploymentConf.getBigdataResourceId(), null);
                }
            } else {
                // 获取新的库配置信息
                Date updateTime = this.lastLoadTimeForDatabase;
                this.lastLoadTimeForDatabase = queryLastTimeForDatabase();
                List<DatabaseConf> databaseConfList = databaseConfService.queryListByDbType(dbResourceEnum.getVal(), updateTime);
                List<DatabaseConf> deploymentConfList = getDeploymentConfList(updateTime);
                if (CollectionUtil.isNotEmpty(deploymentConfList)) {
                    databaseConfList.addAll(deploymentConfList);
                }
                if (CollectionUtils.isNotEmpty(databaseConfList)) {
                    for (int i = 0; i < databaseConfList.size(); i++) {
                        DatabaseConf deploymentConf = databaseConfList.get(i);
                        if (deploymentConf.getIsDel() == null || !deploymentConf.getIsDel().equals(1)) {
                            // 新库配置
                            D newDatabaseConfig = getDbModuleConfig(deploymentConf);
                            // 获取旧的库配置
                            D oldDatabaseConfig = this.dbModuleConfigCache.get(deploymentConf.getBigdataResourceId());
                            if (oldDatabaseConfig != null) {
                                // 校验是否需要重新创建连接对象
                                checkDatabaseCache(newDatabaseConfig, null, oldDatabaseConfig, null, deploymentConf.getBigdataResourceId(), null);
                            }
                            this.dbModuleConfigCache.put(deploymentConf.getBigdataResourceId(), newDatabaseConfig);
                        } else {
                            // 从缓存中将无用连接加入待关闭连接缓存中
                            destroyConnectToCache(deploymentConf.getBigdataResourceId(), null);
                        }
                    }
                }
            }
        }
    }

    /**
     * 校验链接对象是否改变
     *
     * @param bigDataResourceId
     * @param newDatabaseConfig
     * @param oldDatabaseConfig
     */
    private void checkChangeDatabase(String bigDataResourceId, D newDatabaseConfig, D oldDatabaseConfig) {
        if (oldDatabaseConfig != null) {
            // 校验是否需要重新创建连接对象
            List<TableConf> tableConfList = tableConfService.queryListByBigResourceId(bigDataResourceId);
            for (int i = 0; i < tableConfList.size(); i++) {
                TableConf tableConf = tableConfList.get(i);
                String id = tableConf.getId();
                T oldTableConfig = this.dbTableConfigCache.get(id);
                T newTableConfig = getTableConfig(tableConf);
                checkDatabaseCache(newDatabaseConfig, newTableConfig, oldDatabaseConfig, oldTableConfig, bigDataResourceId, id);
                this.dbTableConfigCache.put(id, newTableConfig);
            }
        }
    }

    /**
     * 从缓存中将无用连接加入待关闭连接缓存中
     *
     * @param bigResourceId
     * @param dbId
     */
    private void destroyConnectToCache(String bigResourceId, String dbId) {
        // 获取旧的库配置
        D oldDatabaseConfig = this.dbModuleConfigCache.get(bigResourceId);
        if (oldDatabaseConfig == null) {
            return;
        }
        DbHelper annotation = this.getClass().getAnnotation(DbHelper.class);
        T tableConfig = null;
        if (annotation.isTableConnect()) {
            tableConfig = this.dbTableConfigCache.get(dbId);
        }
        String oldCacheKey = getCacheKey(oldDatabaseConfig, tableConfig);
        Set<String> set = connectCacheKeyToBigResourceId.get(oldCacheKey);
        // 只有当前连接对象无其他库配置关联，则进行销毁
        if (CollectionUtils.isNotEmpty(set) && set.size() == 1 && set.contains(connectCacheKeyToValue(bigResourceId, dbId))) {
            connectCacheKeyToBigResourceId.remove(oldCacheKey);
            S remove = this.connectCache.getIfPresent(oldCacheKey);
            this.connectCache.invalidate(oldCacheKey);
            try {
                destroyLock.lock();
                Set<S> connectList = destroyConnect.get(oldCacheKey);
                if (connectList != null) {
                    connectList.add(remove);
                } else {
                    destroyConnect.put(oldCacheKey, CollectionUtil.newHashSet(remove));
                }
            } finally {
                destroyLock.unlock();
            }
        } else {
            set.remove(connectCacheKeyToValue(bigResourceId, dbId));
            log.debug("数据库配置信息刷新: bigResourceId: [{}], dbId: [{}] 对应的连接对象尚存在其他配置使用，不进行销毁!", bigResourceId, dbId);
        }
    }

    /**
     * 表配置刷新
     *
     * @param dbId
     */
    @Override
    public void refreshTableCache(String dbId) {
        DbHelper annotation = this.getClass().getAnnotation(DbHelper.class);
        if (StringUtils.isNotBlank(dbId)) {
            TableConf tableConf = tableConfService.queryOneById(dbId);
            if (tableConf == null) {
                log.warn("表配置缓存刷新 dbId: [{}] 不存在!", dbId);
                throw new BusinessException(ExceptionCode.TABLE_NULL_EXCEPTION, null);
            }
            T tableConfig = getTableConfig(tableConf);
            if (tableConf.getIsDel() == null || !tableConf.getIsDel().equals(1)) {
                T oldTableConf = this.dbTableConfigCache.put(tableConf.getId(), tableConfig);
                if (annotation.isTableConnect()) {
                    DatabaseConf databaseConf = databaseConfService.queryOneByBigResourceId(tableConf.getBigdataResourceId());
                    if (databaseConf == null) {
                        log.warn("dbId: [{}] 表配置刷新，未获取到关联的库信息, bigResourceId: [{}]", tableConf.getId(), tableConf.getBigdataResourceId());
                    } else {
                        D newDatabaseConfig = getDbModuleConfig(databaseConf);
                        D oldDatabaseConfig = this.dbModuleConfigCache.get(databaseConf.getBigdataResourceId());
                        checkDatabaseCache(newDatabaseConfig, tableConfig, oldDatabaseConfig, oldTableConf, databaseConf.getBigdataResourceId(), dbId);
                        this.dbModuleConfigCache.put(databaseConf.getBigdataResourceId(), newDatabaseConfig);
                    }
                }
            } else {
                if (annotation.isTableConnect()) {
                    destroyConnectToCache(tableConf.getBigdataResourceId(), tableConf.getId());
                }
                this.dbTableConfigCache.remove(tableConf.getId());
            }
        } else {
            Date updateTime = this.lastLoadTimeForTable;
            this.lastLoadTimeForTable = queryLastTimeForTable();
            List<TableConf> tableConfList = tableConfService.queryListByUpdateTimeAndDbType(dbResourceEnum.getVal(), updateTime);
            if (CollectionUtils.isNotEmpty(tableConfList)) {
                for (int i = 0; i < tableConfList.size(); i++) {
                    TableConf tableConf = tableConfList.get(i);
                    if (tableConf.getIsDel() == null || !tableConf.getIsDel().equals(1)) {
                        T tableConfig = getTableConfig(tableConf);
                        T oldTableConf = this.dbTableConfigCache.put(tableConf.getId(), tableConfig);
                        DatabaseConf databaseConf = databaseConfService.queryOneByBigResourceId(tableConf.getBigdataResourceId());
                        if (databaseConf == null) {
                            log.warn("dbId: [{}] 表配置刷新，未获取到关联的库信息, bigResourceId: [{}]", tableConf.getId(), tableConf.getBigdataResourceId());
                            continue;
                        }
                        D newDatabaseConfig = getDbModuleConfig(databaseConf);
                        D oldDatabaseConfig = this.dbModuleConfigCache.get(databaseConf.getBigdataResourceId());
                        checkDatabaseCache(newDatabaseConfig, tableConfig, oldDatabaseConfig, oldTableConf, databaseConf.getBigdataResourceId(), dbId);
                        this.dbModuleConfigCache.put(databaseConf.getBigdataResourceId(), newDatabaseConfig);
                    } else {
                        if (annotation.isTableConnect()) {
                            destroyConnectToCache(tableConf.getBigdataResourceId(), tableConf.getId());
                        }
                        this.dbTableConfigCache.remove(tableConf.getId());
                    }
                }
            }
        }
    }

    /**
     * 校验是否连接对象已经废弃，需要回收处理
     *
     * @param newDatabaseConfig
     * @param newTableConfig
     * @param oldDatabaseConfig
     * @param oldTableConfig
     * @param bigDataResourceId
     * @param dbId
     */
    private void checkDatabaseCache(D newDatabaseConfig, T newTableConfig, D oldDatabaseConfig, T oldTableConfig, String bigDataResourceId, String dbId) {
        String newCacheKey = getCacheKey(newDatabaseConfig, newTableConfig);
        String oldCacheKey = getCacheKey(oldDatabaseConfig, oldTableConfig);

        // 判断是否改变数据库连接信息
        if (!StringUtils.equals(newCacheKey, oldCacheKey)) {
            Set<String> set = connectCacheKeyToBigResourceId.get(oldCacheKey);
            // 只有当前连接对象无其他库配置关联，则进行销毁
            if (CollectionUtil.isNotEmpty(set) && set.contains(connectCacheKeyToValue(bigDataResourceId, dbId))) {
                if (set.size() == 1) {
                    connectCacheKeyToBigResourceId.remove(oldCacheKey);
                    S remove = this.connectCache.getIfPresent(oldCacheKey);
                    if (remove != null) {
                        this.connectCache.invalidate(oldCacheKey);
                        try {
                            destroyLock.lock();
                            Set<S> connectList = destroyConnect.get(oldCacheKey);
                            if (connectList != null) {
                                connectList.add(remove);
                            } else {
                                destroyConnect.put(oldCacheKey, CollectionUtil.newHashSet(remove));
                            }
                        } finally {
                            destroyLock.unlock();
                        }
                    }
                } else {
                    set.remove(connectCacheKeyToValue(bigDataResourceId, dbId));
                    log.info("数据库配置信息刷新: [{}] 对应的连接对象尚存在其他配置使用，不进行销毁!", bigDataResourceId);
                }
            } else {
                S remove = this.connectCache.getIfPresent(oldCacheKey);
                if (remove != null)  {
                    this.connectCache.invalidate(oldCacheKey);
                    try {
                        destroyLock.lock();
                        Set<S> connectList = destroyConnect.get(oldCacheKey);
                        if (connectList != null) {
                            connectList.add(remove);
                        } else {
                            destroyConnect.put(oldCacheKey, CollectionUtil.newHashSet(remove));
                        }
                    } finally {
                        destroyLock.unlock();
                    }
                }
            }
        }
    }

    /**
     * 设置连接缓存对应依赖
     *
     * @param cacheKey
     * @param bigResourceId
     * @param dbId
     */
    private void setConnectCacheKey(String cacheKey, String bigResourceId, String dbId) {
        Set<String> valueSet = connectCacheKeyToBigResourceId.get(cacheKey);
        if (CollectionUtils.isNotEmpty(valueSet)) {
            valueSet.add(connectCacheKeyToValue(bigResourceId, dbId));
        } else {
            valueSet = new HashSet<>();
            connectCacheKeyToBigResourceId.put(cacheKey, valueSet);
            valueSet.add(connectCacheKeyToValue(bigResourceId, dbId));
        }
    }

    /**
     * 获取连接缓存对应的依赖值
     *
     * @param bigResourceId
     * @param dbId
     */
    private String connectCacheKeyToValue(String bigResourceId, String dbId) {
        if (StringUtils.isNotBlank(dbId)) {
            return bigResourceId + "#" + dbId;
        } else {
            return bigResourceId;
        }
    }

    /**
     * 销毁连接对象任务
     */
    public void destroyConnectTask() {
        DbHelper annotation = this.getClass().getAnnotation(DbHelper.class);
        final DbResourceEnum dbResourceEnum = annotation.dbType();
        destroyConnectExecutor = new ScheduledThreadPoolExecutor(1, new ThreadNamedFactory("monitor-" + dbResourceEnum.name() + "-destroy-connect"));
        destroyConnectExecutor.scheduleAtFixedRate(() -> {
            try {
                Map<String, Set<S>> destroyConnectCache;
                try {
                    destroyConnectCache = new ConcurrentHashMap<>();
                    destroyLock.lock();
                    Set<String> cacheKeys = this.destroyConnect.keySet();
                    if (cacheKeys.size() > 0) {
                        log.info("执行连接销毁任务，待销毁连接个数: [{}], cacheKeys: {}", cacheKeys.size(), cacheKeys);
                    }
                    for (String cacheKey : cacheKeys) {
                        long count = this.useConnectCount.get(cacheKey);
                        if (count == 0) {
                            Set<S> connectList = this.destroyConnect.remove(cacheKey);
                            destroyConnectCache.put(cacheKey, connectList);
                        } else {
                            log.warn("待销毁的连接 cacheKey: [{}] 由于仍有调用持有该连接，暂时无法销毁此连接!", cacheKey);
                        }
                    }
                } finally {
                    destroyLock.unlock();
                }

                try {
                    Set destroySuccess = new HashSet();
                    if (MapUtils.isNotEmpty(destroyConnectCache)) {
                        destroyConnectCache.forEach((key, value) -> {
                            try {
                                Iterator<S> iterator = value.iterator();
                                while (iterator.hasNext()) {
                                    S connect = iterator.next();
                                    destroyDbConnect(key, connect);
                                    iterator.remove();
                                }
                                destroySuccess.add(key);
                            } catch (Exception e) {
                                log.error("销毁无用缓存连接失败! cacheKey: [{}]", key, e);
                            }
                        });
                    }
                    if (destroySuccess.size() > 0) {
                        log.info("此次任务共销毁无用连接对象 [{}] 个!", destroySuccess.size());
                    }
                    if (CollectionUtils.isNotEmpty(destroySuccess)) {
                        destroyConnectCache.keySet().removeAll(destroySuccess);
                    }
                } finally {
                    if (MapUtils.isNotEmpty(destroyConnectCache)) {
                        try {
                            destroyLock.lock();
                            for (Map.Entry<String, Set<S>> entry : destroyConnectCache.entrySet()) {
                                Set<S> connectSet = this.destroyConnect.get(entry.getKey());
                                if (connectSet != null) {
                                    connectSet.addAll(entry.getValue());
                                } else {
                                    this.destroyConnect.put(entry.getKey(), entry.getValue());
                                }
                            }
                        } finally {
                            destroyLock.unlock();
                        }
                    }
                }
            } catch (Exception e) {
                log.error("dbType: [{}] destroy connect fail!", dbResourceEnum.name(), e);
            }
        }, 1, 1, TimeUnit.MINUTES);
    }

    /**
     * 定时校验数据源异常链接，并清除连接缓存
     * 每一分钟执行一次巡检
     */
    private void checkDataSourceStatusTask() {
        DbHelper annotation = this.getClass().getAnnotation(DbHelper.class);
        final DbResourceEnum dbResourceEnum = annotation.dbType();
        checkDataSourceStatusExecutor = new ScheduledThreadPoolExecutor(1, new ThreadNamedFactory("monitor-" + dbResourceEnum.name() + "-connect-status"));
        checkDataSourceTaskThreadPoolExecutor = new ThreadPoolExecutor(1, 1,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(1),
                new ThreadNamedFactory("DataSource-CheckStatus" + dbResourceEnum.name() + "-SingleThreadPool"));
        checkDataSourceStatusExecutor.scheduleWithFixedDelay(() -> {
            this.connectCache.asMap().forEach((k, v) -> {
                Future<Boolean> submit = null;
                boolean b;
                try {
                    submit = checkDataSourceTaskThreadPoolExecutor.submit(() -> checkDataSourceStatus(v));
                    b = submit.get(60, TimeUnit.SECONDS);
                } catch (RejectedExecutionException e) {
                    log.error("dbType: [{}] 定时连接校验任务池已满，执行当前校验失败! 不进行清除并关闭当前连接缓存操作! key: [{}]", dbResourceEnum.name(), k, e);
                    b = true;
                } catch (TimeoutException te) {
                    boolean cancel = true;
                    if (submit != null) {
                        cancel = submit.cancel(true);
                    }
                    log.error("dbType: [{}] 定时连接校验超时，执行取消操作结果: [{}]! key: [{}]", dbResourceEnum.name(), cancel, k, te);
                    b = false;
                }
                catch (Exception e) {
                    log.error("dbType: [{}] 定时连接校验异常! key: [{}]", dbResourceEnum.name(), k, e);
                    b = false;
                }

                //测试成功，清楚统计次数
                if(b) {
                    connectTestFailNumMap.put(k, 0);
                } else {
                    //测试失败，累积次数
                    Integer failNum = connectTestFailNumMap.getOrDefault(k, 0);
                    failNum++;
                    connectTestFailNumMap.put(k, failNum);
                    if(failNum >= 3) {
                        log.warn("dbType: [{}] key: [{}] connect lose efficacy, will remove from cache!", dbResourceEnum.name(), k);
                        try {
                            destroyLock.lock();
                            Set<S> connect = destroyConnect.get(k);
                            if (connect != null) {
                                // 当前待销毁的队列中发现同集群的连接对象未完成关闭，说明短期内频繁发生异常暂时不清除当前对象
                                log.error("当前连接对象 cacheKey: [{}] 短时间内频繁发生连接校验异常现象，暂时不回收当前连接，请核查当前连接是否正常!", k);
                                connectTestFailNumMap.put(k, 2);
                            } else {
                                // 当前待销毁的队列中未发现当前同集群的相同连接对象
                                destroyConnect.put(k, CollectionUtil.newHashSet(v));
                                this.connectCache.invalidate(k);
                                connectTestFailNumMap.remove(k);
                            }
                        } finally {
                            destroyLock.unlock();
                        }
                    }
                }
            });
        }, 1, 1, TimeUnit.MINUTES);
    }

    /**
     * 资源回收
     */
    @Override
    public void finalizeRes() throws Exception {
        // 关闭调度资源
        if (this.checkDataSourceStatusExecutor != null) {
            this.checkDataSourceStatusExecutor.shutdownNow();
        }
        if (this.checkDataSourceTaskThreadPoolExecutor != null) {
            this.checkDataSourceTaskThreadPoolExecutor.shutdownNow();
        }
        if (this.destroyConnectExecutor != null) {
            this.destroyConnectExecutor.shutdownNow();
        }
        if (this.connectCache != null) {
            ConcurrentMap<String, S> concurrentMap = this.connectCache.asMap();
            concurrentMap.forEach((key, value) -> {
                try {
                    destroyDbConnect(key, value);
                } catch (Exception e) {
                    log.error("dbType: [{}] cacheKey: [{}] finalize resource fail!", this.dbResourceEnum.name(), key, e);
                }
            });
        }
    }

    @Override
    public void destroyDbConnect(DatabaseSetting databaseSetting, TableSetting tableSetting) {
        String bigDataResourceId = databaseSetting.getBigDataResourceId();
        if (StringUtils.isNotBlank(bigDataResourceId)) {
            destroyConnectToCache(bigDataResourceId, tableSetting != null ? tableSetting.getDbId() : null);
        } else {
            D dbModuleConfig = null;
            T dbTableConfig = null;
            String connSetting = databaseSetting.getConnSetting();
            DatabaseConf databaseConf = new DatabaseConf();
            databaseConf.setConnSetting(connSetting);
            databaseConf.setResourceName(databaseSetting.getDbType());
            databaseConf.setConnTypeId(databaseSetting.getConnTypeId());
            databaseConf.setSecuritySoftwareOrgcode(databaseSetting.getCloudCode());
            dbModuleConfig = getDbModuleConfig(databaseConf);
            if (tableSetting != null && tableSetting.getTableName() != null) {
                TableConf tableConf = new TableConf();
                tableConf.setTableName(tableSetting.getTableName());
                tableConf.setTableJson(tableSetting.getTableJson());
                dbTableConfig = getTableConfig(tableConf);
            }
            if (dbModuleConfig == null) {
                throw new BusinessException(ExceptionCode.DATABASE_NULL_EXCEPTION, null);
            }
            String cacheKey = getCacheKey(dbModuleConfig, dbTableConfig);
            S remove = this.connectCache.getIfPresent(cacheKey);
            if (remove != null) {
                this.connectCache.invalidate(cacheKey);
                try {
                    destroyLock.lock();
                    Set<S> connectList = destroyConnect.get(cacheKey);
                    if (connectList != null) {
                        connectList.add(remove);
                    } else {
                        destroyConnect.put(cacheKey, CollectionUtil.newHashSet(remove));
                    }
                } finally {
                    destroyLock.unlock();
                }
            }
        }
    }

    /**
     * 校验数据源状态
     * 即实现判断当前组件数据源对象是否可用
     *
     * 若为 http 或其他非长连接方式可用直接返回 true 即可；
     *
     * @param connect
     * @return
     * @throws Exception
     */
    public abstract boolean checkDataSourceStatus(S connect) throws Exception;

    /**
     * 初始化加载数据库配置
     * 即根据资源目录上面定义的部署定义配置规则，解析为 数据库配置信息实体
     *
     * @param conf
     * @return
     */
    public abstract D initDbModuleConfig(DatabaseConf conf);

    /**
     * 初始化加载表信息配置
     *
     * 即根据资源目录上面定义的部署定义配置规则，解析为 数据库表配置信息实体
     *
     * @param conf
     * @return
     */
    public abstract T initTableConfig(TableConf conf);

    /**
     * 初始化数据库链接
     * 即实现 根据 配置信息实体 创建对应组件的数据源对象
     *
     * @param databaseConf
     * @param tableConf 有的数据库连接以表为单位（solr）
     * @return
     */
    public abstract S initDbConnect(D databaseConf, T tableConf);

    /**
     * 注销连接对象
     *
     * 关闭数据源的方法
     *
     * @param cacheKey
     * @param connect
     * @throws Exception
     */
    public abstract void destroyDbConnect(String cacheKey,  S connect) throws Exception;

    public D getDbModuleConfig(DatabaseConf conf) {
        String connSetting = conf.getConnSetting();
        return dbConfigCache.get(connSetting.hashCode(), (hashCode)->{return initDbModuleConfig(conf);});
    }

    public T getTableConfig(TableConf conf) {
        return tableConfigCache.get(conf.hashCode(), (hashCode)->{return initTableConfig(conf);});
    }

    /**
     * 从缓存中获取匹配到的同样查询条件下的游标缓存
     *
     * @author xult
     * @date 2020/8/28
     * @param cacheKey
     * @return C
     */
    public C findCursorCache(String cacheKey) {
        C ifPresent = dbCursorCache.getIfPresent(cacheKey);
        if (ifPresent != null) {
            dbCursorCache.invalidate(cacheKey);
        }
        return ifPresent;
    };

    /**
     * 创建或更新游标缓存
     *
     * @param cacheKey
     * @param cache
     * @return void
     * @author xult
     * @date 2020/8/28
     */
    public void createOrUpdateCursorCache(String cacheKey, C cache) {
        dbCursorCache.put(cacheKey, cache);
    };

    /**
     * 获取游标ID 根据时间戳拼装的随机值
     *
     * @return
     */
    public String createCursorId(String dbId) {
        long current = DateUtil.current();
        String simpleUUID = IdUtil.simpleUUID();
        return DigestUtil.md5Hex(new StringBuffer().append(current).append("_").append(dbId).append("_").append(simpleUUID).toString());
    }

    /**
     * 游标操作缓存KEY
     * @return
     */
    public String cacheKeyForCursor(String cursorId, D databaseConf, T tableInfo) {
        String cacheKey = getCacheKey(databaseConf, tableInfo);
        StringBuilder sb = new StringBuilder();
        StringBuilder key = sb.append(cursorId).append("-").append(cacheKey).append(tableInfo.getTableName());
        return key.toString();
    }

    @Override
    public void createTransactionManager(String transactionId, TransactionManager transactionManager) {
        // 1.将事务管理对象放入缓存池 transactionManagerCache.put
        transactionManagerCache.put(transactionId,transactionManager);
    }

    @Override
    public TransactionManager getTransactionManager(String transactionId) {
        // 1.从缓存池中获取事务对象 transactionManagerCache.get
        try {
            return transactionManagerCache.getIfPresent(transactionId);
        }catch (NullPointerException e) {
            return null;
        }
    }

    @Override
    public TransactionManager removeTransactionManager(String transactionId) {
        // 1.从缓存池中移除获取事务对象 transactionManagerCache.invalidate
        TransactionManager transactionManager = transactionManagerCache.getIfPresent(transactionId);
        transactionManagerCache.invalidate(transactionId);
        // 2.返回事务对象
        return transactionManager;
    }
}
