package com.meiya.whalex.db.util.helper;

import com.meiya.whalex.db.entity.*;

/**
 * 组件配置信息缓存工具类接口
 * <p>
 * S 泛型：数据库连接对象
 * D 泛型：数据库配置信息对象
 * T 泛型：表信息对象
 *
 * @author 黄河森
 * @date 2021/5/11
 * @project whalex-data-driver
 */
public interface DbModuleConfigHelper<S, D extends AbstractDatabaseInfo, T extends AbstractDbTableInfo, C extends AbstractCursorCache> {

    /**
     * 初始化相关配置
     *
     * @param loadDbConf
     */
    void init(boolean loadDbConf);

    /**
     * 通过大数据组件编码获取组件配置信息
     *
     * @param bigDataResourceId
     * @return
     */
    D getDbModuleConfig(String bigDataResourceId);

    /**
     * 通过表信息编码获取表配置信息
     *
     * @param tableId
     * @return
     */
    T getDbTableConfigCache(String tableId);

    /**
     * 提供给外部直接获取连接对象
     *
     * @param dbHandleEntity
     * @return
     * @throws Exception
     */
    @Deprecated
    S getDbConnect(DbHandleEntity dbHandleEntity) throws Exception;

    /**
     * 提供给外部直接获取连接对象
     *
     * @param databaseSetting
     * @param tableSetting
     * @return
     * @throws Exception
     */
    S getDbConnect(DatabaseSetting databaseSetting, TableSetting tableSetting) throws Exception;

    /**
     * 获取连接对象
     *
     * @param databaseConf
     * @param tableInfo
     * @param bigResourceId
     * @param dbId
     * @return
     * @throws Exception
     */
    S getDbConnect(D databaseConf, T tableInfo, String bigResourceId, String dbId) throws Exception;

    /**
     * 通过缓存Key换错连接，不存在则返回null
     *
     * @param cacheKey
     * @return
     */
    S getDbConnect(String cacheKey);

    /**
     * 数据库链接缓存key策略
     * 如果 数据库连接 与 表信息无关，即创建数据库连接对象，与具体哪个表的信息无关，则不需要关注 tableInfo
     *
     * @param dbDatabaseConf
     * @param tableInfo
     * @return
     */
    String getCacheKey(D dbDatabaseConf, T tableInfo);

    /**
     * 刷新数据库缓存
     *
     * @param bigDataResourceId
     */
    void refreshDatabaseCache(String bigDataResourceId);

    /**
     * 刷新表配置
     *
     * @param dbId
     */
    void refreshTableCache(String dbId);

    /**
     * 资源回收
     *
     * @throws Exception
     */
    void finalizeRes() throws Exception;

    /**
     * 销毁指定配置数据源
     *
     * @param databaseSetting
     * @param tableSetting
     */
    void destroyDbConnect(DatabaseSetting databaseSetting, TableSetting tableSetting);

    /**
     * 将事务管理对象纳入缓存池管理，若存在则更新过期时间
     *
     * @param transactionId
     * @param transactionManager
     */
    void createTransactionManager(String transactionId, TransactionManager transactionManager);

    /**
     * 获取事务管理对象
     *
     * @param transactionId
     * @return
     */
    TransactionManager getTransactionManager(String transactionId);

    /**
     * 从缓存池中剔除事务管理对象，并返回
     *
     * @param transactionId
     * @return
     */
    TransactionManager removeTransactionManager(String transactionId);
}
