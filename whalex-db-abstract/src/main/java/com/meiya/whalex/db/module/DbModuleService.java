package com.meiya.whalex.db.module;

import com.meiya.whalex.db.constant.IsolationLevel;
import com.meiya.whalex.db.constant.SupportPower;
import com.meiya.whalex.db.entity.*;
import com.meiya.whalex.interior.db.operation.in.AlterTableParamCondition;
import com.meiya.whalex.interior.db.operation.in.CreateDatabaseParamCondition;
import com.meiya.whalex.interior.db.operation.in.CreateTableParamCondition;
import com.meiya.whalex.interior.db.operation.in.DropDatabaseParamCondition;
import com.meiya.whalex.interior.db.operation.in.MergeDataParamCondition;
import com.meiya.whalex.interior.db.operation.in.PublishMessage;
import com.meiya.whalex.interior.db.operation.in.QueryDatabasesCondition;
import com.meiya.whalex.interior.db.operation.in.QueryTablesCondition;
import com.meiya.whalex.interior.db.operation.in.SubscribeMessage;
import com.meiya.whalex.interior.db.operation.in.UpdateDatabaseParamCondition;
import com.meiya.whalex.interior.db.search.in.QueryParamCondition;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * 底层组件服务调用接口
 *
 * @author 黄河森
 * @date 2019/9/13
 * @project whale-cloud-platformX
 */
public interface DbModuleService {

    /**
     * 开启事务
     */
    @Deprecated
    void openTransaction();

    /**
     * 开启事务
     *
     * @param isolationLevel
     */
    @Deprecated
    void openTransaction(IsolationLevel isolationLevel);

    /**
     * 开启事务
     *
     * @param transactionId
     */
    @Deprecated
    void openTransaction(String transactionId);

    /**
     * 开启事务
     *
     * @param transactionId
     * @param isolationLevel
     */
    @Deprecated
    void openTransaction(String transactionId, IsolationLevel isolationLevel);

    /**
     * 开启事务
     *
     * @param databaseName
     */
    @Deprecated
    void openTransactionByDatabase(DatabaseSetting databaseName);

    /**
     * 开启事务
     *
     * @param databaseName
     * @param isolationLevel
     */
    @Deprecated
    void openTransactionByDatabase(DatabaseSetting databaseName, IsolationLevel isolationLevel);

    /**
     * 开启事务
     * @param databaseSetting
     * @return
     */
    DbTransactionModuleService newTransaction(DatabaseSetting databaseSetting);

    /**
     * 开启事务
     * @param databaseSetting
     * @param isolationLevel 事务级别
     * @return
     */
    DbTransactionModuleService newTransaction(DatabaseSetting databaseSetting, IsolationLevel isolationLevel);

    /**
     * 获取已存在的事务
     * @param transactionId
     */
    void fetchTransaction(String transactionId);

    /**
     * 事务回滚
     *
     * @throws Exception
     */
    void rollback() throws Exception;

    /**
     * 事务回滚
     *
     * @throws Exception
     */
    void rollback(DatabaseSetting databaseSetting) throws Exception;

    /**
     * 事务提交
     *
     * @throws Exception
     */
    void commit() throws Exception;

    /**
     * 事务提交
     *
     * @param databaseSetting
     * @throws Exception
     */
    void commit(DatabaseSetting databaseSetting) throws Exception;

    /**
     * 支持的能力
     * @return
     */
    List<SupportPower> getSupportList();

    /**
     * 支持的能力
     *
     * @return
     */
    PageResult supportList();
    /**
     * 单条记录查询
     *
     * @param databaseSetting
     * @param tableSetting
     * @param queryParamCondition
     * @return
     * @throws Exception
     */
    PageResult queryOne(DatabaseSetting databaseSetting, TableSetting tableSetting, QueryParamCondition queryParamCondition) throws Exception;

    /**
     * 集合查询
     *
     * @param databaseSetting
     * @param tableSetting
     * @param queryParamCondition
     * @return
     * @throws Exception
     */
    PageResult queryList(DatabaseSetting databaseSetting, TableSetting tableSetting, QueryParamCondition queryParamCondition) throws Exception;

    /**
     * 分页查询
     *
     * @param databaseSetting
     * @param tableSetting
     * @param queryParamCondition
     * @return
     * @throws Exception
     */
    PageResult queryListForPage(DatabaseSetting databaseSetting, TableSetting tableSetting, QueryParamCondition queryParamCondition) throws Exception;

    /**
     * 数据统计
     *
     * @param databaseSetting
     * @param tableSetting
     * @param queryParamCondition
     * @return
     * @throws Exception
     */
    PageResult statisticalLine(DatabaseSetting databaseSetting, TableSetting tableSetting, QueryParamCondition queryParamCondition) throws Exception;

    /**
     * 插入方法
     *
     * @param databaseSetting
     * @param tableSetting
     * @param addParamCondition
     * @return
     * @throws Exception
     */
    PageResult insert(DatabaseSetting databaseSetting, TableSetting tableSetting, AddParamCondition addParamCondition) throws Exception;

    /**
     * 批量插入
     *
     * @param databaseSetting
     * @param tableSetting
     * @param addParamCondition
     * @return
     * @throws Exception
     */
    PageResult insertBatch(DatabaseSetting databaseSetting, TableSetting tableSetting, AddParamCondition addParamCondition) throws Exception;

    /**
     * 更新
     *
     * @param databaseSetting
     * @param tableSetting
     * @param updateParamCondition
     * @return
     * @throws Exception
     */
    PageResult update(DatabaseSetting databaseSetting, TableSetting tableSetting, UpdateParamCondition updateParamCondition) throws Exception;

    /**
     * 删除方法
     *
     * @param databaseSetting
     * @param tableSetting
     * @param delParamCondition
     * @return
     * @throws Exception
     */
    PageResult delete(DatabaseSetting databaseSetting, TableSetting tableSetting, DelParamCondition delParamCondition) throws Exception;

    /**
     * 获取索引
     *
     * @param databaseSetting
     * @param tableSetting
     * @return
     * @throws Exception
     */
    PageResult getIndexes(DatabaseSetting databaseSetting, TableSetting tableSetting) throws Exception;

    /**
     * 创建索引
     *
     * @param databaseSetting
     * @param tableSetting
     * @param indexParamCondition
     * @return
     * @throws Exception
     */
    PageResult createIndex(DatabaseSetting databaseSetting, TableSetting tableSetting, IndexParamCondition indexParamCondition) throws Exception;

    /**
     * 数据库连接状态测试
     *
     * @param databaseSetting
     * @return
     * @throws Exception
     */
    PageResult testConnection(DatabaseSetting databaseSetting) throws Exception;

    /**
     * 是否开始binlog功能
     * @param databaseSetting
     * @return
     * @throws Exception
     */
    PageResult changelog(DatabaseSetting databaseSetting) throws Exception;

    /**
     * 查询库中所有的表
     *
     * @param databaseSetting
     * @param queryTablesCondition
     * @return
     * @throws Exception
     */
    PageResult queryListTable(DatabaseSetting databaseSetting, QueryTablesCondition queryTablesCondition) throws Exception;

    /**
     * 查询库中所有的表
     *
     * @param databaseSetting
     * @return
     * @throws Exception
     */
    @Deprecated
    PageResult queryListTable(DatabaseSetting databaseSetting) throws Exception;

    /**
     * 创建表
     *
     * @param databaseSetting
     * @param tableSetting
     * @param createTableParamCondition
     * @return
     * @throws Exception
     */
    PageResult createTable(DatabaseSetting databaseSetting, TableSetting tableSetting, CreateTableParamCondition createTableParamCondition) throws Exception;

    /**
     * 删除表
     *
     * @param databaseSetting
     * @param tableSetting
     * @param dropTableParamCondition
     * @return
     * @throws Exception
     */
    PageResult dropTable(DatabaseSetting databaseSetting, TableSetting tableSetting, DropTableParamCondition dropTableParamCondition) throws Exception;

    /**
     * 清空表
     * @param databaseSetting
     * @param tableSetting
     * @param emptyTableParamCondition
     * @return
     * @throws Exception
     */
    PageResult emptyTable(DatabaseSetting databaseSetting, TableSetting tableSetting, EmptyTableParamCondition emptyTableParamCondition) throws Exception;

    /**
     * 删除索引
     *
     * @param databaseSetting
     * @param tableSetting
     * @param indexParamCondition
     * @return
     * @throws Exception
     */
    PageResult deleteIndex(DatabaseSetting databaseSetting, TableSetting tableSetting, IndexParamCondition indexParamCondition) throws Exception;

    /**
     * 组件状态监控
     *
     * @param databaseSetting
     * @return
     * @throws Exception
     */
    PageResult monitorStatus(DatabaseSetting databaseSetting) throws Exception;

    /**
     * 查询表结构
     *
     * @param databaseSetting
     * @param tableSetting
     * @return
     * @throws Exception
     */
    PageResult queryTableSchema(DatabaseSetting databaseSetting, TableSetting tableSetting) throws Exception;

    /**
     * 通过游标查询
     *
     * @param databaseSetting
     * @param tableSetting
     * @param queryParamCondition
     * @param consumer
     * @return
     * @throws Exception
     */
    PageResult queryCursor(DatabaseSetting databaseSetting, TableSetting tableSetting, QueryParamCondition queryParamCondition, Consumer<Map<String, Object>> consumer) throws Exception;

    /**
     * 新增 OR 更新
     * 可以指定字段，若指定字段值存在则更新，不存在则新增
     *
     * @param databaseSetting
     * @param tableSetting
     * @param upsertParamCondition
     * @return
     * @throws Exception
     */
    PageResult saveOrUpdate(DatabaseSetting databaseSetting, TableSetting tableSetting, UpsertParamCondition upsertParamCondition) throws Exception;

    /**
     * 批量新增 OR 更新
     * 可以指定字段，若指定字段值存在则更新，不存在则新增
     *
     *
     * @param databaseSetting
     * @param tableSetting
     * @param upsertParamBatchCondition
     * @return
     * @throws Exception
     */
    PageResult saveOrUpdateBatch(DatabaseSetting databaseSetting, TableSetting tableSetting, UpsertParamBatchCondition upsertParamBatchCondition) throws Exception;


    /**
     * 查询库schema列表
     * <p>
     * p.s 只适合用于存在schema的组件(pg/libra/...)
     *
     * @param databaseSetting
     * @return
     * @throws Exception
     */
    PageResult queryDatabaseSchema(DatabaseSetting databaseSetting) throws Exception;

    /**
     * 修改表
     *
     * @param databaseSetting
     * @param tableSetting
     * @param alterTableParamCondition
     * @return
     * @throws Exception
     */
    PageResult alterTable(DatabaseSetting databaseSetting, TableSetting tableSetting, AlterTableParamCondition alterTableParamCondition) throws Exception;

    /**
     * 判断指定表名是否存在
     *
     * @param databaseSetting
     * @param tableSetting
     * @return
     * @throws Exception
     */
    PageResult tableExists(DatabaseSetting databaseSetting, TableSetting tableSetting) throws Exception;

    /**
     * 查询表配置信息，如分区、分布、分片等等
     *
     * @param databaseSetting
     * @param tableSetting
     * @return
     * @throws Exception
     */
    PageResult queryTableInformation(DatabaseSetting databaseSetting, TableSetting tableSetting) throws Exception;

    /**
     * 查询数据库列表
     *
     * @return
     * @throws Exception
     */
    PageResult queryListDatabase(DatabaseSetting databaseSetting, QueryDatabasesCondition queryDatabasesCondition) throws Exception;

    /**
     * 查询数据库详情
     *
     * @return
     * @throws Exception
     */
    PageResult queryDatabaseInformation(DatabaseSetting databaseSetting) throws Exception;

    /**
     * 查询数据库列表
     *
     * @return
     * @throws Exception
     */
    PageResult queryListDatabase(DatabaseSetting databaseSetting) throws Exception;

    /**
     * 合并数据
     *
     * @return
     * @throws Exception
     */
    PageResult mergeData(DatabaseSetting databaseSetting, TableSetting tableSetting, MergeDataParamCondition mergeDataParamCondition) throws Exception;

    /**
     * 创建数据库
     *
     * @return
     * @throws Exception
     */
    PageResult createDatabase(DatabaseSetting databaseSetting, CreateDatabaseParamCondition createDatabaseParamCondition) throws Exception;

    /**
     * 修改数据库
     *
     * @return
     * @throws Exception
     */
    PageResult updateDatabase(DatabaseSetting databaseSetting, UpdateDatabaseParamCondition updateDatabaseParamCondition) throws Exception;

    /**
     * 删除数据库
     *
     * @return
     * @throws Exception
     */
    PageResult dropDatabase(DatabaseSetting databaseSetting, DropDatabaseParamCondition dropDatabaseParamCondition) throws Exception;

    /**
     * 查询分区信息
     *
     * @return
     * @throws Exception
     */
    PageResult queryPartitionInformation(DatabaseSetting databaseSetting, TableSetting tableSetting) throws Exception;


    /**
     * 查询分区信息
     *
     * @return
     * @throws Exception
     */
    PageResult queryPartitionInformationV2(DatabaseSetting databaseSetting, TableSetting tableSetting) throws Exception;

    void publish(DatabaseSetting databaseSetting, TableSetting tableSetting, PublishMessage message) throws Exception;

    void subscribe(DatabaseSetting databaseSetting, TableSetting tableSetting, SubscribeMessage subscribeMessage) throws Exception;
    // ----------------------------------------------------------------------------------------------
    // 废弃 API
    // ----------------------------------------------------------------------------------------------

    /**
     * 单条记录查询
     *
     * @param dbHandleEntity
     * @param queryParamCondition
     * @return
     * @throws Exception
     */
    @Deprecated
    PageResult queryOne(DbHandleEntity dbHandleEntity, QueryParamCondition queryParamCondition) throws Exception;

    /**
     * 集合查询
     *
     * @param dbHandleEntity
     * @param queryParamCondition
     * @return
     * @throws Exception
     */
    @Deprecated
    PageResult queryList(DbHandleEntity dbHandleEntity, QueryParamCondition queryParamCondition) throws Exception;

    /**
     * 分页查询
     *
     * @param dbHandleEntity
     * @param queryParamCondition
     * @return
     * @throws Exception
     */
    @Deprecated
    PageResult queryListForPage(DbHandleEntity dbHandleEntity, QueryParamCondition queryParamCondition) throws Exception;

    /**
     * 数据统计
     *
     * @param dbHandleEntity
     * @param queryParamCondition
     * @return
     * @throws Exception
     */
    @Deprecated
    PageResult statisticalLine(DbHandleEntity dbHandleEntity, QueryParamCondition queryParamCondition) throws Exception;

    /**
     * 插入方法
     *
     * @param dbHandleEntity
     * @param addParamCondition
     * @return
     * @throws Exception
     */
    @Deprecated
    PageResult insert(DbHandleEntity dbHandleEntity, AddParamCondition addParamCondition) throws Exception;

    /**
     * 批量插入
     *
     * @param dbHandleEntity
     * @param addParamCondition
     * @return
     * @throws Exception
     */
    @Deprecated
    PageResult insertBatch(DbHandleEntity dbHandleEntity, AddParamCondition addParamCondition) throws Exception;

    /**
     * 更新
     *
     * @param dbHandleEntity
     * @param updateParamCondition
     * @return
     * @throws Exception
     */
    @Deprecated
    PageResult update(DbHandleEntity dbHandleEntity, UpdateParamCondition updateParamCondition) throws Exception;

    /**
     * 删除方法
     *
     * @param dbHandleEntity
     * @param delParamCondition
     * @return
     * @throws Exception
     */
    @Deprecated
    PageResult delete(DbHandleEntity dbHandleEntity, DelParamCondition delParamCondition) throws Exception;

    /**
     * 获取索引
     *
     * @param dbHandleEntity
     * @return
     * @throws Exception
     */
    @Deprecated
    PageResult getIndexes(DbHandleEntity dbHandleEntity) throws Exception;

    /**
     * 创建索引
     *
     * @param dbHandleEntity
     * @param indexParamCondition
     * @return
     * @throws Exception
     */
    @Deprecated
    PageResult createIndex(DbHandleEntity dbHandleEntity, IndexParamCondition indexParamCondition) throws Exception;

    /**
     * 数据库连接状态测试
     *
     * @param dbHandleEntity
     * @return
     * @throws Exception
     */
    @Deprecated
    PageResult testConnection(DbHandleEntity dbHandleEntity) throws Exception;

    /**
     * 是否开始binlog功能
     * @param dbHandleEntity
     * @return
     * @throws Exception
     */
    @Deprecated
    PageResult changelog(DbHandleEntity dbHandleEntity) throws Exception;

    /**
     * 查询库中所有的表
     *
     * @param dbHandleEntity
     * @param queryTablesCondition
     * @return
     * @throws Exception
     */
    @Deprecated
    PageResult queryListTable(DbHandleEntity dbHandleEntity, QueryTablesCondition queryTablesCondition) throws Exception;

    /**
     * 查询数据库详情
     *
     * @return
     * @throws Exception
     */
    @Deprecated
    PageResult queryDatabaseInformation(DbHandleEntity dbHandleEntity) throws Exception;

    /**
     * 查询数据库中所有的库
     *
     * @param dbHandleEntity
     * @param queryDatabasesCondition
     * @return
     * @throws Exception
     */
    @Deprecated
    PageResult queryListDatabase(DbHandleEntity dbHandleEntity, QueryDatabasesCondition queryDatabasesCondition) throws Exception;
    /**
     * 查询库中所有的表
     *
     * @param dbHandleEntity
     * @return
     * @throws Exception
     */
    @Deprecated
    PageResult queryListTable(DbHandleEntity dbHandleEntity) throws Exception;

    /**
     * 创建表
     *
     * @param dbHandleEntity
     * @param createTableParamCondition
     * @return
     * @throws Exception
     */
    @Deprecated
    PageResult createTable(DbHandleEntity dbHandleEntity, CreateTableParamCondition createTableParamCondition) throws Exception;

    /**
     * 删除表
     *
     * @param dbHandleEntity
     * @param dropTableParamCondition
     * @return
     * @throws Exception
     */
    @Deprecated
    PageResult dropTable(DbHandleEntity dbHandleEntity, DropTableParamCondition dropTableParamCondition) throws Exception;

    /**
     * 删除表
     *
     * @param dbHandleEntity
     * @param emptyTableParamCondition
     * @return
     * @throws Exception
     */
    @Deprecated
    PageResult emptyTable(DbHandleEntity dbHandleEntity, EmptyTableParamCondition emptyTableParamCondition) throws Exception;

    /**
     * 删除索引
     *
     * @param dbHandleEntity
     * @param indexParamCondition
     * @return
     * @throws Exception
     */
    @Deprecated
    PageResult deleteIndex(DbHandleEntity dbHandleEntity, IndexParamCondition indexParamCondition) throws Exception;

    /**
     * 组件状态监控
     *
     * @param dbHandleEntity
     * @return
     * @throws Exception
     */
    @Deprecated
    PageResult monitorStatus(DbHandleEntity dbHandleEntity) throws Exception;

    /**
     * 查询表结构
     *
     * @param dbHandleEntity
     * @return
     * @throws Exception
     */
    @Deprecated
    PageResult queryTableSchema(DbHandleEntity dbHandleEntity) throws Exception;

    /**
     * 通过游标查询
     *
     * @param dbHandleEntity
     * @param queryParamCondition
     * @param consumer
     * @return
     * @throws Exception
     */
    @Deprecated
    PageResult queryCursor(DbHandleEntity dbHandleEntity, QueryParamCondition queryParamCondition, Consumer<Map<String, Object>> consumer) throws Exception;

    /**
     * 新增 OR 更新
     * 可以指定字段，若指定字段值存在则更新，不存在则新增
     *
     * @param dbHandleEntity
     * @param upsertParamCondition
     * @return
     * @throws Exception
     */
    @Deprecated
    PageResult saveOrUpdate(DbHandleEntity dbHandleEntity, UpsertParamCondition upsertParamCondition) throws Exception;


    /**
     * 查询库schema列表
     * <p>
     * p.s 只适合用于存在schema的组件(pg/libra/...)
     *
     * @param dbHandleEntity
     * @return
     * @throws Exception
     */
    @Deprecated
    PageResult queryDatabaseSchema(DbHandleEntity dbHandleEntity) throws Exception;

    /**
     * 修改表
     *
     * @param dbHandleEntity
     * @param alterTableParamCondition
     * @return
     * @throws Exception
     */
    @Deprecated
    PageResult alterTable(DbHandleEntity dbHandleEntity, AlterTableParamCondition alterTableParamCondition) throws Exception;

    /**
     * 判断指定表名是否存在
     *
     * @param dbHandleEntity
     * @return
     * @throws Exception
     */
    @Deprecated
    PageResult tableExists(DbHandleEntity dbHandleEntity) throws Exception;

    /**
     * 查询表配置信息，如分区、分布、分片等等
     *
     * @param dbHandleEntity
     * @return
     * @throws Exception
     */
    @Deprecated
    PageResult queryTableInformation(DbHandleEntity dbHandleEntity) throws Exception;

    /**
     * 合并数据
     *
     * @return
     * @throws Exception
     */
    @Deprecated
    PageResult mergeData(DbHandleEntity dbHandleEntity, MergeDataParamCondition mergeDataParamCondition) throws Exception;

    /**
     * 创建数据库
     *
     * @return
     * @throws Exception
     */
    @Deprecated
    PageResult createDatabase(DbHandleEntity dbHandleEntity, CreateDatabaseParamCondition createDatabaseParamCondition) throws Exception;

    /**
     * 修改数据库
     *
     * @return
     * @throws Exception
     */
    @Deprecated
    PageResult updateDatabase(DbHandleEntity dbHandleEntity, UpdateDatabaseParamCondition updateDatabaseParamCondition) throws Exception;

    /**
     * 删除数据库
     *
     * @return
     * @throws Exception
     */
    @Deprecated
    PageResult dropDatabase(DbHandleEntity dbHandleEntity, DropDatabaseParamCondition dropDatabaseParamCondition) throws Exception;

    /**
     * 查询分区信息
     *
     * @return
     * @throws Exception
     */
    @Deprecated
    PageResult queryPartitionInformation(DbHandleEntity dbHandleEntity) throws Exception;

    /**
     * 查询分区信息
     *
     * @return
     * @throws Exception
     */
    @Deprecated
    PageResult queryPartitionInformationV2(DbHandleEntity dbHandleEntity) throws Exception;
}
