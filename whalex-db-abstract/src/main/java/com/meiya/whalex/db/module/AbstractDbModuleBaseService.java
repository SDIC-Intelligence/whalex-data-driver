package com.meiya.whalex.db.module;

import com.meiya.whalex.annotation.DbService;
import com.meiya.whalex.annotation.Support;
import com.meiya.whalex.business.entity.DatabaseConf;
import com.meiya.whalex.business.entity.TableConf;
import com.meiya.whalex.db.constant.*;
import com.meiya.whalex.db.entity.*;
import com.meiya.whalex.db.util.helper.AbstractDbModuleConfigHelper;
import com.meiya.whalex.db.util.param.AbstractDbModuleParamUtil;
import com.meiya.whalex.exception.BusinessException;
import com.meiya.whalex.exception.ExceptionCode;
import com.meiya.whalex.interior.db.constant.DbResourceEnum;
import com.meiya.whalex.interior.db.constant.ReturnCodeEnum;
import com.meiya.whalex.db.constant.SupportPower;
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
import com.meiya.whalex.thread.entity.DbThreadBaseConfig;
import com.meiya.whalex.util.UUIDGenerator;
import com.meiya.whalex.util.concurrent.ThreadLocalUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.skywalking.apm.toolkit.trace.Trace;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static com.meiya.whalex.exception.ExceptionCode.TRANSACTION_ID_NULL_EXCEPTION;

/**
 * 数据库组件基础服务定义
 * <p>
 * S 泛型: 数据库连接对象
 * Q 泛型: 组件参数实体
 * D 泛型: 数据库配置信息实体
 * T 泛型: 数据库表配置实体
 *
 * @author 黄河森
 * @date 2019/9/13
 * @project whale-cloud-platformX
 */
@Slf4j
public abstract class AbstractDbModuleBaseService<S,
        Q extends AbstractDbHandler,
        D extends AbstractDatabaseInfo,
        T extends AbstractDbTableInfo,
        C extends AbstractCursorCache> implements DbModuleService {

    /**
     * 参数转换工具类
     */
    private AbstractDbModuleParamUtil dbModuleParamUtil;

    /**
     * 配置缓存工具类
     */
    protected AbstractDbModuleConfigHelper<S, D, T, C> helper;

    /**
     * 线程配置
     */
    protected DbThreadBaseConfig dbThreadBaseConfig;

    public AbstractDbModuleParamUtil getDbModuleParamUtil() {
        return dbModuleParamUtil;
    }

    public AbstractDbModuleConfigHelper getHelper() {
        return helper;
    }

    public void setHelper(AbstractDbModuleConfigHelper helper) {
        this.helper = helper;
    }

    public void setDbModuleParamUtil(AbstractDbModuleParamUtil dbModuleParamUtil) {
        this.dbModuleParamUtil = dbModuleParamUtil;
    }

    public void setDbThreadBaseConfig(DbThreadBaseConfig dbThreadBaseConfig) {
        this.dbThreadBaseConfig = dbThreadBaseConfig;
    }

    /**
     * 查询入口方法
     *
     * @param connect
     * @param queryEntity
     * @param databaseConf
     * @param tableConf
     * @return
     * @throws Exception
     */
    protected abstract QueryMethodResult queryMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception;

    /**
     * 数据统计
     *
     * @param connect
     * @param queryEntity
     * @param databaseConf
     * @param tableConf
     * @return
     * @throws Exception
     */
    protected abstract QueryMethodResult countMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception;

    /**
     * 连接测试
     *
     * @param connect
     * @param databaseConf
     * @return
     * @throws Exception
     */
    protected abstract QueryMethodResult testConnectMethod(S connect, D databaseConf) throws Exception;

    /**
     * 是否开始binlog功能
     *
     * @param connect
     * @param databaseConf
     * @return
     * @throws Exception
     */
    protected QueryMethodResult changelog(S connect, D databaseConf) throws Exception{
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "AbstractDbModuleBaseService.isOpenBinlog");
    }


    /**
     * 查询数据库中所有表信息
     *
     * @param connect
     * @param queryEntity
     * @param databaseConf
     * @return
     * @throws Exception
     */
    protected abstract QueryMethodResult showTablesMethod(S connect, Q queryEntity, D databaseConf) throws Exception;

    /**
     * 查询表索引信息
     *
     * @param connect
     * @param databaseConf
     * @param tableConf
     * @return
     * @throws Exception
     */
    protected abstract QueryMethodResult getIndexesMethod(S connect, D databaseConf, T tableConf) throws Exception;

    /**
     * 创建表方法
     *
     * @param connect
     * @param queryEntity
     * @param databaseConf
     * @param tableConf
     * @return
     * @throws Exception
     */
    protected abstract QueryMethodResult createTableMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception;

    /**
     * 删除表方法
     *
     * @param connect
     * @param queryEntity
     * @param databaseConf
     * @param tableConf
     * @return
     * @throws Exception
     */
    protected abstract QueryMethodResult dropTableMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception;

    protected QueryMethodResult emptyTableMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "AbstractDbModuleBaseService.emptyTableMethod");
    }

    /**
     * 删除索引方法
     *
     * @param connect
     * @param queryEntity
     * @param databaseConf
     * @param tableConf
     * @return
     * @throws Exception
     */
    protected abstract QueryMethodResult deleteIndexMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception;

    /**
     * 创建索引
     *
     * @param connect
     * @param databaseConf
     * @param tableConf
     * @param queryEntity
     * @return
     * @throws Exception
     */
    protected abstract QueryMethodResult createIndexMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception;

    /**
     * 插入方法
     *
     * @param connect
     * @param queryEntity
     * @param databaseConf
     * @param tableConf
     * @return
     * @throws Exception
     */
    protected abstract QueryMethodResult insertMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception;

    /**
     * 更新方法
     *
     * @param connect
     * @param queryEntity
     * @param databaseConf
     * @param tableConf
     * @return
     * @throws Exception
     */
    protected abstract QueryMethodResult updateMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception;

    /**
     * 删除方法
     *
     * @param connect
     * @param queryEntity
     * @param databaseConf
     * @param tableConf
     * @return
     * @throws Exception
     */
    protected abstract QueryMethodResult delMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception;

    /**
     * 组件监控方法
     *
     * @param connect
     * @param databaseConf
     * @return
     * @throws Exception
     */
    protected abstract QueryMethodResult monitorStatusMethod(S connect, D databaseConf) throws Exception;

    /**
     * 查询表结构信息
     *
     * @param connect
     * @param databaseConf
     * @param tableConf
     * @return
     * @throws Exception
     */
    protected abstract QueryMethodResult querySchemaMethod(S connect, D databaseConf, T tableConf) throws Exception;

    /**
     * 获取库schema列表
     *
     * @param connect
     * @param databaseConf
     * @param tableConf
     * @return
     * @throws Exception
     */
    protected abstract QueryMethodResult queryDatabaseSchemaMethod(S connect, D databaseConf, T tableConf) throws Exception;

    /**
     * 游标查询
     *
     * @param connect
     * @param queryEntity
     * @param databaseConf
     * @param tableConf
     * @param cursorCache
     * @param consumer
     * @param rollAllData
     * @return
     * @throws Exception
     */
    protected abstract QueryCursorMethodResult queryCursorMethod(S connect, Q queryEntity, D databaseConf, T tableConf, C cursorCache, Consumer<Map<String, Object>> consumer, Boolean rollAllData) throws Exception;

    /**
     * 新增或者更新方法
     *
     * @param connect
     * @param queryEntity
     * @param databaseConf
     * @param tableConf
     * @return
     * @throws Exception
     */
    protected abstract QueryMethodResult saveOrUpdateMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception;

    /**
     *
     * 批量新增或者更新方法
     *
     * @param connect
     * @param queryEntity
     * @param databaseConf
     * @param tableConf
     * @return
     * @throws Exception
     */
    protected abstract QueryMethodResult saveOrUpdateBatchMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception;

    /**
     * 修改表方法
     *
     * @param connect
     * @param queryEntity
     * @param databaseConf
     * @param tableConf
     * @return
     * @throws Exception
     */
    protected abstract QueryMethodResult alterTableMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception;

    /**
     * 判断表是否存在
     *
     * @param connect
     * @param databaseConf
     * @param tableConf
     * @return
     * @throws Exception
     */
    protected abstract QueryMethodResult tableExistsMethod(S connect, D databaseConf, T tableConf) throws Exception;

    /**
     * 查询表配置信息，如分区、分布、分片等等
     *
     * @param connect
     * @param databaseConf
     * @param tableConf
     * @return
     * @throws Exception
     */
    protected abstract QueryMethodResult queryTableInformationMethod(S connect, D databaseConf, T tableConf) throws Exception;

    /**
     * 查询数据库列表
     *
     * @param connect
     * @param queryEntity
     * @param databaseConf
     * @param tableConf
     * @return
     * @throws Exception
     */
    protected abstract QueryMethodResult queryListDatabaseMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception;


    /**
     * 查询数据库信息
     *
     * @param connect
     * @param queryEntity
     * @param databaseConf
     * @param tableConf
     * @return
     * @throws Exception
     */
    protected abstract QueryMethodResult queryDatabaseInformationMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception;


    /**
     * 创建数据库
     *
     * @param connect
     * @param queryEntity
     * @param databaseConf
     * @param tableConf
     * @return
     * @throws Exception
     */
    protected QueryMethodResult mergeDataMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "AbstractDbModuleBaseService.mergeDataMethod");
    }

    /**
     * 创建数据库
     *
     * @param connect
     * @param queryEntity
     * @param databaseConf
     * @param tableConf
     * @return
     * @throws Exception
     */
    protected QueryMethodResult createDatabaseMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "AbstractDbModuleBaseService.createDatabaseMethod");
    }

    /**
     * 更新数据库
     *
     * @param connect
     * @param queryEntity
     * @param databaseConf
     * @param tableConf
     * @return
     * @throws Exception
     */
    protected QueryMethodResult updateDatabaseMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "AbstractDbModuleBaseService.updateDatabaseMethod");
    }

    /**
     * 删除数据库
     *
     * @param connect
     * @param queryEntity
     * @param databaseConf
     * @param tableConf
     * @return
     * @throws Exception
     */
    protected QueryMethodResult dropDatabaseMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "AbstractDbModuleBaseService.dropDatabaseMethod");
    }

    /**
     * 查询分区信息
     *
     * @param connect
     * @param queryEntity
     * @param databaseConf
     * @param tableConf
     * @return
     * @throws Exception
     */
    protected QueryMethodResult queryPartitionInformationMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "AbstractDbModuleBaseService.queryPartitionInformationMethod");
    }
    /**
     * 查询分区信息V2
     *
     * @param connect
     * @param queryEntity
     * @param databaseConf
     * @param tableConf
     * @return
     * @throws Exception
     */
    protected QueryMethodResult queryPartitionInformationMethodV2(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "AbstractDbModuleBaseService.queryPartitionInformationMethodV2");
    }

    protected QueryMethodResult publishMethod(S connect, Q queryEntity, D databaseConf, T tableConf) {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "AbstractDbModuleBaseService.publishMethod");
    }

    protected QueryMethodResult subscribeMethod(S connect, Q queryEntity, D databaseConf, T tableConf) {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "AbstractDbModuleBaseService.subscribeMethod");
    }

    /**
     * 支持的能力
     * @return
     */
    @Override
    public List<SupportPower> getSupportList() {

        Set<SupportPower> supportPowers = new HashSet<>();

        Class<?> aClass = this.getClass();

        while (!aClass.equals(AbstractDbModuleBaseService.class)) {
            Support[] supports = aClass.getDeclaredAnnotationsByType(Support.class);
            for (Support support : supports) {
                SupportPower[] value = support.value();
                supportPowers.addAll(Arrays.asList(value));
            }
            aClass = aClass.getSuperclass();
        }

        return new ArrayList<>(supportPowers);
    }

    /**
     * 支持的能力 getSupportList() 一样的功能，输出包装一层
     * @return
     */
    @Override
    public PageResult supportList() {

        List<SupportPower> supportList = getSupportList();

        PageResult result = new PageResult();
        List<Map<String, Object>> dataList = new ArrayList<>(supportList.size());
        for (SupportPower supportPower : supportList) {
            Map<String, Object> data = new HashMap<>();
            data.put("name", supportPower.name());
            data.put("desc", supportPower.desc);
            dataList.add(data);
        }

        result.setRows(dataList);

        return result;
    }

    @Override
    public void openTransaction() {
        openTransaction((IsolationLevel) null);
    }

    @Override
    public void openTransaction(IsolationLevel isolationLevel) {
        // 1.生成一个UUID
        // UUIDGenerator.generate();
        // 2.将 UUID 放入 ThreadLocal 中，标志位key：transactionId
        ThreadLocalUtil.set(TransactionConstant.ID, UUIDGenerator.generate());
        ThreadLocalUtil.set(TransactionConstant.FIRST,TransactionConstant.FIRST);
        if (isolationLevel != null) {
            ThreadLocalUtil.set(TransactionConstant.ISOLATION_LEVEL, isolationLevel.value());
        }
    }

    @Override
    public void openTransaction(String transactionId) {
        openTransaction(transactionId, null);
    }

    @Override
    public void openTransaction(String transactionId, IsolationLevel isolationLevel) {
        if(StringUtils.isBlank(transactionId)) {
            throw new BusinessException(TRANSACTION_ID_NULL_EXCEPTION);
        }
        ThreadLocalUtil.set(TransactionConstant.ID, transactionId);
        ThreadLocalUtil.set(TransactionConstant.FIRST,TransactionConstant.FIRST);
        if (isolationLevel != null) {
            ThreadLocalUtil.set(TransactionConstant.ISOLATION_LEVEL, isolationLevel.value());
        }
    }

    @Override
    public void fetchTransaction(String transactionId) {
        if(StringUtils.isBlank(transactionId)) {
            throw new BusinessException(TRANSACTION_ID_NULL_EXCEPTION);
        }
        ThreadLocalUtil.set(TransactionConstant.ID, transactionId);
    }

    @Override
    public void openTransactionByDatabase(DatabaseSetting databaseSetting) {
        openTransactionByDatabase(databaseSetting, null);
    }

    @Override
    public void openTransactionByDatabase(DatabaseSetting databaseSetting, IsolationLevel isolationLevel) {
        String databaseKey = getDatabaseKey(databaseSetting);
        ThreadLocalUtil.set(databaseKey.hashCode() + "_" + TransactionConstant.ID, UUIDGenerator.generate());
        ThreadLocalUtil.set(databaseKey.hashCode() + "_" + TransactionConstant.FIRST,TransactionConstant.FIRST);
        if (isolationLevel != null) {
            ThreadLocalUtil.set(databaseKey.hashCode() + "_" + TransactionConstant.ISOLATION_LEVEL, isolationLevel.value());
        }
    }

    @Override
    public DbTransactionModuleService newTransaction(DatabaseSetting databaseSetting) {
        return newTransaction(databaseSetting, null);
    }

    @Override
    public DbTransactionModuleService newTransaction(DatabaseSetting databaseSetting, IsolationLevel isolationLevel) {
        String databaseKey = getDatabaseKey(databaseSetting);
        String transactionIdKey = databaseKey.hashCode() + "_" + TransactionConstant.ID;
        String transactionFirstKey = databaseKey.hashCode() + "_" + TransactionConstant.FIRST;
        String isolationLevelKey = null;
        if (isolationLevel != null) {
            isolationLevelKey = databaseKey.hashCode() + "_" + TransactionConstant.ISOLATION_LEVEL;
        }
        String transactionId = UUIDGenerator.generate();
        return getDbTransactionModuleService(databaseSetting, transactionIdKey, transactionFirstKey, isolationLevelKey, transactionId, isolationLevel);
    }

    protected DbTransactionModuleService getDbTransactionModuleService(DatabaseSetting databaseSetting,
                                                                       String transactionIdKey,
                                                                       String transactionFirstKey,
                                                                       String isolationLevelKey,
                                                                       String transactionId,
                                                                       IsolationLevel isolationLevel) {
        return new DbTransactionModuleService(
                databaseSetting,
                this,
                transactionId,
                isolationLevel,
                transactionIdKey,
                transactionFirstKey,
                isolationLevelKey
        );
    }

    private String getDatabaseKey(DatabaseSetting databaseSetting) {
        String connSetting = databaseSetting.getConnSetting();
        DatabaseConf databaseConf = new DatabaseConf();
        databaseConf.setConnSetting(connSetting);
        D moduleConfig = this.helper.getDbModuleConfig(databaseConf);
        String cacheKey = this.helper.getCacheKey(moduleConfig, null);
        return cacheKey;
    }

    @Override
    public void rollback(DatabaseSetting databaseSetting) throws Exception {
        //first存在，说明没有进行过数据库操作
        if(!this.existFirstFlag(databaseSetting)) {
            TransactionManager transactionManager = this.getTransactionManager(databaseSetting);
            // 3.执行 TransactionManager.rollback();
            transactionManager.rollback();
        }
        //事务完成，需要删除事务id
        String databaseKey = getDatabaseKey(databaseSetting);
        ThreadLocalUtil.remove(databaseKey.hashCode() + "_" + TransactionConstant.FIRST);
        ThreadLocalUtil.remove(databaseKey.hashCode() + "_" + TransactionConstant.ID);
        ThreadLocalUtil.remove(databaseKey.hashCode() + "_" + TransactionConstant.ISOLATION_LEVEL);
    }

    @Override
    public void rollback() throws Exception {
        //first存在，说明没有进行过数据库操作
        if(!this.existFirstFlag()) {
            TransactionManager transactionManager = this.getTransactionManager();
            // 3.执行 TransactionManager.rollback();
            transactionManager.rollback();
        }
        //事务完成，需要删除事务id
        ThreadLocalUtil.remove();
    }

    @Override
    public void commit() throws Exception {
        //first存在，说明没有进行过数据库操作
        if(!this.existFirstFlag()) {
            TransactionManager transactionManager = this.getTransactionManager();
            // 3.执行 TransactionManager.commit();
            transactionManager.commit();
        }
        //事务完成，需要删除事务id
        ThreadLocalUtil.remove();
    }

    @Override
    public void commit(DatabaseSetting databaseSetting) throws Exception {
        //first存在，说明没有进行过数据库操作
        if(!this.existFirstFlag(databaseSetting)) {
            TransactionManager transactionManager = this.getTransactionManager(databaseSetting);
            // 3.执行 TransactionManager.commit();
            transactionManager.commit();
        }
        String databaseKey = getDatabaseKey(databaseSetting);
        //事务完成，需要删除事务id
        ThreadLocalUtil.remove(databaseKey.hashCode() + "_" + TransactionConstant.FIRST);
        ThreadLocalUtil.remove(databaseKey.hashCode() + "_" + TransactionConstant.ID);
        ThreadLocalUtil.remove(databaseKey.hashCode() + "_" + TransactionConstant.ISOLATION_LEVEL);
    }

    /**
     * 获取事务管理器
     * @return
     */
    protected boolean existFirstFlag(DatabaseSetting databaseSetting) {
        String databaseKey = getDatabaseKey(databaseSetting);
        String first = (String) ThreadLocalUtil.get(databaseKey.hashCode() + "_" + TransactionConstant.FIRST);
        return !(first == null);
    }

    /**
     * 获取事务管理器
     * @return
     */
    protected boolean existFirstFlag() {
        String first = (String) ThreadLocalUtil.get(TransactionConstant.FIRST);
        return !(first == null);
    }

    /**
     * 获取事务管理器
     * @return
     */
    protected TransactionManager getTransactionManager(DatabaseSetting databaseSetting) {
        // 1.从 ThreadLocal 中获取 UUID
        String databaseKey = getDatabaseKey(databaseSetting);
        String transactionId = (String) ThreadLocalUtil.get(databaseKey.hashCode() + "_" + TransactionConstant.ID);
        // 2.从 helper 的事务管理对象缓存池中获取并移除 UUID 对应的缓存对象：TransactionManager->this.helper.getTransactionManager() this.helper.removeTransactionManager()
        TransactionManager transactionManager = this.helper.getTransactionManager(transactionId);
        if (transactionManager == null) {
            throw new BusinessException(ExceptionCode.TRANSACTION_NULL_EXCEPTION);
        }
        this.helper.removeTransactionManager(transactionId);
        return transactionManager;
    }

    /**
     * 获取事务管理器
     * @return
     */
    protected TransactionManager getTransactionManager() {
        // 1.从 ThreadLocal 中获取 UUID
        String transactionId = (String) ThreadLocalUtil.get(TransactionConstant.ID);
        // 2.从 helper 的事务管理对象缓存池中获取并移除 UUID 对应的缓存对象：TransactionManager->this.helper.getTransactionManager() this.helper.removeTransactionManager()
        TransactionManager transactionManager = this.helper.getTransactionManager(transactionId);
        if (transactionManager == null) {
            throw new BusinessException(ExceptionCode.TRANSACTION_NULL_EXCEPTION);
        }
        this.helper.removeTransactionManager(transactionId);
        return transactionManager;
    }

    @Override
    @Trace
    public PageResult queryOne(DatabaseSetting databaseSetting, TableSetting tableSetting, QueryParamCondition queryParamCondition) throws Exception {
        return queryListForPage(databaseSetting, tableSetting, queryParamCondition);
    }

    @Override
    @Trace
    public PageResult queryList(DatabaseSetting databaseSetting, TableSetting tableSetting, QueryParamCondition queryParamCondition) throws Exception {
        return queryListForPage(databaseSetting, tableSetting, queryParamCondition);
    }

    @Override
    @Trace
    public PageResult queryListForPage(DatabaseSetting databaseSetting, TableSetting tableSetting, QueryParamCondition queryParamCondition) throws Exception {
        QueryMethodResult queryResult = queryExecute(databaseSetting, tableSetting, queryParamCondition, DbMethodEnum.QUERY_PAGE,
                (connect, queryEntity, databaseConf, tableConf) -> queryMethod((S) connect, (Q) queryEntity, (D) databaseConf, (T) tableConf));
        if (queryResult.isSuccess()) {
            // 无查询结果返回
            if (CollectionUtils.isEmpty(queryResult.getRows())) {
                return new PageResult(true, ReturnCodeEnum.CODE_SUCCESS_QUERY_RESULT_NULL);
            } else {
                return new PageResult(queryResult.getTotal(), queryResult.getRows(), true, ReturnCodeEnum.CODE_SUCCESS);
            }
        } else {
            PageResult pageResult = new PageResult(queryResult.getTotal(), queryResult.getRows(), false, ReturnCodeEnum.CODE_BUSINESS_FAIL);
            return pageResult;
        }
    }

    @Override
    @Trace
    public PageResult queryCursor(DatabaseSetting databaseSetting, TableSetting tableSetting, QueryParamCondition queryParamCondition, Consumer<Map<String, Object>> consumer) throws Exception {
        // 设置游标查询不限制读取时间
        queryParamCondition.setLimitReadTime(Boolean.FALSE);
        QueryMethodResult queryResult = queryExecute(databaseSetting, tableSetting, queryParamCondition, DbMethodEnum.QUERY_CURSOR,
                (connect, queryEntity, databaseConf, tableConf) -> {
                    // 判断是单次游标查询所有数据(true)，还是每次请求只查询批量数即返回(false)
                    Boolean cursorRollAllData = queryParamCondition.getCursorRollAllData();
                    // 若存在游标ID，并且不是一次获取游标，则获取对应缓存游标信息
                    if (StringUtils.isNotBlank(queryParamCondition.getCursorId()) && !queryParamCondition.getFirstCursor()) {
                        String hashCacheKey = helper.cacheKeyForCursor(queryParamCondition.getCursorId(), (D) databaseConf, (T) tableConf);
                        C cursorCache = helper.findCursorCache(hashCacheKey);
                        if (StringUtils.isNotBlank(queryParamCondition.getCursorId()) && cursorCache == null) {
                            // 若未获取到缓存则抛出异常
                            log.debug("非首次游标查询,并且缓存中无游标缓存相关信息,直接返回结果, dbId: [{}]", tableSetting.getDbId());
                            throw new BusinessException(ExceptionCode.NOT_FOUND_CURSOR, tableSetting.getDbIdOrTableName(), queryParamCondition.getCursorId());
                        }
                        // 从缓存中获取初次查询报文
                        if (cursorCache.getQueryEntity() != null) {
                            queryEntity = cursorCache.getQueryEntity();
                        }
                        QueryCursorMethodResult queryCursorMethodResult = queryCursorMethod((S) connect, (Q) queryEntity, (D) databaseConf, (T) tableConf, cursorCache, consumer, cursorRollAllData);
                        // 获取返回的游标信息
                        AbstractCursorCache newCursorCache = queryCursorMethodResult.getCursorCache();
                        // 若不为空，则更新游标信息缓存
                        if (newCursorCache != null) {
                            getHelper().createOrUpdateCursorCache(getHelper().cacheKeyForCursor(queryParamCondition.getCursorId(), (D) databaseConf, (T) tableConf), newCursorCache);
                            queryCursorMethodResult.setCursorId(queryParamCondition.getCursorId());
                        }
                        return queryCursorMethodResult;
                    } else {
                        QueryCursorMethodResult queryCursorMethodResult = queryCursorMethod((S) connect, (Q) queryEntity, (D) databaseConf, (T) tableConf, null, consumer, cursorRollAllData);
                        // 获取返回的游标信息
                        AbstractCursorCache cursorCache = queryCursorMethodResult.getCursorCache();
                        // 若不为空，则更新缓存游标信息
                        if (cursorCache != null) {
                            // 设置初次查询报文
                            cursorCache.setQueryEntity(queryEntity);
                            // 若入参无指定游标ID序列，则自动生成一个序列
                            String cursorId = queryParamCondition.getCursorId();
                            if (StringUtils.isBlank(cursorId)) {
                                cursorId = getHelper().createCursorId(tableSetting.getDbIdOrTableName());
                            }
                            getHelper().createOrUpdateCursorCache(getHelper().cacheKeyForCursor(cursorId, (D) databaseConf, (T) tableConf), cursorCache);
                            queryCursorMethodResult.setCursorId(cursorId);
                        }
                        return queryCursorMethodResult;
                    }
                });
        if (queryResult.isSuccess()) {
            PageResult pageResult = new PageResult(queryResult.getTotal(), queryResult.getRows(), queryResult.isSuccess(), ReturnCodeEnum.CODE_SUCCESS);
            pageResult.setCursorId(((QueryCursorMethodResult) queryResult).getCursorId());
            return pageResult;
        } else {
            //出现业务异常
            return new PageResult(queryResult.getTotal(), queryResult.getRows(), false, ReturnCodeEnum.CODE_BUSINESS_FAIL);
        }
    }

    @Override
    @Trace
    public PageResult statisticalLine(DatabaseSetting databaseSetting, TableSetting tableSetting, QueryParamCondition queryParamCondition) throws Exception {
        queryParamCondition.setCountFlag(true);
        QueryMethodResult queryResult = null;
        try {
            queryResult = queryExecute(databaseSetting, tableSetting, queryParamCondition, DbMethodEnum.STATISTICAl_LINE, (connect, queryEntity, databaseConf, tableConf) -> countMethod((S) connect, (Q) queryEntity, (D) databaseConf, (T) tableConf));
        } catch (BusinessException e) {
            // 若未目标表不存在，则状态未成功，但是告警表不存在
            if (e.getCode() == ExceptionCode.NOT_FOUND_TABLE_EXCEPTION.getCode()) {
                log.error("当前 dbId/tableName: [{}] 数据量统计失败, 未找到匹配的目标表, 将返回数据量为 0!", tableSetting.getDbIdOrTableName());
                return new PageResult(true, ReturnCodeEnum.CODE_COUNT_NOT_FOUND_TABLE_WARN);
            }
            throw e;
        }
        return new PageResult(queryResult.getTotal(), queryResult.getRows(), true, ReturnCodeEnum.CODE_SUCCESS);
    }

    @Override
    @Trace
    public PageResult insert(DatabaseSetting databaseSetting, TableSetting tableSetting, AddParamCondition addParamCondition) throws Exception {
        return insertBatch(databaseSetting, tableSetting, addParamCondition);
    }

    @Override
    @Trace
    public PageResult insertBatch(DatabaseSetting databaseSetting, TableSetting tableSetting, AddParamCondition addParamCondition) throws Exception {
        QueryMethodResult queryResult = queryExecute(databaseSetting, tableSetting, addParamCondition, DbMethodEnum.INSERT_BATCH, (connect, queryEntity, databaseConf, tableConf) -> insertMethod((S) connect, (Q) queryEntity, (D) databaseConf, (T) tableConf));
        if (queryResult.isSuccess()) {
            return new PageResult(queryResult.getTotal(), queryResult.getRows(), true, ReturnCodeEnum.CODE_SUCCESS);
        } else {
            //出现业务异常
            return new PageResult(queryResult.getTotal(), queryResult.getRows(), false, ReturnCodeEnum.CODE_BUSINESS_FAIL);
        }
    }

    @Override
    @Trace
    public PageResult update(DatabaseSetting databaseSetting, TableSetting tableSetting, UpdateParamCondition updateParamCondition) throws Exception {
        QueryMethodResult queryResult = queryExecute(databaseSetting, tableSetting, updateParamCondition, DbMethodEnum.UPDATE, (connect, queryEntity, databaseConf, tableConf) -> updateMethod((S) connect, (Q) queryEntity, (D) databaseConf, (T) tableConf));
        return new PageResult(queryResult.getTotal(), queryResult.getRows(), true, ReturnCodeEnum.CODE_SUCCESS);
    }

    @Override
    @Trace
    public PageResult delete(DatabaseSetting databaseSetting, TableSetting tableSetting, DelParamCondition delParamCondition) throws Exception {
        QueryMethodResult queryResult = queryExecute(databaseSetting, tableSetting, delParamCondition, DbMethodEnum.DELETE, (connect, queryEntity, databaseConf, tableConf) -> delMethod((S) connect, (Q) queryEntity, (D) databaseConf, (T) tableConf));
        return new PageResult(queryResult.getTotal(), queryResult.getRows(), true, ReturnCodeEnum.CODE_SUCCESS);
    }

    @Override
    @Trace
    public PageResult getIndexes(DatabaseSetting databaseSetting, TableSetting tableSetting) throws Exception {
        QueryMethodResult queryMethodResult = queryExecute(databaseSetting, tableSetting, null, DbMethodEnum.QUERY_INDEXES, (connect, queryEntity, databaseConf, tableConf) -> getIndexesMethod((S) connect, (D) databaseConf, (T) tableConf));
        return new PageResult(queryMethodResult.getTotal(), queryMethodResult.getRows(), true, ReturnCodeEnum.CODE_SUCCESS);
    }

    @Override
    @Trace
    public PageResult createIndex(DatabaseSetting databaseSetting, TableSetting tableSetting, IndexParamCondition indexParamCondition) throws Exception {
        try {
            queryExecute(databaseSetting, tableSetting, indexParamCondition, DbMethodEnum.CREATE_INDEXES, (connect, queryEntity, databaseConf, tableConf) -> createIndexMethod((S) connect, (Q) queryEntity, (D) databaseConf, (T) tableConf));
            return new PageResult(0, null, true, ReturnCodeEnum.CODE_SUCCESS);
        } catch (Exception e) {
            log.error("dbType: [{}] createIndex is error! dbInfo: [{}]", DbResourceEnum.findDbResourceEnum(databaseSetting.getDbType()).name(), databaseSetting.toString(), e);
            return new PageResult(0, null, false, ReturnCodeEnum.CODE_CREATE_INDEX_FAIL, e.getMessage());
        }
    }

    @Override
    @Trace
    public PageResult testConnection(DatabaseSetting databaseSetting) throws Exception {
        try {
            queryExecute(databaseSetting, null, null, DbMethodEnum.TEST_CONNECTION, (connect, queryEntity, databaseConf, tableConf) -> testConnectMethod((S) connect, (D) databaseConf));
            return new PageResult(true, ReturnCodeEnum.CODE_SUCCESS);
        } catch (Exception e) {
            log.error("dbType: [{}] testConnection is error! dbInfo: [{}]", DbResourceEnum.findDbResourceEnum(databaseSetting.getDbType()).name(), databaseSetting.toString(), e);
            return new PageResult(0, null, false, ReturnCodeEnum.CODE_TEST_CONNECT_FAIL, e.getMessage());
        }
    }

    @Override
    @Trace
    public PageResult changelog(DatabaseSetting databaseSetting) throws Exception {
        try {
            QueryMethodResult queryMethodResult = queryExecute(databaseSetting, null, null, DbMethodEnum.CHANGELOG, (connect, queryEntity, databaseConf, tableConf) -> changelog((S) connect, (D) databaseConf));
            return new PageResult(queryMethodResult.getTotal(), queryMethodResult.getRows(), true, ReturnCodeEnum.CODE_SUCCESS);
        } catch (Exception e) {
            log.error("dbType: [{}] isOpenBinlog is error! dbInfo: [{}]", DbResourceEnum.findDbResourceEnum(databaseSetting.getDbType()).name(), databaseSetting.toString(), e);
            return new PageResult(0, null, false, ReturnCodeEnum.CODE_TEST_CONNECT_FAIL, e.getMessage());
        }
    }

    @Override
    @Trace
    public PageResult queryListTable(DatabaseSetting databaseSetting, QueryTablesCondition queryTablesCondition) throws Exception {
        if(queryTablesCondition == null) {
            queryTablesCondition = new QueryTablesCondition();
        }
        QueryMethodResult queryMethodResult = queryExecute(databaseSetting, null, queryTablesCondition, DbMethodEnum.QUERY_LIST_TABLE, (connect, queryEntity, databaseConf, tableConf) -> showTablesMethod((S) connect, (Q) queryEntity, (D) databaseConf));
        return new PageResult(queryMethodResult.getTotal(), queryMethodResult.getRows(), true, ReturnCodeEnum.CODE_SUCCESS);
    }

    @Override
    @Trace
    public PageResult queryListTable(DatabaseSetting databaseSetting) throws Exception {
        QueryMethodResult queryMethodResult = queryExecute(databaseSetting, null, new QueryTablesCondition(), DbMethodEnum.QUERY_LIST_TABLE, (connect, queryEntity, databaseConf, tableConf) -> showTablesMethod((S) connect, (Q) queryEntity, (D) databaseConf));
        return new PageResult(queryMethodResult.getTotal(), queryMethodResult.getRows(), true, ReturnCodeEnum.CODE_SUCCESS);
    }

    @Override
    @Trace
    public PageResult createTable(DatabaseSetting databaseSetting, TableSetting tableSetting, CreateTableParamCondition createTableParamCondition) throws Exception {
        try {
            queryExecute(databaseSetting, tableSetting, createTableParamCondition, DbMethodEnum.CREATE_TABLE, (connect, queryEntity, databaseConf, tableConf) -> createTableMethod((S) connect, (Q) queryEntity, (D) databaseConf, (T) tableConf));
            return new PageResult(0, null, true, ReturnCodeEnum.CODE_SUCCESS);
        } catch (Exception e) {
            log.error("dbType: [{}] createTable is error! database: [{}] table: [{}]", DbResourceEnum.findDbResourceEnum(databaseSetting.getDbType()).name(), databaseSetting.toString(), tableSetting.toString(), e);
            return new PageResult(0, null, false, ReturnCodeEnum.CODE_CREATE_TABLE_FAIL, e.getMessage());
        }
    }

    @Override
    @Trace
    public PageResult dropTable(DatabaseSetting databaseSetting, TableSetting tableSetting, DropTableParamCondition dropTableParamCondition) throws Exception {
        queryExecute(databaseSetting, tableSetting, dropTableParamCondition, DbMethodEnum.DROP_TABLE, (connect, queryEntity, databaseConf, tableConf) -> dropTableMethod((S) connect, (Q) queryEntity, (D) databaseConf, (T) tableConf));
        return new PageResult(0, null, true, ReturnCodeEnum.CODE_SUCCESS);
    }

    @Override
    @Trace
    public PageResult emptyTable(DatabaseSetting databaseSetting, TableSetting tableSetting, EmptyTableParamCondition emptyTableParamCondition) throws Exception {
        queryExecute(databaseSetting, tableSetting, emptyTableParamCondition, DbMethodEnum.EMPTY_TABLE, (connect, queryEntity, databaseConf, tableConf) -> emptyTableMethod((S) connect, (Q) queryEntity, (D) databaseConf, (T) tableConf));
        return new PageResult(0, null, true, ReturnCodeEnum.CODE_SUCCESS);
    }

    @Override
    @Trace
    public PageResult deleteIndex(DatabaseSetting databaseSetting, TableSetting tableSetting, IndexParamCondition indexParamCondition) throws Exception {
        try {
            queryExecute(databaseSetting, tableSetting, indexParamCondition, DbMethodEnum.DELETE_INDEX, (connect, queryEntity, databaseConf, tableConf) -> deleteIndexMethod((S) connect, (Q) queryEntity, (D) databaseConf, (T) tableConf));
            return new PageResult(0, null, true, ReturnCodeEnum.CODE_SUCCESS);
        } catch (Exception e) {
            log.error("dbType: [{}] deleteIndex is error! dbInfo: [{}-{}]", DbResourceEnum.findDbResourceEnum(databaseSetting.getDbType()).name(), databaseSetting.toString(), tableSetting.toString(), e);
            return new PageResult(0, null, false, ReturnCodeEnum.CODE_DROP_INDEX_FAIL, e.getMessage());
        }
    }

    @Override
    @Trace
    public PageResult monitorStatus(DatabaseSetting databaseSetting) throws Exception {
        QueryMethodResult queryMethodResult = queryExecute(databaseSetting, null, null, DbMethodEnum.MONITOR_STATUS, (connect, queryEntity, databaseConf, tableConf) -> monitorStatusMethod((S) connect, (D) databaseConf));
        return new PageResult(queryMethodResult.getTotal(), queryMethodResult.getRows(), true, ReturnCodeEnum.CODE_SUCCESS);
    }

    @Override
    @Trace
    public PageResult queryTableSchema(DatabaseSetting databaseSetting, TableSetting tableSetting) throws Exception {
        QueryMethodResult queryMethodResult = queryExecute(databaseSetting, tableSetting, null, DbMethodEnum.QUERY_TABLE_SCHEMA, (connect, queryEntity, databaseConf, tableConf) -> querySchemaMethod((S) connect, (D) databaseConf, (T) tableConf));
        return new PageResult(queryMethodResult.getTotal(), queryMethodResult.getRows(), true, ReturnCodeEnum.CODE_SUCCESS);
    }

    @Override
    @Trace
    public PageResult saveOrUpdate(DatabaseSetting databaseSetting, TableSetting tableSetting, UpsertParamCondition upsertParamCondition) throws Exception {
        QueryMethodResult queryResult = queryExecute(databaseSetting, tableSetting, upsertParamCondition, DbMethodEnum.UPSERT, (connect, queryEntity, databaseConf, tableConf) -> saveOrUpdateMethod((S) connect, (Q) queryEntity, (D) databaseConf, (T) tableConf));
        return new PageResult(queryResult.getTotal(), queryResult.getRows(), true, ReturnCodeEnum.CODE_SUCCESS);
    }

    @Override
    public PageResult saveOrUpdateBatch(DatabaseSetting databaseSetting, TableSetting tableSetting, UpsertParamBatchCondition upsertParamBatchCondition) throws Exception {
        QueryMethodResult queryResult = queryExecute(databaseSetting, tableSetting, upsertParamBatchCondition, DbMethodEnum.UPSERT, (connect, queryEntity, databaseConf, tableConf) -> saveOrUpdateBatchMethod((S) connect, (Q) queryEntity, (D) databaseConf, (T) tableConf));
        return new PageResult(queryResult.getTotal(), queryResult.getRows(), true, ReturnCodeEnum.CODE_SUCCESS);
    }

    @Override
    public PageResult queryDatabaseSchema(DatabaseSetting databaseSetting) throws Exception {
        QueryMethodResult queryMethodResult = queryExecute(databaseSetting, null, null, DbMethodEnum.QUERY_DATABASE_SCHEMA, (connect, queryEntity, databaseConf, tableConf) -> queryDatabaseSchemaMethod((S) connect, (D) databaseConf, (T) tableConf));
        return new PageResult(queryMethodResult.getTotal(), queryMethodResult.getRows(), true, ReturnCodeEnum.CODE_SUCCESS);
    }

    @Override
    public PageResult alterTable(DatabaseSetting databaseSetting, TableSetting tableSetting, AlterTableParamCondition alterTableParamCondition) throws Exception {
        try {
            queryExecute(databaseSetting, tableSetting, alterTableParamCondition, DbMethodEnum.ALTER_TABLE, (connect, queryEntity, databaseConf, tableConf) -> alterTableMethod((S) connect, (Q) queryEntity, (D) databaseConf, (T) tableConf));
            return new PageResult(0, null, true, ReturnCodeEnum.CODE_SUCCESS);
        } catch (Exception e) {
            log.error("dbType: [{}] alterTable is error! dbInfo: [{} - {}]", DbResourceEnum.findDbResourceEnum(databaseSetting.getDbType()).name(), databaseSetting.toString(), tableSetting.toString(), e);
            return new PageResult(0, null, false, ReturnCodeEnum.CODE_ALTER_TABLE_FAIL, e.getMessage());
        }
    }

    @Override
    public PageResult tableExists(DatabaseSetting databaseSetting, TableSetting tableSetting) throws Exception {
        QueryMethodResult queryMethodResult = queryExecute(databaseSetting, tableSetting, null, DbMethodEnum.TABLE_EXISTS, (connect, queryEntity, databaseConf, tableConf) -> tableExistsMethod((S) connect, (D) databaseConf, (T) tableConf));
        return new PageResult(queryMethodResult.getTotal(), queryMethodResult.getRows(), true, ReturnCodeEnum.CODE_SUCCESS);
    }

    @Override
    public PageResult queryTableInformation(DatabaseSetting databaseSetting, TableSetting tableSetting) throws Exception {
        QueryMethodResult queryMethodResult = queryExecute(databaseSetting, tableSetting, null, DbMethodEnum.TABLE_INFORMATION, (connect, queryEntity, databaseConf, tableConf) -> queryTableInformationMethod((S) connect, (D) databaseConf, (T) tableConf));
        return new PageResult(queryMethodResult.getTotal(), queryMethodResult.getRows(), true, ReturnCodeEnum.CODE_SUCCESS);
    }

    @Override
    public PageResult queryListDatabase(DatabaseSetting databaseSetting, QueryDatabasesCondition queryDatabasesCondition) throws Exception {
        QueryMethodResult queryMethodResult = queryExecute(databaseSetting, null, queryDatabasesCondition, DbMethodEnum.QUERY_LIST_DATABASE, (connect, queryEntity, databaseConf, tableConf) -> queryListDatabaseMethod((S) connect, (Q) queryEntity, (D) databaseConf, (T) tableConf));
        return new PageResult(queryMethodResult.getTotal(), queryMethodResult.getRows(), true, ReturnCodeEnum.CODE_SUCCESS);
    }

    @Override
    public PageResult queryDatabaseInformation(DatabaseSetting databaseSetting) throws Exception {
        QueryMethodResult queryMethodResult = queryExecute(databaseSetting, null, null, DbMethodEnum.DATABASE_INFORMATION, (connect, queryEntity, databaseConf, tableConf) -> queryDatabaseInformationMethod((S) connect, (Q) queryEntity, (D) databaseConf, (T) tableConf));
        return new PageResult(queryMethodResult.getTotal(), queryMethodResult.getRows(), true, ReturnCodeEnum.CODE_SUCCESS);
    }

    @Override
    public PageResult queryListDatabase(DatabaseSetting databaseSetting) throws Exception {
        QueryMethodResult queryMethodResult = queryExecute(databaseSetting, null, new QueryDatabasesCondition(), DbMethodEnum.QUERY_LIST_DATABASE, (connect, queryEntity, databaseConf, tableConf) -> queryListDatabaseMethod((S) connect, (Q) queryEntity, (D) databaseConf, (T) tableConf));
        return new PageResult(queryMethodResult.getTotal(), queryMethodResult.getRows(), true, ReturnCodeEnum.CODE_SUCCESS);
    }

    @Override
    public PageResult mergeData(DatabaseSetting databaseSetting, TableSetting tableSetting, MergeDataParamCondition mergeDataParamCondition) throws Exception {
        QueryMethodResult queryMethodResult = queryExecute(databaseSetting, tableSetting, mergeDataParamCondition, DbMethodEnum.MERGE_DATA, (connect, queryEntity, databaseConf, tableConf) -> mergeDataMethod((S) connect, (Q) queryEntity, (D) databaseConf, (T) tableConf));
        return new PageResult(queryMethodResult.getTotal(), queryMethodResult.getRows(), true, ReturnCodeEnum.CODE_SUCCESS);
    }

    @Override
    public PageResult createDatabase(DatabaseSetting databaseSetting, CreateDatabaseParamCondition createDatabaseParamCondition) throws Exception {
        QueryMethodResult queryMethodResult = queryExecute(databaseSetting, null, createDatabaseParamCondition, DbMethodEnum.CREATE_DATABASE, (connect, queryEntity, databaseConf, tableConf) -> createDatabaseMethod((S) connect, (Q) queryEntity, (D) databaseConf, (T) tableConf));
        return new PageResult(queryMethodResult.getTotal(), queryMethodResult.getRows(), true, ReturnCodeEnum.CODE_SUCCESS);
    }

    @Override
    public PageResult updateDatabase(DatabaseSetting databaseSetting, UpdateDatabaseParamCondition updateDatabaseParamCondition) throws Exception {
        QueryMethodResult queryMethodResult = queryExecute(databaseSetting, null, updateDatabaseParamCondition, DbMethodEnum.UPDATE_DATABASE, (connect, queryEntity, databaseConf, tableConf) -> updateDatabaseMethod((S) connect, (Q) queryEntity, (D) databaseConf, (T) tableConf));
        return new PageResult(queryMethodResult.getTotal(), queryMethodResult.getRows(), true, ReturnCodeEnum.CODE_SUCCESS);
    }

    @Override
    public PageResult dropDatabase(DatabaseSetting databaseSetting, DropDatabaseParamCondition dropDatabaseParamCondition) throws Exception {
        QueryMethodResult queryMethodResult = queryExecute(databaseSetting, null, dropDatabaseParamCondition, DbMethodEnum.DROP_DATABASE, (connect, queryEntity, databaseConf, tableConf) -> dropDatabaseMethod((S) connect, (Q) queryEntity, (D) databaseConf, (T) tableConf));
        return new PageResult(queryMethodResult.getTotal(), queryMethodResult.getRows(), true, ReturnCodeEnum.CODE_SUCCESS);
    }

    @Override
    public PageResult queryPartitionInformation(DatabaseSetting databaseSetting, TableSetting tableSetting) throws Exception {
        QueryMethodResult queryMethodResult = queryExecute(databaseSetting, tableSetting, null, DbMethodEnum.QUERY_PARTITION, (connect, queryEntity, databaseConf, tableConf) -> queryPartitionInformationMethod((S) connect, (Q) queryEntity, (D) databaseConf, (T) tableConf));
        return new PageResult(queryMethodResult.getTotal(), queryMethodResult.getRows(), true, ReturnCodeEnum.CODE_SUCCESS);
    }

    @Override
    public PageResult queryPartitionInformationV2(DatabaseSetting databaseSetting, TableSetting tableSetting) throws Exception {
        QueryMethodResult queryMethodResult = queryExecute(databaseSetting, tableSetting, null, DbMethodEnum.QUERY_PARTITION_V2, (connect, queryEntity, databaseConf, tableConf) -> queryPartitionInformationMethodV2((S) connect, (Q) queryEntity, (D) databaseConf, (T) tableConf));
        return new PageResult(queryMethodResult.getTotal(), queryMethodResult.getRows(), true, ReturnCodeEnum.CODE_SUCCESS);
    }

    @Override
    public void subscribe(DatabaseSetting databaseSetting, TableSetting tableSetting, SubscribeMessage subscribeMessage) throws Exception {
        queryExecute(databaseSetting, tableSetting, subscribeMessage, DbMethodEnum.SUBSCRIBE,
                (connect, queryEntity, databaseConf, tableConf) -> subscribeMethod((S) connect, (Q) queryEntity, (D) databaseConf, (T) tableConf));
    }

    @Override
    public void publish(DatabaseSetting databaseSetting, TableSetting tableSetting, PublishMessage message) throws Exception {
        queryExecute(databaseSetting, tableSetting, message, DbMethodEnum.PUBLISH,
                (connect, queryEntity, databaseConf, tableConf) -> publishMethod((S) connect, (Q) queryEntity, (D) databaseConf, (T) tableConf));

    }

    // ----------------------------------------------------------------------------------------
    // 废弃 API
    // ----------------------------------------------------------------------------------------
    @Override
    @Trace
    public PageResult queryOne(DbHandleEntity dbHandleEntity, QueryParamCondition queryParamCondition) throws Exception {
        return queryListForPage(dbHandleEntity, queryParamCondition);
    }

    @Override
    @Trace
    public PageResult queryList(DbHandleEntity dbHandleEntity, QueryParamCondition queryParamCondition) throws Exception {
        return queryListForPage(dbHandleEntity, queryParamCondition);
    }

    @Override
    @Trace
    public PageResult queryListForPage(DbHandleEntity dbHandleEntity, QueryParamCondition queryParamCondition) throws Exception {
        return queryListForPage(dbHandleEntity.getDatabaseSetting(), dbHandleEntity.getTableSetting(), queryParamCondition);
    }

    @Override
    @Trace
    public PageResult queryCursor(DbHandleEntity dbHandleEntity, QueryParamCondition queryParamCondition, Consumer<Map<String, Object>> consumer) throws Exception {
        return queryCursor(dbHandleEntity.getDatabaseSetting(), dbHandleEntity.getTableSetting(), queryParamCondition, consumer);
    }

    @Override
    @Trace
    public PageResult statisticalLine(DbHandleEntity dbHandleEntity, QueryParamCondition queryParamCondition) throws Exception {
        return statisticalLine(dbHandleEntity.getDatabaseSetting(), dbHandleEntity.getTableSetting(), queryParamCondition);
    }

    @Override
    @Trace
    public PageResult insert(DbHandleEntity dbHandleEntity, AddParamCondition addParamCondition) throws Exception {
        return insertBatch(dbHandleEntity, addParamCondition);
    }

    @Override
    @Trace
    public PageResult insertBatch(DbHandleEntity dbHandleEntity, AddParamCondition addParamCondition) throws Exception {
        return insertBatch(dbHandleEntity.getDatabaseSetting(), dbHandleEntity.getTableSetting(), addParamCondition);
    }

    @Override
    @Trace
    public PageResult update(DbHandleEntity dbHandleEntity, UpdateParamCondition updateParamCondition) throws Exception {
        return update(dbHandleEntity.getDatabaseSetting(), dbHandleEntity.getTableSetting(), updateParamCondition);
    }

    @Override
    @Trace
    public PageResult delete(DbHandleEntity dbHandleEntity, DelParamCondition delParamCondition) throws Exception {
        return delete(dbHandleEntity.getDatabaseSetting(), dbHandleEntity.getTableSetting(), delParamCondition);
    }

    @Override
    @Trace
    public PageResult getIndexes(DbHandleEntity dbHandleEntity) throws Exception {
        return getIndexes(dbHandleEntity.getDatabaseSetting(), dbHandleEntity.getTableSetting());
    }

    @Override
    @Trace
    public PageResult createIndex(DbHandleEntity dbHandleEntity, IndexParamCondition indexParamCondition) throws Exception {
        return createIndex(dbHandleEntity.getDatabaseSetting(), dbHandleEntity.getTableSetting(), indexParamCondition);
    }

    @Override
    @Trace
    public PageResult testConnection(DbHandleEntity dbHandleEntity) throws Exception {
        return testConnection(dbHandleEntity.getDatabaseSetting());
    }

    @Override
    @Trace
    public PageResult changelog(DbHandleEntity dbHandleEntity) throws Exception {
        return changelog(dbHandleEntity.getDatabaseSetting());
    }

    @Override
    @Trace
    public PageResult queryListTable(DbHandleEntity dbHandleEntity, QueryTablesCondition queryTablesCondition) throws Exception {
        return queryListTable(dbHandleEntity.getDatabaseSetting(), queryTablesCondition);
    }

    @Override
    @Trace
    public PageResult queryListDatabase(DbHandleEntity dbHandleEntity, QueryDatabasesCondition queryDatabasesCondition) throws Exception {
        return queryListDatabase(dbHandleEntity.getDatabaseSetting(), queryDatabasesCondition);
    }

    @Override
    @Trace
    public PageResult queryListTable(DbHandleEntity dbHandleEntity) throws Exception {
        return queryListTable(dbHandleEntity.getDatabaseSetting());
    }

    @Override
    @Trace
    public PageResult createTable(DbHandleEntity dbHandleEntity, CreateTableParamCondition createTableParamCondition) throws Exception {
        return createTable(dbHandleEntity.getDatabaseSetting(), dbHandleEntity.getTableSetting(), createTableParamCondition);
    }

    @Override
    @Trace
    public PageResult dropTable(DbHandleEntity dbHandleEntity, DropTableParamCondition dropTableParamCondition) throws Exception {
        return dropTable(dbHandleEntity.getDatabaseSetting(), dbHandleEntity.getTableSetting(), dropTableParamCondition);
    }

    @Override
    @Trace
    public PageResult emptyTable(DbHandleEntity dbHandleEntity, EmptyTableParamCondition emptyTableParamCondition) throws Exception {
        return emptyTable(dbHandleEntity.getDatabaseSetting(), dbHandleEntity.getTableSetting(), emptyTableParamCondition);
    }

    @Override
    @Trace
    public PageResult deleteIndex(DbHandleEntity dbHandleEntity, IndexParamCondition indexParamCondition) throws Exception {
        return deleteIndex(dbHandleEntity.getDatabaseSetting(), dbHandleEntity.getTableSetting(), indexParamCondition);
    }

    @Override
    @Trace
    public PageResult monitorStatus(DbHandleEntity dbHandleEntity) throws Exception {
        return monitorStatus(dbHandleEntity.getDatabaseSetting());
    }

    @Override
    @Trace
    public PageResult queryTableSchema(DbHandleEntity dbHandleEntity) throws Exception {
        return queryTableSchema(dbHandleEntity.getDatabaseSetting(), dbHandleEntity.getTableSetting());
    }

    @Override
    @Trace
    public PageResult saveOrUpdate(DbHandleEntity dbHandleEntity, UpsertParamCondition upsertParamCondition) throws Exception {
        return saveOrUpdate(dbHandleEntity.getDatabaseSetting(), dbHandleEntity.getTableSetting(), upsertParamCondition);
    }

    @Override
    public PageResult queryDatabaseSchema(DbHandleEntity dbHandleEntity) throws Exception {
        return queryDatabaseSchema(dbHandleEntity.getDatabaseSetting());
    }

    @Override
    public PageResult alterTable(DbHandleEntity dbHandleEntity, AlterTableParamCondition alterTableParamCondition) throws Exception {
        return alterTable(dbHandleEntity.getDatabaseSetting(), dbHandleEntity.getTableSetting(), alterTableParamCondition);
    }

    @Override
    public PageResult tableExists(DbHandleEntity dbHandleEntity) throws Exception {
        return tableExists(dbHandleEntity.getDatabaseSetting(), dbHandleEntity.getTableSetting());
    }

    @Override
    public PageResult queryTableInformation(DbHandleEntity dbHandleEntity) throws Exception {
        return queryTableInformation(dbHandleEntity.getDatabaseSetting(), dbHandleEntity.getTableSetting());
    }

    @Override
    public PageResult queryDatabaseInformation(DbHandleEntity dbHandleEntity) throws Exception {
        return queryDatabaseInformation(dbHandleEntity.getDatabaseSetting());
    }

    @Override
    public PageResult mergeData(DbHandleEntity dbHandleEntity, MergeDataParamCondition mergeDataParamCondition) throws Exception {
        return mergeData(dbHandleEntity.getDatabaseSetting(), dbHandleEntity.getTableSetting(), mergeDataParamCondition);
    }

    @Override
    public PageResult createDatabase(DbHandleEntity dbHandleEntity, CreateDatabaseParamCondition createDatabaseParamCondition) throws Exception {
        return createDatabase(dbHandleEntity.getDatabaseSetting(), createDatabaseParamCondition);
    }

    @Override
    public PageResult updateDatabase(DbHandleEntity dbHandleEntity, UpdateDatabaseParamCondition updateDatabaseParamCondition) throws Exception {
        return updateDatabase(dbHandleEntity.getDatabaseSetting(), updateDatabaseParamCondition);
    }

    @Override
    public PageResult dropDatabase(DbHandleEntity dbHandleEntity, DropDatabaseParamCondition dropDatabaseParamCondition) throws Exception {
        return dropDatabase(dbHandleEntity.getDatabaseSetting(), dropDatabaseParamCondition);
    }

    @Override
    public PageResult queryPartitionInformation(DbHandleEntity dbHandleEntity) throws Exception {
        return queryPartitionInformation(dbHandleEntity.getDatabaseSetting(), dbHandleEntity.getTableSetting());
    }

    @Override
    public PageResult queryPartitionInformationV2(DbHandleEntity dbHandleEntity) throws Exception {
        return queryPartitionInformationV2(dbHandleEntity.getDatabaseSetting(), dbHandleEntity.getTableSetting());
    }

    /**
     * 查询参数转换
     *
     * @param queryParamCondition
     * @return
     */
    protected Q transitionQueryParam(QueryParamCondition queryParamCondition, D databaseConf, T tableConf) throws Exception {
        return (Q) dbModuleParamUtil.getQueryParam(queryParamCondition, databaseConf, tableConf);
    }

    /**
     * 插入参数转换
     *
     * @param addParamCondition
     * @return
     */
    protected Q transitionAddParam(AddParamCondition addParamCondition, D databaseConf, T tableConf) throws Exception {
        return (Q) dbModuleParamUtil.getAddParam(addParamCondition, databaseConf, tableConf);
    }

    /**
     * 更新参数转换
     *
     * @param updateParamCondition
     * @return
     */
    protected Q transitionUpdateParam(UpdateParamCondition updateParamCondition, D databaseConf, T tableConf) throws Exception {
        return (Q) dbModuleParamUtil.getUpdateParam(updateParamCondition, databaseConf, tableConf);
    }

    /**
     * 删除参数转换
     *
     * @param delParamCondition
     * @return
     */
    protected Q transitionDelParam(DelParamCondition delParamCondition, D databaseConf, T tableConf) throws Exception {
        return (Q) dbModuleParamUtil.getDelParam(delParamCondition, databaseConf, tableConf);
    }

    /**
     * 索引参数转换
     *
     * @param indexParamCondition
     * @return
     */
    private Q transitionCreateIndexParam(IndexParamCondition indexParamCondition, D databaseConf, T tableConf) throws Exception {
        return (Q) dbModuleParamUtil.getCreateIndexParam(indexParamCondition, databaseConf, tableConf);
    }

    /**
     * 索引参数转换
     *
     * @param indexParamCondition
     * @return
     */
    private Q transitionDropIndexParam(IndexParamCondition indexParamCondition, D databaseConf, T tableConf) throws Exception {
        return (Q) dbModuleParamUtil.getDropIndexParam(indexParamCondition, databaseConf, tableConf);
    }

    /**
     * 建表参数转换
     *
     * @param createTableParamCondition
     * @return
     */
    private Q transitionCreateTableParam(CreateTableParamCondition createTableParamCondition, D databaseConf, T tableConf) throws Exception {
        return (Q) dbModuleParamUtil.getCreateTableParam(createTableParamCondition, databaseConf, tableConf);
    }

    /**
     * 修改表参数转换
     *
     * @param alterTableParamCondition
     * @return
     */
    private Q transitionAlterTableParam(AlterTableParamCondition alterTableParamCondition, D databaseConf, T tableConf) throws Exception {
        return (Q) dbModuleParamUtil.getAlterTableParam(alterTableParamCondition, databaseConf, tableConf);
    }

    /**
     * 删表参数转换
     *
     * @param dropTableParamCondition
     * @return
     */
    private Q transitionDropTableParam(DropTableParamCondition dropTableParamCondition, D databaseConf, T tableConf) throws Exception {
        return (Q) dbModuleParamUtil.getDropTableParam(dropTableParamCondition, databaseConf, tableConf);
    }

    /**
     * 清空表参数转换
     *
     * @param emptyTableParamCondition
     * @return
     */
    private Q transitionEmptyTableParam(EmptyTableParamCondition emptyTableParamCondition, D databaseConf, T tableConf) throws Exception {
        return (Q) dbModuleParamUtil.getEmptyTableParam(emptyTableParamCondition, databaseConf, tableConf);
    }

    /**
     * 新增或删除参数转换
     *
     * @param paramCondition
     * @return
     */
    private Q transitionUpsertParam(UpsertParamCondition paramCondition, D databaseConf, T tableConf) throws Exception {
        return (Q) dbModuleParamUtil.transitionUpsertParam(paramCondition, databaseConf, tableConf);
    }

    /**
     * 批量新增或更新参数转换
     *
     * @param paramCondition
     * @return
     */
    private Q transitionUpsertParamBatch(UpsertParamBatchCondition paramCondition, D databaseConf, T tableConf) throws Exception {
        return (Q) dbModuleParamUtil.transitionUpsertParamBatch(paramCondition, databaseConf, tableConf);
    }

    /**
     * 基础通用操作执行器
     *
     * @param databaseSetting
     * @param tableSetting
     * @param paramCondition
     * @param dbMethodEnum
     * @param callback
     * @return
     * @throws Exception
     */
    protected QueryMethodResult queryExecute(DatabaseSetting databaseSetting, TableSetting tableSetting, Object paramCondition, DbMethodEnum dbMethodEnum, BaseDbModuleCallback callback) throws Exception {
        return (QueryMethodResult) this.interiorExecute(databaseSetting, tableSetting, paramCondition, dbMethodEnum, callback, new DbModuleParserCallback<Object, Q, D, T>() {
            @Override
            public Q parser(Object params, D database, T table) throws Exception{
                Q queryEntity = null;
                if (paramCondition instanceof QueryParamCondition) {
                    queryEntity = transitionQueryParam((QueryParamCondition) paramCondition, database, table);
                } else if (paramCondition instanceof UpdateParamCondition) {
                    queryEntity = transitionUpdateParam((UpdateParamCondition) paramCondition, database, table);
                    if (queryEntity == null) {
                        throw new BusinessException(ExceptionCode.PARAM_NULL_EXCEPTION, null);
                    }
                } else if (paramCondition instanceof DelParamCondition) {
                    queryEntity = transitionDelParam((DelParamCondition) paramCondition, database, table);
                    if (queryEntity == null) {
                        throw new BusinessException(ExceptionCode.PARAM_NULL_EXCEPTION, null);
                    }
                } else if (paramCondition instanceof AddParamCondition) {
                    queryEntity = transitionAddParam((AddParamCondition) paramCondition, database, table);
                    if (queryEntity == null) {
                        throw new BusinessException(ExceptionCode.PARAM_NULL_EXCEPTION, null);
                    }
                } else if (paramCondition instanceof IndexParamCondition) {
                    switch (dbMethodEnum) {
                        case CREATE_INDEXES:
                            queryEntity = transitionCreateIndexParam((IndexParamCondition) paramCondition, database, table);
                            break;
                        case DELETE_INDEX:
                            queryEntity = transitionDropIndexParam((IndexParamCondition) paramCondition, database, table);
                            break;
                    }
                    if (queryEntity == null) {
                        throw new BusinessException(ExceptionCode.PARAM_NULL_EXCEPTION, null);
                    }
                } else if (paramCondition instanceof CreateTableParamCondition) {
                    queryEntity = transitionCreateTableParam((CreateTableParamCondition) paramCondition, database, table);
                    if (queryEntity == null) {
                        throw new BusinessException(ExceptionCode.PARAM_NULL_EXCEPTION, null);
                    }
                } else if (paramCondition instanceof DropTableParamCondition) {
                    queryEntity = transitionDropTableParam((DropTableParamCondition) paramCondition, database, table);
                    if (queryEntity == null) {
                        throw new BusinessException(ExceptionCode.PARAM_NULL_EXCEPTION, null);
                    }
                } else if (paramCondition instanceof EmptyTableParamCondition) {
                    queryEntity = transitionEmptyTableParam((EmptyTableParamCondition) paramCondition, database, table);
                    if (queryEntity == null) {
                        throw new BusinessException(ExceptionCode.PARAM_NULL_EXCEPTION, null);
                    }
                } else if (paramCondition instanceof UpsertParamCondition) {
                    queryEntity = transitionUpsertParam((UpsertParamCondition) paramCondition, database, table);
                    if (queryEntity == null) {
                        throw new BusinessException(ExceptionCode.PARAM_NULL_EXCEPTION, null);
                    }
                } else if (paramCondition instanceof UpsertParamBatchCondition) {
                    queryEntity = transitionUpsertParamBatch((UpsertParamBatchCondition) paramCondition, database, table);
                    if (queryEntity == null) {
                        throw new BusinessException(ExceptionCode.PARAM_NULL_EXCEPTION, null);
                    }
                } else if (paramCondition instanceof AlterTableParamCondition) {
                    queryEntity = transitionAlterTableParam((AlterTableParamCondition) paramCondition, database, table);
                    if (queryEntity == null) {
                        throw new BusinessException(ExceptionCode.PARAM_NULL_EXCEPTION, null);
                    }
                } else if (paramCondition instanceof QueryTablesCondition) {
                    queryEntity = transitionListTableParam((QueryTablesCondition) paramCondition, database);
                    if (queryEntity == null) {
                        throw new BusinessException(ExceptionCode.PARAM_NULL_EXCEPTION, null);
                    }
                }else if (paramCondition instanceof QueryDatabasesCondition) {
                    queryEntity = transitionListDatabaseParam((QueryDatabasesCondition) paramCondition, database);
                    if (queryEntity == null) {
                        throw new BusinessException(ExceptionCode.PARAM_NULL_EXCEPTION, null);
                    }
                }else if(paramCondition instanceof MergeDataParamCondition) {
                    queryEntity = transitionMergeDataParam((MergeDataParamCondition) paramCondition, database, table);
                    if (queryEntity == null) {
                        throw new BusinessException(ExceptionCode.PARAM_NULL_EXCEPTION, null);
                    }
                }else if(paramCondition instanceof CreateDatabaseParamCondition) {
                    queryEntity = transitionCreateDatabaseParam((CreateDatabaseParamCondition) paramCondition, database);
                    if (queryEntity == null) {
                        throw new BusinessException(ExceptionCode.PARAM_NULL_EXCEPTION, null);
                    }
                }else if(paramCondition instanceof UpdateDatabaseParamCondition) {
                    queryEntity = transitionUpdateDatabaseParam((UpdateDatabaseParamCondition) paramCondition, database);
                    if (queryEntity == null) {
                        throw new BusinessException(ExceptionCode.PARAM_NULL_EXCEPTION, null);
                    }
                }else if(paramCondition instanceof DropDatabaseParamCondition) {
                    queryEntity = transitionDropDatabaseParam((DropDatabaseParamCondition) paramCondition, database);
                    if (queryEntity == null) {
                        throw new BusinessException(ExceptionCode.PARAM_NULL_EXCEPTION, null);
                    }
                }else if(paramCondition instanceof PublishMessage) {
                    queryEntity = transitionPublishMessage((PublishMessage) paramCondition, database);
                    if (queryEntity == null) {
                        throw new BusinessException(ExceptionCode.PARAM_NULL_EXCEPTION, null);
                    }
                }else if(paramCondition instanceof SubscribeMessage) {
                    queryEntity = transitionSubscribeMessage((SubscribeMessage) paramCondition, database);
                    if (queryEntity == null) {
                        throw new BusinessException(ExceptionCode.PARAM_NULL_EXCEPTION, null);
                    }
                }
                return queryEntity;
            }
        });
    }

    /**
     * 查询统一处理方法
     *
     * @param databaseSetting
     * @param tableSetting
     * @param paramCondition
     * @param dbMethodEnum
     * @param executeCallback
     * @param parserCallback
     * @return
     * @throws Exception
     */
//    @Trace
    protected <P, R> R interiorExecute(DatabaseSetting databaseSetting, TableSetting tableSetting, Object paramCondition, DbMethodEnum dbMethodEnum, DbModuleCallback<S, Q, D, T, R> executeCallback, DbModuleParserCallback<P, Q, D, T> parserCallback) throws Exception {
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

            // 获取表信息，不为以下方法需要输入表信息
            if (!dbMethodEnum.equals(DbMethodEnum.TEST_CONNECTION)
                    && !dbMethodEnum.equals(DbMethodEnum.CHANGELOG)
                    && !dbMethodEnum.equals(DbMethodEnum.QUERY_LIST_TABLE)
                    && !dbMethodEnum.equals(DbMethodEnum.QUERY_DATABASE_SCHEMA)
                    && !dbMethodEnum.equals(DbMethodEnum.DATABASE_INFORMATION)
                    && !dbMethodEnum.equals(DbMethodEnum.QUERY_LIST_DATABASE)
                    && !dbMethodEnum.equals(DbMethodEnum.MONITOR_STATUS)
                    && !dbMethodEnum.equals(DbMethodEnum.CUSTOMIZE_OPERATION_ON_DATABASE)
                    && !dbMethodEnum.equals(DbMethodEnum.CREATE_DATABASE)
                    && !dbMethodEnum.equals(DbMethodEnum.UPDATE_DATABASE)
                    && !dbMethodEnum.equals(DbMethodEnum.DROP_DATABASE)) {
                long tableSettingStart = System.currentTimeMillis();
                if (tableSetting == null) {
                    throw new BusinessException(ExceptionCode.DB_INFO_NULL_EXCEPTION, null);
                }
                if (StringUtils.isNotBlank(tableSetting.getDbId())) {
                    tableConf = helper.getDbTableConfigCache(tableSetting.getDbId());
                } else {
                    TableConf tableInfo = new TableConf();
                    tableInfo.setTableJson(tableSetting.getTableJson());
                    tableInfo.setTableName(tableSetting.getTableName());
                    tableConf = helper.getTableConfig(tableInfo);
                }
                if (tableConf == null) {
                    throw new BusinessException(ExceptionCode.TABLE_NULL_EXCEPTION, null);
                }
                long tableSettingEnd = System.currentTimeMillis();
                tableSettingCost = tableSettingEnd - tableSettingStart;
            }
            // 参数转换
            if (paramCondition != null) {
                long paramConditionStart = System.currentTimeMillis();
                queryEntity = parserCallback.parser((P) paramCondition, dataConf, tableConf);
                long paramConditionEnd = System.currentTimeMillis();
                paramConditionCost = paramConditionEnd - paramConditionStart;
            }
            // 根据表信息才能创建数据库连接的（如：solr）则在符合以下条件情况下，创建连接
            // 特殊解释这段复杂的逻辑：
            // 因为存在一些组件，需要根据表信息才能构建一个可用的表相关查询操作，
            // 而在非操作表而是操作库的方法中，它只需要库信息就可以完成对应操作，
            // 因此我们在实现 S 连接对象时，是以根据表相关信息创建出来的对象，而在非操作表的方法中，就另外独自创建库连接
            DbService annotation = this.getClass().getAnnotation(DbService.class);
            if (!((dbMethodEnum.equals(DbMethodEnum.TEST_CONNECTION)
                    || dbMethodEnum.equals(DbMethodEnum.QUERY_LIST_TABLE)
                    || dbMethodEnum.equals(DbMethodEnum.QUERY_DATABASE_SCHEMA)
                    || dbMethodEnum.equals(DbMethodEnum.DATABASE_INFORMATION)
                    || dbMethodEnum.equals(DbMethodEnum.CREATE_TABLE)
                    || dbMethodEnum.equals(DbMethodEnum.DROP_TABLE)
                    || dbMethodEnum.equals(DbMethodEnum.MONITOR_STATUS))
                    && annotation.isTableConnect())) {
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
                    R queryMethodResult = executeCallback.doWithExecute(dbConnect, queryEntity, dataConf, tableConf);
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
                        log.debug("db query execute info dbType:[{}] - dbId:[{}] method:[{}] - param:[{}] - cost[{}]ms", annotation.dbType().name(), tableSetting == null ? null : tableSetting.getDbIdOrTableName(), dbMethodEnum.getMethod(), queryEntity != null ? queryEntity.getQueryStr() : null, allCost);
                    }
                }
            } else {
                // 在实现类中创建连接对象
                long start = System.currentTimeMillis();
                try {
                    // 防止线程复用，清除  ThreadLocal
                    DatabaseExecuteStatementLog.remove();
                    R queryMethodResult = executeCallback.doWithExecute(null, queryEntity, dataConf, tableConf);
                    return queryMethodResult;
                } finally {
                    long end = System.currentTimeMillis();
                    invokeDbCost = end - start;
                    allCost = end - allStart;
                    //记录原生sql
                    recordInvokeMsg(queryEntity, dataConf, tableConf, invokeDbCost, databaseCost, tableSettingCost, paramConditionCost, getDbConnectCost, allCost);
                    if (log.isDebugEnabled()) {
                        log.debug("db query execute info dbType:[{}] - dbId:[{}] method:[{}] - param:[{}] - cost[{}]ms", annotation.dbType().name(), tableSetting == null ? null : tableSetting.getDbIdOrTableName(), dbMethodEnum.getMethod(), queryEntity != null ? queryEntity.getQueryStr() : null, allCost);
                    }
                }
            }
        } else {
            throw new BusinessException(ExceptionCode.DB_INFO_NULL_EXCEPTION, null);
        }
    }

    /**
     * 转换库表查询接口参数
     *
     * @param paramCondition
     * @return
     */
    private Q transitionListTableParam(QueryTablesCondition paramCondition, D dataConf) throws Exception {
        return (Q) dbModuleParamUtil.getListTableParam(paramCondition, dataConf);
    }

    /**
     * 转换库查询接口参数
     *
     * @param paramCondition
     * @return
     */
    private Q transitionListDatabaseParam(QueryDatabasesCondition paramCondition, D dataConf) throws Exception {
        return (Q) dbModuleParamUtil.getListDatabaseParam(paramCondition, dataConf);
    }

    /**
     * 转换库查询接口参数
     *
     * @param paramCondition
     * @return
     */
    private Q transitionMergeDataParam(MergeDataParamCondition paramCondition, D dataConf, T tableConf) throws Exception {
        return (Q) dbModuleParamUtil.getMergeDataParam(paramCondition, dataConf, tableConf);
    }


    /**
     * 转换库查询接口参数
     *
     * @param paramCondition
     * @return
     */
    private Q transitionUpdateDatabaseParam(UpdateDatabaseParamCondition paramCondition, D dataConf) throws Exception {
        return (Q) dbModuleParamUtil.getUpdateDatabaseParam(paramCondition, dataConf);
    }

    /**
     * 转换库查询接口参数
     *
     * @param paramCondition
     * @return
     */
    private Q transitionDropDatabaseParam(DropDatabaseParamCondition paramCondition, D dataConf) throws Exception {
        return (Q) dbModuleParamUtil.getDropDatabaseParam(paramCondition, dataConf);
    }

    /**
     * 转换库查询接口参数
     *
     * @param paramCondition
     * @return
     */
    private Q transitionCreateDatabaseParam(CreateDatabaseParamCondition paramCondition, D dataConf) throws Exception {
        return (Q) dbModuleParamUtil.getCreateDatabaseParam(paramCondition, dataConf);
    }

    private Q transitionPublishMessage(PublishMessage paramCondition, D database) {
        return (Q) dbModuleParamUtil.getPublishMessage(paramCondition, database);
    }

    private Q transitionSubscribeMessage(SubscribeMessage paramCondition, D database) {
        return (Q) dbModuleParamUtil.getSubscribeMessage(paramCondition, database);
    }

    /**
     * 记录执行信息
     *
     * @param dataConf
     * @param tableConf
     * @param invokeDbCost
     * @param databaseCost
     * @param tableSettingCost
     * @param paramConditionCost
     * @param getDbConnectCost
     */
    protected void recordInvokeMsg(Q queryEntity, D dataConf, T tableConf, long invokeDbCost, long databaseCost, long tableSettingCost, long paramConditionCost, long getDbConnectCost, long allCost) {
        String statementLog = DatabaseExecuteStatementLog.get();
        DatabaseExecuteStatementLog.remove();
        if(statementLog.length() == 0 && queryEntity != null) {
           statementLog = queryEntity.getQueryStr();
        }
        ThreadLocalUtil.set(DbInvokeMsgConstant.QUERY_SQL_STR, statementLog);
        if (dataConf != null && StringUtils.isNotBlank(dataConf.getServerAddr())) {
            ThreadLocalUtil.set(DbInvokeMsgConstant.DATABASE_ADDR, dataConf.getServerAddr());
        }
        if (dataConf != null && StringUtils.isNotBlank(dataConf.getDbName())) {
            ThreadLocalUtil.set(DbInvokeMsgConstant.DATABASE_NAME, dataConf.getDbName());
        }
        if (tableConf != null && StringUtils.isNotBlank(tableConf.getTableName())) {
            ThreadLocalUtil.set(DbInvokeMsgConstant.TABLE_NAME, tableConf.getTableName());
        }
        ThreadLocalUtil.set(DbInvokeMsgConstant.QUERY_EXECUTE_COST, invokeDbCost);
        ThreadLocalUtil.set(DbInvokeMsgConstant.GET_DATABASE_COST, databaseCost);
        ThreadLocalUtil.set(DbInvokeMsgConstant.GET_TABLE_COST, tableSettingCost);
        ThreadLocalUtil.set(DbInvokeMsgConstant.GET_CONDITION_COST, paramConditionCost);
        ThreadLocalUtil.set(DbInvokeMsgConstant.GET_DB_CONNECT_COST, getDbConnectCost);
        ThreadLocalUtil.set(DbInvokeMsgConstant.LINK_COST, allCost);
    }

    /**
     * 基础通用回调方法
     *
     * @param <S>
     * @param <Q>
     * @param <D>
     * @param <T>
     */
    public interface BaseDbModuleCallback<S, Q extends AbstractDbHandler, D extends AbstractDatabaseInfo, T extends AbstractDbTableInfo> extends DbModuleCallback<S, Q, D, T, QueryMethodResult> {

    }

    /**
     * 回调方法
     *
     * @param <S>
     * @param <Q>
     * @param <D>
     * @param <T>
     */
    public interface DbModuleCallback<S, Q extends AbstractDbHandler, D extends AbstractDatabaseInfo, T extends AbstractDbTableInfo, R> {

        /**
         * 回调方法
         *
         * @param connect
         * @param queryEntity
         * @param databaseConf
         * @param tableConf
         * @return
         * @throws Exception
         */
        @Trace
        R doWithExecute(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception;
    }

    /**
     * 语法解析回调
     *
     * @param <P>
     * @param <Q>
     * @param <D>
     * @param <T>
     */
    public interface DbModuleParserCallback<P, Q extends AbstractDbHandler, D extends AbstractDatabaseInfo, T extends AbstractDbTableInfo> {
        /**
         * 解析函数
         *
         * @param params 待解析对象
         * @param database 数据库配置
         * @param table 表配置
         * @return
         * @throws
         */
        Q parser(P params, D database, T table) throws Exception;
    }

}
