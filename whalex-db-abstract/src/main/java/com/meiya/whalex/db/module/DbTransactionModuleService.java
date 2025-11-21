package com.meiya.whalex.db.module;

import com.meiya.whalex.db.constant.IsolationLevel;
import com.meiya.whalex.db.constant.SupportPower;
import com.meiya.whalex.db.constant.TransactionConstant;
import com.meiya.whalex.db.entity.AddParamCondition;
import com.meiya.whalex.db.entity.DataResult;
import com.meiya.whalex.db.entity.DatabaseSetting;
import com.meiya.whalex.db.entity.DbHandleEntity;
import com.meiya.whalex.db.entity.DelParamCondition;
import com.meiya.whalex.db.entity.DropTableParamCondition;
import com.meiya.whalex.db.entity.EmptyTableParamCondition;
import com.meiya.whalex.db.entity.IndexParamCondition;
import com.meiya.whalex.db.entity.PageResult;
import com.meiya.whalex.db.entity.TableSetting;
import com.meiya.whalex.db.entity.UpdateParamCondition;
import com.meiya.whalex.db.entity.UpsertParamBatchCondition;
import com.meiya.whalex.db.entity.UpsertParamCondition;
import com.meiya.whalex.interior.db.constant.ReturnCodeEnum;
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
import com.meiya.whalex.util.concurrent.ThreadLocalUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 *
 * 组件事务代理对象
 *
 * @author 蔡荣桂
 * @date 2023/6/8
 * @project whalex-data-driver
 */
@Slf4j
public class DbTransactionModuleService implements DbModuleService, DbRawStatementModule{

    protected DatabaseSetting databaseSetting;

    private DbModuleService service;

    private String transactionId;

    private String transactionIdKey;

    private String transactionFirstKey;

    private String isolationLevelKey;

    protected boolean first;

    /**
     * 事务级别
     */
    private IsolationLevel isolationLevel;

    public DbTransactionModuleService(DatabaseSetting databaseSetting,
                                      DbModuleService service,
                                      String transactionId,
                                      IsolationLevel isolationLevel,
                                      String transactionIdKey,
                                      String transactionFirstKey,
                                      String isolationLevelKey) {
        this.databaseSetting = databaseSetting;
        this.service = service;
        this.transactionId = transactionId;
        this.transactionIdKey = transactionIdKey;
        this.transactionFirstKey = transactionFirstKey;
        this.isolationLevelKey = isolationLevelKey;
        this.isolationLevel = isolationLevel;
        this.first = true;
    }

    /**
     * 事务操作前设置当前线程变量
     */
    protected void transactionHandler() {
        if(first) {
            ThreadLocalUtil.set(transactionFirstKey, TransactionConstant.FIRST);
        }
        ThreadLocalUtil.set(transactionIdKey, transactionId);
        if (isolationLevel != null) {
            ThreadLocalUtil.set(isolationLevelKey, isolationLevel.value());
        }
    }

    /**
     * 标志已完成事务开启
     */
    protected void realOpenTransaction() {
        first = false;
    }

    /**
     * 事务操作结束之后清除当前线程变量
     */
    protected void clearTransactionHandler() {
        if(ThreadLocalUtil.get(transactionFirstKey) == null) {
            first = false;
        }
        ThreadLocalUtil.remove(transactionFirstKey);
        ThreadLocalUtil.remove(transactionIdKey);
        ThreadLocalUtil.remove(isolationLevelKey);
    }
    
    @Override
    public void openTransaction() {
        throw new RuntimeException("事务已开启");
    }

    @Override
    public void openTransaction(IsolationLevel isolationLevel) {
        openTransaction();
    }

    @Override
    public void openTransaction(String transactionId) {
        openTransaction();
    }

    @Override
    public void openTransaction(String transactionId, IsolationLevel isolationLevel) {
        openTransaction();
    }

    @Override
    public void openTransactionByDatabase(DatabaseSetting databaseName) {
        openTransaction();
    }

    @Override
    public void openTransactionByDatabase(DatabaseSetting databaseName, IsolationLevel isolationLevel) {
        openTransaction();
    }

    @Override
    public DbTransactionModuleService newTransaction(DatabaseSetting databaseSetting) {
        throw new RuntimeException("事务已开启");
    }

    @Override
    public DbTransactionModuleService newTransaction(DatabaseSetting databaseSetting, IsolationLevel isolationLevel) {
        return null;
    }

    @Override
    public void fetchTransaction(String transactionId) {
        openTransaction();
    }

    @Override
    public void rollback() throws Exception {
        if(first) {
            return;
        }
        transactionHandler();
        service.rollback(databaseSetting);
        clearTransactionHandler();
    }

    @Override
    public void rollback(DatabaseSetting databaseSetting) throws Exception {
        if(first) {
            return;
        }
        transactionHandler();
        service.rollback(databaseSetting);
        clearTransactionHandler();
    }

    @Override
    public void commit() throws Exception {
        if(first) {
            return;
        }
        transactionHandler();
        service.commit(databaseSetting);
        clearTransactionHandler();
    }

    @Override
    public void commit(DatabaseSetting databaseSetting) throws Exception {
        if(first) {
            return;
        }
        transactionHandler();
        service.commit(databaseSetting);
        clearTransactionHandler();
    }

    @Override
    public List<SupportPower> getSupportList() {
        return service.getSupportList();
    }

    @Override
    public PageResult supportList() {
        return service.supportList();
    }

    @Override
    public PageResult queryOne(DatabaseSetting databaseSetting, TableSetting tableSetting, QueryParamCondition queryParamCondition) throws Exception {
        transactionHandler();
        try {
            PageResult pageResult = service.queryOne(databaseSetting, tableSetting, queryParamCondition);
            realOpenTransaction();
            return pageResult;
        }finally {
            clearTransactionHandler();
        }
    }

    @Override
    public PageResult queryList(DatabaseSetting databaseSetting, TableSetting tableSetting, QueryParamCondition queryParamCondition) throws Exception {
        transactionHandler();
        try {
            PageResult pageResult = service.queryList(databaseSetting, tableSetting, queryParamCondition);
            realOpenTransaction();
            return pageResult;
        }finally {
            clearTransactionHandler();
        }
    }

    @Override
    public PageResult queryListForPage(DatabaseSetting databaseSetting, TableSetting tableSetting, QueryParamCondition queryParamCondition) throws Exception {
        transactionHandler();
        try {
            PageResult pageResult = service.queryListForPage(databaseSetting, tableSetting, queryParamCondition);
            realOpenTransaction();
            return pageResult;
        }finally {
            clearTransactionHandler();
        }
    }

    @Override
    public PageResult statisticalLine(DatabaseSetting databaseSetting, TableSetting tableSetting, QueryParamCondition queryParamCondition) throws Exception {
        transactionHandler();
        try {
            PageResult pageResult = service.statisticalLine(databaseSetting, tableSetting, queryParamCondition);
            realOpenTransaction();
            return pageResult;
        }finally {
            clearTransactionHandler();
        }
    }

    @Override
    public PageResult insert(DatabaseSetting databaseSetting, TableSetting tableSetting, AddParamCondition addParamCondition) throws Exception {
        transactionHandler();
        try {
            PageResult pageResult = service.insert(databaseSetting, tableSetting, addParamCondition);
            realOpenTransaction();
            return pageResult;
        }finally {
            clearTransactionHandler();
        }
    }

    @Override
    public PageResult insertBatch(DatabaseSetting databaseSetting, TableSetting tableSetting, AddParamCondition addParamCondition) throws Exception {
        transactionHandler();
        try {
            PageResult pageResult = service.insertBatch(databaseSetting, tableSetting, addParamCondition);
            realOpenTransaction();
            return pageResult;
        }finally {
            clearTransactionHandler();
        }
    }

    @Override
    public PageResult update(DatabaseSetting databaseSetting, TableSetting tableSetting, UpdateParamCondition updateParamCondition) throws Exception {
        transactionHandler();
        try {
            PageResult pageResult = service.update(databaseSetting, tableSetting, updateParamCondition);
            realOpenTransaction();
            return pageResult;
        }finally {
            clearTransactionHandler();
        }
    }

    @Override
    public PageResult delete(DatabaseSetting databaseSetting, TableSetting tableSetting, DelParamCondition delParamCondition) throws Exception {
        transactionHandler();
        try {
            PageResult pageResult = service.delete(databaseSetting, tableSetting, delParamCondition);
            realOpenTransaction();
            return pageResult;
        }finally {
            clearTransactionHandler();
        }
    }

    @Override
    public PageResult getIndexes(DatabaseSetting databaseSetting, TableSetting tableSetting) throws Exception {
        transactionHandler();
        try {
            PageResult pageResult = service.getIndexes(databaseSetting, tableSetting);
            realOpenTransaction();
            return pageResult;
        }finally {
            clearTransactionHandler();
        }
    }

    @Override
    public PageResult createIndex(DatabaseSetting databaseSetting, TableSetting tableSetting, IndexParamCondition indexParamCondition) throws Exception {
        transactionHandler();
        try {
            PageResult pageResult = service.createIndex(databaseSetting, tableSetting, indexParamCondition);
            realOpenTransaction();
            return pageResult;
        }finally {
            clearTransactionHandler();
        }
    }

    @Override
    public PageResult testConnection(DatabaseSetting databaseSetting) throws Exception {
        return service.testConnection(databaseSetting);
    }

    @Override
    public PageResult changelog(DatabaseSetting databaseSetting) throws Exception {
        return service.changelog(databaseSetting);
    }

    @Override
    public PageResult queryListTable(DatabaseSetting databaseSetting, QueryTablesCondition queryTablesCondition) throws Exception {
        transactionHandler();
        try {
            PageResult pageResult = service.queryListTable(databaseSetting, queryTablesCondition);
            realOpenTransaction();
            return pageResult;
        }finally {
            clearTransactionHandler();
        }
    }

    @Override
    public PageResult queryListTable(DatabaseSetting databaseSetting) throws Exception {
        transactionHandler();
        try {
            PageResult pageResult = service.queryListTable(databaseSetting);
            realOpenTransaction();
            return pageResult;
        }finally {
            clearTransactionHandler();
        }
    }

    @Override
    public PageResult createTable(DatabaseSetting databaseSetting, TableSetting tableSetting, CreateTableParamCondition createTableParamCondition) throws Exception {
        transactionHandler();
        try {
            PageResult pageResult = service.createTable(databaseSetting, tableSetting, createTableParamCondition);
            realOpenTransaction();
            return pageResult;
        }finally {
            clearTransactionHandler();
        }
    }

    @Override
    public PageResult dropTable(DatabaseSetting databaseSetting, TableSetting tableSetting, DropTableParamCondition dropTableParamCondition) throws Exception {
        transactionHandler();
        try {
            PageResult pageResult = service.dropTable(databaseSetting, tableSetting, dropTableParamCondition);
            realOpenTransaction();
            return pageResult;
        }finally {
            clearTransactionHandler();
        }
    }

    @Override
    public PageResult emptyTable(DatabaseSetting databaseSetting, TableSetting tableSetting, EmptyTableParamCondition emptyTableParamCondition) throws Exception {
        transactionHandler();
        try {
            PageResult pageResult = service.emptyTable(databaseSetting, tableSetting, emptyTableParamCondition);
            realOpenTransaction();
            return pageResult;
        }finally {
            clearTransactionHandler();
        }
    }

    @Override
    public PageResult deleteIndex(DatabaseSetting databaseSetting, TableSetting tableSetting, IndexParamCondition indexParamCondition) throws Exception {
        transactionHandler();
        try {
            PageResult pageResult = service.deleteIndex(databaseSetting, tableSetting, indexParamCondition);
            realOpenTransaction();
            return pageResult;
        }finally {
            clearTransactionHandler();
        }
    }

    @Override
    public PageResult monitorStatus(DatabaseSetting databaseSetting) throws Exception {
        transactionHandler();
        try {
            PageResult pageResult = service.monitorStatus(databaseSetting);
            realOpenTransaction();
            return pageResult;
        }finally {
            clearTransactionHandler();
        }
    }

    @Override
    public PageResult queryTableSchema(DatabaseSetting databaseSetting, TableSetting tableSetting) throws Exception {
        transactionHandler();
        try {
            PageResult pageResult = service.queryTableSchema(databaseSetting, tableSetting);
            realOpenTransaction();
            return pageResult;
        }finally {
            clearTransactionHandler();
        }
    }

    @Override
    public PageResult queryCursor(DatabaseSetting databaseSetting, TableSetting tableSetting, QueryParamCondition queryParamCondition, Consumer<Map<String, Object>> consumer) throws Exception {
        return service.queryCursor(databaseSetting, tableSetting, queryParamCondition, consumer);
    }

    @Override
    public PageResult saveOrUpdate(DatabaseSetting databaseSetting, TableSetting tableSetting, UpsertParamCondition upsertParamCondition) throws Exception {
        transactionHandler();
        try {
            PageResult pageResult = service.saveOrUpdate(databaseSetting, tableSetting, upsertParamCondition);
            realOpenTransaction();
            return pageResult;
        }finally {
            clearTransactionHandler();
        }
    }

    @Override
    public PageResult saveOrUpdateBatch(DatabaseSetting databaseSetting, TableSetting tableSetting, UpsertParamBatchCondition upsertParamBatchCondition) throws Exception {
        transactionHandler();
        try {
            PageResult pageResult = service.saveOrUpdateBatch(databaseSetting, tableSetting, upsertParamBatchCondition);
            realOpenTransaction();
            return pageResult;
        }finally {
            clearTransactionHandler();
        }
    }

    @Override
    public PageResult queryDatabaseSchema(DatabaseSetting databaseSetting) throws Exception {
        return service.queryDatabaseSchema(databaseSetting);
    }

    @Override
    public PageResult alterTable(DatabaseSetting databaseSetting, TableSetting tableSetting, AlterTableParamCondition alterTableParamCondition) throws Exception {
        transactionHandler();
        try {
            PageResult pageResult = service.alterTable(databaseSetting, tableSetting, alterTableParamCondition);
            realOpenTransaction();
            return pageResult;
        }finally {
            clearTransactionHandler();
        }
    }

    @Override
    public PageResult tableExists(DatabaseSetting databaseSetting, TableSetting tableSetting) throws Exception {
        transactionHandler();
        try {
            PageResult pageResult = service.tableExists(databaseSetting, tableSetting);
            realOpenTransaction();
            return pageResult;
        }finally {
            clearTransactionHandler();
        }
    }

    @Override
    public PageResult queryTableInformation(DatabaseSetting databaseSetting, TableSetting tableSetting) throws Exception {
        transactionHandler();
        try {
            PageResult pageResult = service.queryTableInformation(databaseSetting, tableSetting);
            realOpenTransaction();
            return pageResult;
        }finally {
            clearTransactionHandler();
        }
    }

    @Override
    public PageResult queryListDatabase(DatabaseSetting databaseSetting, QueryDatabasesCondition queryDatabasesCondition) throws Exception {
        transactionHandler();
        try {
            PageResult pageResult = service.queryListDatabase(databaseSetting, queryDatabasesCondition);
            realOpenTransaction();
            return pageResult;
        }finally {
            clearTransactionHandler();
        }
    }

    @Override
    public PageResult queryDatabaseInformation(DatabaseSetting databaseSetting) throws Exception {
        transactionHandler();
        try {
            PageResult pageResult = service.queryDatabaseInformation(databaseSetting);
            realOpenTransaction();
            return pageResult;
        }finally {
            clearTransactionHandler();
        }
    }

    @Override
    public PageResult queryListDatabase(DatabaseSetting databaseSetting) throws Exception {
        transactionHandler();
        try {
            PageResult pageResult = service.queryListDatabase(databaseSetting);
            realOpenTransaction();
            return pageResult;
        }finally {
            clearTransactionHandler();
        }
    }

    @Override
    public PageResult mergeData(DatabaseSetting databaseSetting, TableSetting tableSetting, MergeDataParamCondition mergeDataParamCondition) throws Exception {
        transactionHandler();
        try {
            PageResult pageResult = service.mergeData(databaseSetting, tableSetting, mergeDataParamCondition);
            realOpenTransaction();
            return pageResult;
        }finally {
            clearTransactionHandler();
        }
    }

    @Override
    public PageResult createDatabase(DatabaseSetting databaseSetting, CreateDatabaseParamCondition createDatabaseParamCondition) throws Exception {
        transactionHandler();
        try {
            PageResult pageResult = service.createDatabase(databaseSetting, createDatabaseParamCondition);
            realOpenTransaction();
            return pageResult;
        }finally {
            clearTransactionHandler();
        }
    }

    @Override
    public PageResult updateDatabase(DatabaseSetting databaseSetting, UpdateDatabaseParamCondition updateDatabaseParamCondition) throws Exception {
        transactionHandler();
        try {
            PageResult pageResult = service.updateDatabase(databaseSetting, updateDatabaseParamCondition);
            realOpenTransaction();
            return pageResult;
        }finally {
            clearTransactionHandler();
        }
    }

    @Override
    public PageResult dropDatabase(DatabaseSetting databaseSetting, DropDatabaseParamCondition dropDatabaseParamCondition) throws Exception {
        transactionHandler();
        try {
            PageResult pageResult = service.dropDatabase(databaseSetting, dropDatabaseParamCondition);
            realOpenTransaction();
            return pageResult;
        }finally {
            clearTransactionHandler();
        }
    }

    @Override
    public PageResult queryPartitionInformation(DatabaseSetting databaseSetting, TableSetting tableSetting) throws Exception {
        transactionHandler();
        try {
            PageResult pageResult = service.queryPartitionInformation(databaseSetting, tableSetting);
            realOpenTransaction();
            return pageResult;
        }finally {
            clearTransactionHandler();
        }
    }

    @Override
    public PageResult queryPartitionInformationV2(DatabaseSetting databaseSetting, TableSetting tableSetting) throws Exception {
        transactionHandler();
        try {
            PageResult pageResult = service.queryPartitionInformationV2(databaseSetting, tableSetting);
            realOpenTransaction();
            return pageResult;
        }finally {
            clearTransactionHandler();
        }
    }

    @Override
    public void publish(DatabaseSetting databaseSetting, TableSetting tableSetting, PublishMessage message) throws Exception {
        transactionHandler();
        try {
            service.publish(databaseSetting, tableSetting, message);
            realOpenTransaction();
        }finally {
            clearTransactionHandler();
        }
    }

    @Override
    public void subscribe(DatabaseSetting databaseSetting, TableSetting tableSetting, SubscribeMessage subscribeMessage) throws Exception {
        transactionHandler();
        try {
            service.subscribe(databaseSetting, tableSetting, subscribeMessage);
            realOpenTransaction();
        }finally {
            clearTransactionHandler();
        }
    }

    @Override
    public PageResult queryOne(DbHandleEntity dbHandleEntity, QueryParamCondition queryParamCondition) throws Exception {
        transactionHandler();
        try {
            PageResult pageResult = service.queryOne(dbHandleEntity, queryParamCondition);
            realOpenTransaction();
            return pageResult;
        }finally {
            clearTransactionHandler();
        }
    }

    @Override
    public PageResult queryList(DbHandleEntity dbHandleEntity, QueryParamCondition queryParamCondition) throws Exception {
        transactionHandler();
        try {
            PageResult pageResult = service.queryList(dbHandleEntity, queryParamCondition);
            realOpenTransaction();
            return pageResult;
        }finally {
            clearTransactionHandler();
        }
    }

    @Override
    public PageResult queryListForPage(DbHandleEntity dbHandleEntity, QueryParamCondition queryParamCondition) throws Exception {
        transactionHandler();
        try {
            PageResult pageResult = service.queryListForPage(dbHandleEntity, queryParamCondition);
            realOpenTransaction();
            return pageResult;
        }finally {
            clearTransactionHandler();
        }
    }

    @Override
    public PageResult statisticalLine(DbHandleEntity dbHandleEntity, QueryParamCondition queryParamCondition) throws Exception {
        transactionHandler();
        try {
            PageResult pageResult = service.statisticalLine(dbHandleEntity, queryParamCondition);
            realOpenTransaction();
            return pageResult;
        }finally {
            clearTransactionHandler();
        }
    }

    @Override
    public PageResult insert(DbHandleEntity dbHandleEntity, AddParamCondition addParamCondition) throws Exception {
        transactionHandler();
        try {
            PageResult pageResult = service.insert(dbHandleEntity, addParamCondition);
            realOpenTransaction();
            return pageResult;
        }finally {
            clearTransactionHandler();
        }
    }

    @Override
    public PageResult insertBatch(DbHandleEntity dbHandleEntity, AddParamCondition addParamCondition) throws Exception {
        transactionHandler();
        try {
            PageResult pageResult = service.insertBatch(dbHandleEntity, addParamCondition);
            realOpenTransaction();
            return pageResult;
        }finally {
            clearTransactionHandler();
        }
    }

    @Override
    public PageResult update(DbHandleEntity dbHandleEntity, UpdateParamCondition updateParamCondition) throws Exception {
        transactionHandler();
        try {
            PageResult pageResult = service.update(dbHandleEntity, updateParamCondition);
            realOpenTransaction();
            return pageResult;
        }finally {
            clearTransactionHandler();
        }
    }

    @Override
    public PageResult delete(DbHandleEntity dbHandleEntity, DelParamCondition delParamCondition) throws Exception {
        transactionHandler();
        try {
            PageResult pageResult = service.delete(dbHandleEntity, delParamCondition);
            realOpenTransaction();
            return pageResult;
        } finally {
            clearTransactionHandler();
        }
    }

    @Override
    public PageResult getIndexes(DbHandleEntity dbHandleEntity) throws Exception {
        transactionHandler();
        try {
            PageResult pageResult = service.getIndexes(dbHandleEntity);
            realOpenTransaction();
            return pageResult;
        }finally {
            clearTransactionHandler();
        }
    }

    @Override
    public PageResult createIndex(DbHandleEntity dbHandleEntity, IndexParamCondition indexParamCondition) throws Exception {
        transactionHandler();
        try {
            PageResult pageResult = service.createIndex(dbHandleEntity, indexParamCondition);
            realOpenTransaction();
            return pageResult;
        }finally {
            clearTransactionHandler();
        }
    }

    @Override
    public PageResult testConnection(DbHandleEntity dbHandleEntity) throws Exception {
        return service.testConnection(dbHandleEntity);
    }

    @Override
    public PageResult changelog(DbHandleEntity dbHandleEntity) throws Exception {
        return service.changelog(dbHandleEntity);
    }

    @Override
    public PageResult queryListTable(DbHandleEntity dbHandleEntity, QueryTablesCondition queryTablesCondition) throws Exception {
        transactionHandler();
        try {
            PageResult pageResult = service.queryListTable(dbHandleEntity, queryTablesCondition);
            realOpenTransaction();
            return pageResult;
        }finally {
            clearTransactionHandler();
        }
    }

    @Override
    public PageResult queryListDatabase(DbHandleEntity dbHandleEntity, QueryDatabasesCondition queryDatabasesCondition) throws Exception {
        transactionHandler();
        try {
            PageResult pageResult = service.queryListDatabase(dbHandleEntity, queryDatabasesCondition);
            realOpenTransaction();
            return pageResult;
        }finally {
            clearTransactionHandler();
        }
    }

    @Override
    public PageResult queryListTable(DbHandleEntity dbHandleEntity) throws Exception {
        transactionHandler();
        try {
            PageResult pageResult = service.queryListTable(dbHandleEntity);
            realOpenTransaction();
            return pageResult;
        }finally {
            clearTransactionHandler();
        }
    }

    @Override
    public PageResult createTable(DbHandleEntity dbHandleEntity, CreateTableParamCondition createTableParamCondition) throws Exception {
        transactionHandler();
        try {
            PageResult pageResult = service.createTable(dbHandleEntity, createTableParamCondition);
            realOpenTransaction();
            return pageResult;
        }finally {
            clearTransactionHandler();
        }
    }

    @Override
    public PageResult dropTable(DbHandleEntity dbHandleEntity, DropTableParamCondition dropTableParamCondition) throws Exception {
        transactionHandler();
        try {
            PageResult pageResult = service.dropTable(dbHandleEntity, dropTableParamCondition);
            realOpenTransaction();
            return pageResult;
        }finally {
            clearTransactionHandler();
        }
    }

    @Override
    public PageResult emptyTable(DbHandleEntity dbHandleEntity, EmptyTableParamCondition emptyTableParamCondition) throws Exception {
        transactionHandler();
        try {
            PageResult pageResult = service.emptyTable(dbHandleEntity, emptyTableParamCondition);
            realOpenTransaction();
            return pageResult;
        }finally {
            clearTransactionHandler();
        }
    }

    @Override
    public PageResult deleteIndex(DbHandleEntity dbHandleEntity, IndexParamCondition indexParamCondition) throws Exception {
        transactionHandler();
        try {
            PageResult pageResult = service.deleteIndex(dbHandleEntity, indexParamCondition);
            realOpenTransaction();
            return pageResult;
        }finally {
            clearTransactionHandler();
        }
    }

    @Override
    public PageResult monitorStatus(DbHandleEntity dbHandleEntity) throws Exception {
        transactionHandler();
        try {
            PageResult pageResult = service.monitorStatus(dbHandleEntity);
            realOpenTransaction();
            return pageResult;
        }finally {
            clearTransactionHandler();
        }
    }

    @Override
    public PageResult queryTableSchema(DbHandleEntity dbHandleEntity) throws Exception {
        transactionHandler();
        try {
            PageResult pageResult = service.queryTableSchema(dbHandleEntity);
            realOpenTransaction();
            return pageResult;
        }finally {
            clearTransactionHandler();
        }
    }

    @Override
    public PageResult queryCursor(DbHandleEntity dbHandleEntity, QueryParamCondition queryParamCondition, Consumer<Map<String, Object>> consumer) throws Exception {
        return service.queryCursor(dbHandleEntity, queryParamCondition, consumer);
    }

    @Override
    public PageResult saveOrUpdate(DbHandleEntity dbHandleEntity, UpsertParamCondition upsertParamCondition) throws Exception {
        transactionHandler();
        try {
            PageResult pageResult = service.saveOrUpdate(dbHandleEntity, upsertParamCondition);
            realOpenTransaction();
            return pageResult;
        }finally {
            clearTransactionHandler();
        }
    }

    @Override
    public PageResult queryDatabaseSchema(DbHandleEntity dbHandleEntity) throws Exception {
        transactionHandler();
        try {
            PageResult pageResult = service.queryDatabaseSchema(dbHandleEntity);
            realOpenTransaction();
            return pageResult;
        }finally {
            clearTransactionHandler();
        }
    }

    @Override
    public PageResult alterTable(DbHandleEntity dbHandleEntity, AlterTableParamCondition alterTableParamCondition) throws Exception {
        transactionHandler();
        try {
            PageResult pageResult = service.alterTable(dbHandleEntity, alterTableParamCondition);
            realOpenTransaction();
            return pageResult;
        }finally {
            clearTransactionHandler();
        }
    }

    @Override
    public PageResult tableExists(DbHandleEntity dbHandleEntity) throws Exception {
        transactionHandler();
        try {
            PageResult pageResult = service.tableExists(dbHandleEntity);
            realOpenTransaction();
            return pageResult;
        }finally {
            clearTransactionHandler();
        }
    }

    @Override
    public PageResult queryTableInformation(DbHandleEntity dbHandleEntity) throws Exception {
        transactionHandler();
        try {
            PageResult pageResult = service.queryTableInformation(dbHandleEntity);
            realOpenTransaction();
            return pageResult;
        }finally {
            clearTransactionHandler();
        }
    }

    @Override
    public PageResult queryDatabaseInformation(DbHandleEntity dbHandleEntity) throws Exception {
        transactionHandler();
        try {
            PageResult pageResult = service.queryDatabaseInformation(dbHandleEntity);
            realOpenTransaction();
            return pageResult;
        }finally {
            clearTransactionHandler();
        }
    }

    @Override
    public PageResult mergeData(DbHandleEntity dbHandleEntity, MergeDataParamCondition mergeDataParamCondition) throws Exception {
        transactionHandler();
        try {
            PageResult pageResult = service.mergeData(dbHandleEntity, mergeDataParamCondition);
            realOpenTransaction();
            return pageResult;
        }finally {
            clearTransactionHandler();
        }
    }

    @Override
    public PageResult createDatabase(DbHandleEntity dbHandleEntity, CreateDatabaseParamCondition createDatabaseParamCondition) throws Exception {
        transactionHandler();
        try {
            PageResult pageResult = service.createDatabase(dbHandleEntity, createDatabaseParamCondition);
            realOpenTransaction();
            return pageResult;
        }finally {
            clearTransactionHandler();
        }
    }

    @Override
    public PageResult updateDatabase(DbHandleEntity dbHandleEntity, UpdateDatabaseParamCondition updateDatabaseParamCondition) throws Exception {
        transactionHandler();
        try {
            PageResult pageResult = service.updateDatabase(dbHandleEntity, updateDatabaseParamCondition);
            realOpenTransaction();
            return pageResult;
        }finally {
            clearTransactionHandler();
        }
    }

    @Override
    public PageResult dropDatabase(DbHandleEntity dbHandleEntity, DropDatabaseParamCondition dropDatabaseParamCondition) throws Exception {
        transactionHandler();
        try {
            PageResult pageResult = service.dropDatabase(dbHandleEntity, dropDatabaseParamCondition);
            realOpenTransaction();
            return pageResult;
        }finally {
            clearTransactionHandler();
        }
    }

    @Override
    public PageResult queryPartitionInformation(DbHandleEntity dbHandleEntity) throws Exception {
        transactionHandler();
        try {
            PageResult pageResult = service.queryPartitionInformation(dbHandleEntity);
            realOpenTransaction();
            return pageResult;
        }finally {
            clearTransactionHandler();
        }
    }

    @Override
    public PageResult queryPartitionInformationV2(DbHandleEntity dbHandleEntity) throws Exception {
        transactionHandler();
        try {
            PageResult pageResult = service.queryPartitionInformationV2(dbHandleEntity);
            realOpenTransaction();
            return pageResult;
        }finally {
            clearTransactionHandler();
        }
    }

    @Override
    public DataResult rawStatementExecute(DatabaseSetting databaseSetting, String rawStatement) throws Exception {
        transactionHandler();
        try {
            if(service instanceof DbRawStatementModule) {
                DbRawStatementModule statementModule = (DbRawStatementModule) service;
                realOpenTransaction();
                return statementModule.rawStatementExecute(databaseSetting, rawStatement);
            }
            DataResult<Object> dataResult = new DataResult<>();
            dataResult.setCode(ReturnCodeEnum.CODE_NOT_FOUND_METHOD.getCode());
            dataResult.setSuccess(false);
            dataResult.setMessage(String.format(ReturnCodeEnum.CODE_NOT_FOUND_METHOD.getMsg(), this.getClass().getSimpleName(), "rawStatementExecute"));
            return dataResult;
        }finally {
            clearTransactionHandler();
        }
    }

//    @Override
//    public StreamIterator queryByRawStatement(DatabaseSetting databaseSetting, String rawStatement) throws Exception {
//        transactionHandler();
//        try {
//            if(service instanceof DbRawStatementModule) {
//                DbRawStatementModule statementModule = (DbRawStatementModule) service;
//                realOpenTransaction();
//                return statementModule.queryByRawStatement(databaseSetting, rawStatement);
//            }
//            throw new BusinessException(ReturnCodeEnum.CODE_NOT_FOUND_METHOD.getCode(),
//                    String.format(ReturnCodeEnum.CODE_NOT_FOUND_METHOD.getMsg(), this.getClass().getSimpleName(), "queryByRawStatement"));
//
//        }finally {
//            clearTransactionHandler();
//        }
//    }
}
