package com.meiya.whalex.db.module;

import com.meiya.whalex.business.entity.DatabaseConf;
import com.meiya.whalex.db.entity.AbstractCursorCache;
import com.meiya.whalex.db.entity.AbstractDatabaseInfo;
import com.meiya.whalex.db.entity.AbstractDbHandler;
import com.meiya.whalex.db.entity.AbstractDbTableInfo;
import com.meiya.whalex.db.entity.DatabaseSetting;
import com.meiya.whalex.exception.BusinessException;
import com.meiya.whalex.exception.ExceptionCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.function.Function;

@Slf4j
public abstract class AbstractDbRawStatementModule<S,
        Q extends AbstractDbHandler,
        D extends AbstractDatabaseInfo,
        T extends AbstractDbTableInfo,
        C extends AbstractCursorCache> extends AbstractDbModuleBaseService<S, Q, D, T, C> implements DbRawStatementModule {


    /**
     * 获取库连接
     * @param databaseSetting
     * @return
     */
    protected D getDataConf(DatabaseSetting databaseSetting) {

        if (databaseSetting == null) {
            throw new BusinessException(ExceptionCode.DB_INFO_NULL_EXCEPTION, null);
        }

        D dataConf = null;
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
        return dataConf;
    }

    public interface Executor<S, D extends AbstractDatabaseInfo> {
        <R, P> R execute(D dataConf, S dbConnect, P params) throws Exception;
    }

    /**
     *
     * @param databaseSetting
     * @param paramHandler 返回值，是executor的入参P
     * @param executor 返回值，是函数的最终返回值
     * @param <R> executor的返回值
     * @param <P> paramHandler的返回值
     * @return
     * @throws Exception
     */
    protected  <R,P> R _execute(DatabaseSetting databaseSetting, Function<D, P> paramHandler, Executor<S, D> executor) throws Exception {

        if (databaseSetting == null) {
            throw new BusinessException(ExceptionCode.DB_INFO_NULL_EXCEPTION, null);
        }

        Long databaseCost = 0L;
        Long tableSettingCost = 0L;
        Long paramConditionCost = 0L;
        Long getDbConnectCost = 0L;
        Long invokeDbCost = 0L;
        Long allCost = 0L;

        long allStart = System.currentTimeMillis();
        long databaseStart = System.currentTimeMillis();
        //获取库连接信息
        D dataConf = getDataConf(databaseSetting);
        long databaseEnd = System.currentTimeMillis();
        databaseCost = databaseEnd - databaseStart;
        // 获取连接
        long getDbConnectStart = System.currentTimeMillis();
        S dbConnect = helper.getDbConnect(dataConf, null, databaseSetting.getBigDataResourceId(), null);
        if (dbConnect == null || dataConf == null) {
            throw new BusinessException(ExceptionCode.DATABASE_NULL_EXCEPTION, null);
        }
        long getDbConnectEnd = System.currentTimeMillis();
        getDbConnectCost = getDbConnectEnd - getDbConnectStart;

        // 解析语句
        long paramConditionStart = System.currentTimeMillis();
        P p = paramHandler.apply(dataConf);
        long paramConditionEnd = System.currentTimeMillis();
        paramConditionCost = paramConditionEnd - paramConditionStart;
        String cacheKey = helper.getCacheKey(dataConf, null);
        // 组件查询耗时记录
        long start = 0;
        long end = 0;
        try {
            // 防止线程复用，清除  ThreadLocal
            DatabaseExecuteStatementLog.remove();
            // 连接使用计数
            this.getHelper().getUseConnectCount().incrementAndGet(cacheKey);
            start = System.currentTimeMillis();
            // 调用执行sql
            R result = executor.execute(dataConf, dbConnect, p);
            end = System.currentTimeMillis();
            invokeDbCost = end - start;
            allCost = allStart - end;
            return result;

        } finally {
            if (start != 0 && end == 0) {
                end = System.currentTimeMillis();
            }

            if(log.isDebugEnabled()) {
                log.debug("执行语句: [{}] 耗时: [{}] ms", p, (end - start));
            }
            recordInvokeMsg(null, dataConf, null, invokeDbCost, databaseCost, tableSettingCost, paramConditionCost, getDbConnectCost, allCost);
            // 释放连接计数
            this.getHelper().getUseConnectCount().decrementAndGet(cacheKey);
        }
    }

}
