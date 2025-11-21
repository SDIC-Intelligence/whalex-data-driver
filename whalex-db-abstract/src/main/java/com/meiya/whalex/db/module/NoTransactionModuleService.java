package com.meiya.whalex.db.module;

import com.meiya.whalex.db.constant.IsolationLevel;
import com.meiya.whalex.db.entity.DatabaseSetting;
import com.meiya.whalex.interior.db.constant.DbResourceEnum;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NoTransactionModuleService extends DbTransactionModuleService {

    public NoTransactionModuleService(DatabaseSetting databaseSetting, DbModuleService service, String transactionId, IsolationLevel isolationLevel, String transactionIdKey, String transactionFirstKey, String isolationLevelKey) {
        super(databaseSetting, service, transactionId, isolationLevel, transactionIdKey, transactionFirstKey, isolationLevelKey);
    }

    @Override
    public void rollback() throws Exception {
        if(first) {
            return;
        }
        transactionHandler();
        clearTransactionHandler();
        log.warn(DbResourceEnum.findDbResourceEnum(databaseSetting.getDbType()).name() + "不支持事务");
    }

    @Override
    public void rollback(DatabaseSetting databaseSetting) throws Exception {
        if(first) {
            return;
        }
        transactionHandler();
        clearTransactionHandler();
        log.warn(DbResourceEnum.findDbResourceEnum(databaseSetting.getDbType()).name() + "不支持事务");
    }

    @Override
    public void commit() throws Exception {
        if(first) {
            return;
        }
        transactionHandler();
        clearTransactionHandler();
        log.warn(DbResourceEnum.findDbResourceEnum(databaseSetting.getDbType()).name() + "不支持事务");
    }

    @Override
    public void commit(DatabaseSetting databaseSetting) throws Exception {
        if(first) {
            return;
        }
        transactionHandler();
        clearTransactionHandler();
        log.warn(DbResourceEnum.findDbResourceEnum(databaseSetting.getDbType()).name() + "不支持事务");
    }
}
