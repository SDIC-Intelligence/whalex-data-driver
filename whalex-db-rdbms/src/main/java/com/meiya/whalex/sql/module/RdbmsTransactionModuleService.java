package com.meiya.whalex.sql.module;

import com.meiya.whalex.db.constant.IsolationLevel;
import com.meiya.whalex.db.entity.CreateSequenceBean;
import com.meiya.whalex.db.entity.DatabaseSetting;
import com.meiya.whalex.db.entity.DropSequenceBean;
import com.meiya.whalex.db.entity.PageResult;
import com.meiya.whalex.db.module.DbModuleService;
import com.meiya.whalex.db.module.DbTransactionModuleService;

import java.util.List;

public class RdbmsTransactionModuleService extends DbTransactionModuleService implements RdbmsModuleService {

    private RdbmsModuleService rdbmsModuleService;

    public RdbmsTransactionModuleService(DatabaseSetting databaseSetting,
                                         DbModuleService service,
                                         String transactionId,
                                         IsolationLevel isolationLevel,
                                         String transactionIdKey,
                                         String transactionFirstKey,
                                         String isolationLevelKey) {
        super(databaseSetting, service, transactionId, isolationLevel, transactionIdKey, transactionFirstKey, isolationLevelKey);
        this.rdbmsModuleService = (RdbmsModuleService) service;
    }

    @Override
    public PageResult queryBySql(DatabaseSetting databaseSetting, String sql, List<Object> params) throws Exception {
        transactionHandler();
        try {
            PageResult pageResult = rdbmsModuleService.queryBySql(databaseSetting, sql, params);
            realOpenTransaction();
            return pageResult;
        }finally {
            clearTransactionHandler();
        }
    }

    @Override
    public PageResult updateBySql(DatabaseSetting databaseSetting, String sql, List<Object> params) throws Exception {
        transactionHandler();
        try {
            PageResult pageResult = rdbmsModuleService.updateBySql(databaseSetting, sql, params);
            realOpenTransaction();
            return pageResult;
        }finally {
            clearTransactionHandler();
        }
    }

    @Override
    public PageResult createSequence(DatabaseSetting databaseSetting, CreateSequenceBean createSequenceBean) throws Exception {
        transactionHandler();
        try {
            PageResult pageResult = rdbmsModuleService.createSequence(databaseSetting, createSequenceBean);
            realOpenTransaction();
            return pageResult;
        }finally {
            clearTransactionHandler();
        }
    }

    @Override
    public PageResult dropSequence(DatabaseSetting databaseSetting, DropSequenceBean dropSequenceBean) throws Exception {
        transactionHandler();
        try {
            PageResult pageResult = rdbmsModuleService.dropSequence(databaseSetting, dropSequenceBean);
            realOpenTransaction();
            return pageResult;
        }finally {
            clearTransactionHandler();
        }
    }
}
