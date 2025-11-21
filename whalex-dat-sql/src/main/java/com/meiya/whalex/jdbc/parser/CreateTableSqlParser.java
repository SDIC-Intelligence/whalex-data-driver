package com.meiya.whalex.jdbc.parser;

import com.meiya.whalex.db.builder.TableConfigurationBuilder;
import com.meiya.whalex.db.entity.DatabaseSetting;
import com.meiya.whalex.db.entity.IndexParamCondition;
import com.meiya.whalex.db.entity.PageResult;
import com.meiya.whalex.db.entity.TableSetting;
import com.meiya.whalex.db.module.DbModuleService;
import com.meiya.whalex.db.module.DbTransactionModuleService;
import com.meiya.whalex.interior.db.operation.in.CreateTableParamCondition;
import com.meiya.whalex.sql2dsl.entity.SqlCreateTable;
import com.meiya.whalex.sql2dsl.parser.CreateTableSqlToDslParser;

import java.util.LinkedHashMap;
import java.util.List;

/**
 * 创建表sql解析
 *
 * @author 蔡荣桂
 * @date 2022/06/27
 * @project whalex-dat-sql
 */
public class CreateTableSqlParser implements ISqlParser<Boolean> {


    DbModuleService service;

    DatabaseSetting databaseSetting;

    private String sql;

    public CreateTableSqlParser(String sql, DbModuleService service, DatabaseSetting databaseSetting) {

        this.sql = sql;
        this.service = service;
        this.databaseSetting = databaseSetting;
    }



    @Override
    public Boolean execute() throws Exception {
        PageResult pageResult = executeAndGetPageResult();
        return pageResult.getSuccess();

    }

    @Override
    public PageResult executeAndGetPageResult() throws Exception {
        CreateTableSqlToDslParser parser = new CreateTableSqlToDslParser(sql);
        SqlCreateTable sqlCreateTable = parser.handle();
        // 建表对象
        CreateTableParamCondition condition = sqlCreateTable.getCreateTable();

        // 索引创建对象
        List<IndexParamCondition> createIndexes = sqlCreateTable.getCreateIndex();

        // 操作参数
        String tableName = parser.getTableName();
        LinkedHashMap properties = parser.getProperties();
        if(properties == null) {
            properties = new LinkedHashMap();
        }
        TableSetting tableSetting = TableConfigurationBuilder.builder().tableName(tableName).tableConfig(properties).build();
        boolean isCommit = true;
        PageResult pageResult;
        if (createIndexes == null) {
            pageResult = service.createTable(databaseSetting, tableSetting, condition);
        } else {
            DbTransactionModuleService dbTransactionModuleService = null;
            if(service instanceof DbTransactionModuleService) {
                pageResult = service.createTable(databaseSetting, tableSetting, condition);
                if (pageResult.getSuccess()) {
                    for (IndexParamCondition indexParamCondition : createIndexes) {
                        pageResult = service.createIndex(databaseSetting, tableSetting, indexParamCondition);
                        if (!pageResult.getSuccess()) {
                            break;
                        }
                    }
                }
            }else {
                try {
                    dbTransactionModuleService = service.newTransaction(databaseSetting);
                    pageResult = dbTransactionModuleService.createTable(databaseSetting, tableSetting, condition);
                    if (pageResult.getSuccess()) {
                        for (IndexParamCondition indexParamCondition : createIndexes) {
                            pageResult = dbTransactionModuleService.createIndex(databaseSetting, tableSetting, indexParamCondition);
                            if (!pageResult.getSuccess()) {
                                isCommit = false;
                                break;
                            }
                        }
                    } else {
                        isCommit = false;
                    }
                } catch (Exception e) {
                    isCommit = false;
                    throw e;
                } finally {
                    if (dbTransactionModuleService != null) {
                        if (isCommit) {
                            dbTransactionModuleService.commit();
                        } else {
                            dbTransactionModuleService.rollback();
                        }
                    }
                }
            }
        }

        return pageResult;
    }


}
