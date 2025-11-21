package com.meiya.whalex.jdbc.parser;

import com.meiya.whalex.db.builder.TableConfigurationBuilder;
import com.meiya.whalex.db.entity.DatabaseSetting;
import com.meiya.whalex.db.entity.IndexParamCondition;
import com.meiya.whalex.db.entity.PageResult;
import com.meiya.whalex.db.entity.TableSetting;
import com.meiya.whalex.db.module.DbModuleService;
import com.meiya.whalex.interior.db.operation.in.AlterTableParamCondition;
import com.meiya.whalex.sql2dsl.parser.AlterTableSqlToDslParser;

/**
 * 表修改sql解析
 *
 * @author 蔡荣桂
 * @date 2022/06/27
 * @project whalex-dat-sql
 */
public class AlterTableSqlParser implements ISqlParser<Boolean> {

    private String sql;

    DbModuleService service;

    DatabaseSetting databaseSetting;

    public AlterTableSqlParser(String sql, DbModuleService service, DatabaseSetting databaseSetting) {
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
        AlterTableSqlToDslParser parser = new AlterTableSqlToDslParser(sql);
        Object condition = parser.handle();
        String tableName = parser.getTableName();

        TableSetting tableSetting = TableConfigurationBuilder.builder().tableName(tableName).build();


        if(condition instanceof AlterTableParamCondition) {
            //修改表
            PageResult pageResult = service.alterTable(databaseSetting, tableSetting, (AlterTableParamCondition) condition);
            return pageResult;

        }else {
            //新增索引
            return service.createIndex(databaseSetting, tableSetting, (IndexParamCondition) condition);
        }


    }

}
