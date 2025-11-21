package com.meiya.whalex.jdbc.parser;

import com.meiya.whalex.db.builder.TableConfigurationBuilder;
import com.meiya.whalex.db.entity.DatabaseSetting;
import com.meiya.whalex.db.entity.DropTableParamCondition;
import com.meiya.whalex.db.entity.PageResult;
import com.meiya.whalex.db.entity.TableSetting;
import com.meiya.whalex.db.module.DbModuleService;
import com.meiya.whalex.sql2dsl.parser.DropTableSqlToDslParser;

/**
 * 表删除sql解析
 *
 * @author 蔡荣桂
 * @date 2022/06/27
 * @project whalex-dat-sql
 */
public class DropTableSqlParser implements ISqlParser<Boolean> {


    private String sql;

    DbModuleService service;

    DatabaseSetting databaseSetting;

    public DropTableSqlParser(String sql, DbModuleService service, DatabaseSetting databaseSetting) {
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
        DropTableSqlToDslParser parser = new DropTableSqlToDslParser(sql);
        DropTableParamCondition condition = parser.handle();
        String tableName = parser.getTableName();

        TableSetting tableSetting = TableConfigurationBuilder.builder().tableName(tableName).build();
        PageResult pageResult = service.dropTable(databaseSetting, tableSetting, condition);
        return pageResult;
    }

}
