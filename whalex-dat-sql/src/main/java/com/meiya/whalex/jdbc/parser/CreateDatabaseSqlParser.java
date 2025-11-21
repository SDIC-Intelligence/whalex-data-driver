package com.meiya.whalex.jdbc.parser;

import com.meiya.whalex.db.builder.TableConfigurationBuilder;
import com.meiya.whalex.db.entity.DatabaseSetting;
import com.meiya.whalex.db.entity.PageResult;
import com.meiya.whalex.db.entity.TableSetting;
import com.meiya.whalex.db.module.DbModuleService;
import com.meiya.whalex.interior.db.operation.in.AlterTableParamCondition;
import com.meiya.whalex.interior.db.operation.in.CreateDatabaseParamCondition;
import com.meiya.whalex.sql2dsl.parser.CreateDatabaseSqlToDslParser;

/**
 * 展示表信息sql解析
 *
 * @author 蔡荣桂
 * @date 2022/07/07
 * @project whalex-dat-sql
 */
public class CreateDatabaseSqlParser implements ISqlParser<Boolean> {

    private String sql;

    DbModuleService service;

    DatabaseSetting databaseSetting;

    public CreateDatabaseSqlParser(String sql, DbModuleService service, DatabaseSetting databaseSetting) {
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
        CreateDatabaseSqlToDslParser parser = new CreateDatabaseSqlToDslParser(sql);
        CreateDatabaseParamCondition condition = parser.handle();
        PageResult pageResult = service.createDatabase(databaseSetting, condition);
        return pageResult;
    }

}
