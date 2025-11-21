package com.meiya.whalex.jdbc.parser;

import com.meiya.whalex.db.entity.DatabaseSetting;
import com.meiya.whalex.db.entity.PageResult;
import com.meiya.whalex.db.module.DbModuleService;
import com.meiya.whalex.interior.db.operation.in.DropDatabaseParamCondition;
import com.meiya.whalex.sql2dsl.parser.DropDatabaseSqlToDslParser;

/**
 * 展示表信息sql解析
 *
 * @author 蔡荣桂
 * @date 2022/07/07
 * @project whalex-dat-sql
 */
public class DropDatabaseSqlParser implements ISqlParser<Boolean> {

    private String sql;

    DbModuleService service;

    DatabaseSetting databaseSetting;

    public DropDatabaseSqlParser(String sql, DbModuleService service, DatabaseSetting databaseSetting) {
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
        DropDatabaseSqlToDslParser parser = new DropDatabaseSqlToDslParser(sql);
        DropDatabaseParamCondition condition = parser.handle();
        PageResult pageResult = service.dropDatabase(databaseSetting, condition);
        return pageResult;
    }

}
