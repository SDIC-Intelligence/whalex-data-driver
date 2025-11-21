package com.meiya.whalex.jdbc.parser;

import com.meiya.whalex.db.builder.TableConfigurationBuilder;
import com.meiya.whalex.db.entity.DatabaseSetting;
import com.meiya.whalex.db.entity.DelParamCondition;
import com.meiya.whalex.db.entity.PageResult;
import com.meiya.whalex.db.entity.TableSetting;
import com.meiya.whalex.db.module.DbModuleService;
import com.meiya.whalex.sql.module.RdbmsModuleService;
import com.meiya.whalex.sql2dsl.parser.DeleteSqlToDslParser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 删除数据sql解析
 *
 * @author 蔡荣桂
 * @date 2022/06/27
 * @project whalex-dat-sql
 */
public class DeleteSqlParser implements ISqlParser<Integer> {

    DbModuleService service;

    DatabaseSetting databaseSetting;

    private String sql;

    private Object[] params;

    public DeleteSqlParser(String sql, Object[] params, DbModuleService service, DatabaseSetting databaseSetting) {
        this.sql = sql;
        this.params = params;
        this.service = service;
        this.databaseSetting = databaseSetting;
    }


    @Override
    public Integer execute() throws Exception {
        PageResult pageResult = executeAndGetPageResult();
        return (int) pageResult.getTotal();
    }

    @Override
    public PageResult executeAndGetPageResult() throws Exception {
        // 使用复杂sql能力，不需要解析
        if(service instanceof RdbmsModuleService) {
            return executeBySql();
        }

        DeleteSqlToDslParser parser = new DeleteSqlToDslParser(sql, params);
        DelParamCondition condition = parser.handle();
        String tableName = parser.getTableName();

        TableSetting tableSetting = TableConfigurationBuilder.builder().tableName(tableName).build();
        PageResult pageResult = service.delete(databaseSetting, tableSetting, condition);
        return pageResult;
    }

    private PageResult executeBySql() throws Exception {
        RdbmsModuleService rdbmsModuleService = (RdbmsModuleService) service;

        List<Object> paramList = new ArrayList<>();
        if(params != null) {
            paramList.addAll(Arrays.asList(params));
        }

        PageResult pageResult = rdbmsModuleService.updateBySql(databaseSetting, sql, paramList);
        return pageResult;
    }

}
