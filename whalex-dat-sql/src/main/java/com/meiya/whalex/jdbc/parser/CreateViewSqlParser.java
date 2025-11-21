package com.meiya.whalex.jdbc.parser;

import com.meiya.whalex.db.entity.DatabaseSetting;
import com.meiya.whalex.db.entity.PageResult;
import com.meiya.whalex.db.module.DbModuleService;
import com.meiya.whalex.sql.module.RdbmsModuleService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


/**
 * 新建视图sql解析
 *
 * @author 蔡荣桂
 * @date 2023/04/21
 * @project whalex-dat-sql
 */
public class CreateViewSqlParser implements ISqlParser<Boolean>{

    private String sql;

    private Object[] params;

    DbModuleService service;

    DatabaseSetting databaseSetting;

    public CreateViewSqlParser(String sql, Object[] params, DbModuleService service, DatabaseSetting databaseSetting) {
        this.sql = sql;
        this.params = params;
        this.service = service;
        this.databaseSetting = databaseSetting;
    }


    private PageResult _executeBySql() throws Exception {
        RdbmsModuleService rdbmsModuleService = (RdbmsModuleService) service;

        List<Object> paramList = new ArrayList<>();
        if(params != null) {
            paramList.addAll(Arrays.asList(params));
        }

        PageResult pageResult = rdbmsModuleService.updateBySql(databaseSetting, sql, paramList);
        return pageResult;
    }

    private Boolean executeBySql() throws Exception {
        PageResult pageResult = _executeBySql();
        return pageResult.getSuccess();
    }


    public Boolean execute() throws Exception {

        // 使用复杂sql能力，不需要解析
        if(service instanceof RdbmsModuleService) {
           return executeBySql();
        }
        throw new RuntimeException("新建视图，不支持非关型数据库");
    }

    @Override
    public PageResult executeAndGetPageResult() throws Exception {
        // 使用复杂sql能力，不需要解析
        if(service instanceof RdbmsModuleService) {
            return _executeBySql();
        }

        throw new RuntimeException("新建视图，不支持非关型数据库");
    }

}
