package com.meiya.whalex.jdbc.parser;

import cn.hutool.core.collection.CollectionUtil;
import com.meiya.whalex.db.builder.TableConfigurationBuilder;
import com.meiya.whalex.db.entity.DatabaseSetting;
import com.meiya.whalex.db.entity.PageResult;
import com.meiya.whalex.db.entity.TableSetting;
import com.meiya.whalex.db.module.DbModuleService;
import com.meiya.whalex.jdbc.DatResultSet;
import com.meiya.whalex.sql2dsl.parser.DescTableSqlToDslParser;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 展示表信息sql解析
 *
 * @author 蔡荣桂
 * @date 2022/07/07
 * @project whalex-dat-sql
 */
public class DescTableSqlParser implements ISqlParser<DatResultSet> {

    private String sql;

    DbModuleService service;

    DatabaseSetting databaseSetting;

    public DescTableSqlParser(String sql, DbModuleService service, DatabaseSetting databaseSetting) {
        this.sql = sql;
        this.service = service;
        this.databaseSetting = databaseSetting;
    }


    @Override
    public DatResultSet execute() throws Exception {

        PageResult pageResult = executeAndGetPageResult();
        List<Map> rows = pageResult.getRows();
        if(CollectionUtil.isEmpty(rows)) {
            return new DatResultSet();
        }


        List<String> schema = new ArrayList<>();
        Set<String> keySet = rows.get(0).keySet();
        schema.addAll(keySet);


        return new DatResultSet(rows, schema);

    }

    @Override
    public PageResult executeAndGetPageResult() throws Exception {
        DescTableSqlToDslParser parser = new DescTableSqlToDslParser(sql);
        parser.handle();
        String tableName = parser.getTableName();



        TableSetting tableSetting = TableConfigurationBuilder.builder().tableName(tableName).build();

        PageResult pageResult = service.queryTableSchema(databaseSetting, tableSetting);
        return pageResult;
    }

}
