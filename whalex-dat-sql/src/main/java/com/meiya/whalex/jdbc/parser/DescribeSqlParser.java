package com.meiya.whalex.jdbc.parser;

import com.meiya.whalex.db.entity.DatabaseSetting;
import com.meiya.whalex.db.entity.PageResult;
import com.meiya.whalex.db.entity.TableSetting;
import com.meiya.whalex.db.module.DbModuleService;
import com.meiya.whalex.jdbc.DatResultSet;
import com.meiya.whalex.sql2dsl.parser.DescribeSqlToDslParser;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 创建表sql解析
 *
 * @author 蔡荣桂
 * @date 2022/06/27
 * @project whalex-dat-sql
 */
public class DescribeSqlParser implements ISqlParser<DatResultSet> {

    DbModuleService service;

    DatabaseSetting databaseSetting;

    private String sql;

    public DescribeSqlParser(String sql, DbModuleService service, DatabaseSetting databaseSetting) {
        this.sql = sql;
        this.service = service;
        this.databaseSetting = databaseSetting;
    }




    @Override
    public DatResultSet execute() throws Exception {
        PageResult pageResult = executeAndGetPageResult();
        List<Map> rows = pageResult.getRows();
        List<String> schema = getSchema(rows);
        return new DatResultSet(rows, schema);
    }

    @Override
    public PageResult executeAndGetPageResult() throws Exception {

        DescribeSqlToDslParser parser = new DescribeSqlToDslParser(sql);
        parser.handle();

        String tableName = parser.getTableName();

        TableSetting tableSetting = TableSetting.builder().tableName(tableName).build();

        PageResult pageResult = service.queryTableInformation(databaseSetting, tableSetting);
        return pageResult;
    }

    public List<String> getSchema(List<Map> result) {

        List<String> schema = new ArrayList<>();

        if(CollectionUtils.isEmpty(result)) {
            return schema;
        }

        Set<String> keySet = new LinkedHashSet<>();

        for (Map map : result) {
            keySet.addAll(map.keySet());
        }

        schema.addAll(keySet);

        return schema;
    }


}
