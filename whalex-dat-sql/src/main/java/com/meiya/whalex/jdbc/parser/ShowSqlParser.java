package com.meiya.whalex.jdbc.parser;


import cn.hutool.core.map.MapUtil;
import com.meiya.whalex.db.builder.TableConfigurationBuilder;
import com.meiya.whalex.db.entity.DatabaseSetting;
import com.meiya.whalex.db.entity.PageResult;
import com.meiya.whalex.db.module.DbModuleService;
import com.meiya.whalex.db.resolver.IndexesResolver;
import com.meiya.whalex.interior.db.operation.in.QueryDatabasesCondition;
import com.meiya.whalex.interior.db.operation.in.QueryTablesCondition;
import com.meiya.whalex.interior.db.search.condition.Sort;
import com.meiya.whalex.jdbc.DatResultSet;
import com.meiya.whalex.sql2dsl.parser.ShowSqlToDslParser;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.*;

/**
 * 数据库列表sql解析
 * 表列表sql解析
 *
 * @author 蔡荣桂
 * @date 2022/06/27
 * @project whalex-dat-sql
 */
public class ShowSqlParser implements ISqlParser<DatResultSet> {

    private String sql;

    DbModuleService service;

    DatabaseSetting databaseSetting;

    public ShowSqlParser(String sql, DbModuleService service, DatabaseSetting databaseSetting) {
        this.sql = sql;
        this.service = service;
        this.databaseSetting = databaseSetting;
    }

    @Override
    public DatResultSet execute() throws Exception {

        ShowSqlToDslParser parser = new ShowSqlToDslParser(sql);
        Object condition = parser.handle();

        //表列表
        if (condition instanceof QueryTablesCondition) {
            PageResult pageResult = service.queryListTable(databaseSetting, (QueryTablesCondition) condition);
            List<Map> rows = handleResult(pageResult.getRows(), "tableName");
            return new DatResultSet(rows, Arrays.asList("tableName"));
        } else if (condition instanceof QueryDatabasesCondition) {
            //数据库列表
            PageResult pageResult = service.queryListDatabase(databaseSetting, (QueryDatabasesCondition) condition);
            List<Map> rows = handleResult(pageResult.getRows(), "databaseName");
            return new DatResultSet(rows, Arrays.asList("databaseName"));
        } else {
            String tableName = parser.getTableName();
            if(StringUtils.isBlank(tableName)) {
                throw new RuntimeException("表名不能为空");
            }
            PageResult pageResult = service.getIndexes(databaseSetting, TableConfigurationBuilder.builder().tableName(tableName).build());
            List<Map> showIndexesResult = handleShowIndexesResult(pageResult);
            return new DatResultSet(showIndexesResult, Arrays.asList("indexName", "unique", "primaryKey", "columnName", "collation"));
        }
    }

    @Override
    public PageResult executeAndGetPageResult() throws Exception {
        ShowSqlToDslParser parser = new ShowSqlToDslParser(sql);
        Object condition = parser.handle();
        PageResult pageResult;
        //表列表
        if (condition instanceof QueryTablesCondition) {
            pageResult = service.queryListTable(databaseSetting, (QueryTablesCondition) condition);
        } else if (condition instanceof QueryDatabasesCondition) {
            //数据库列表
            pageResult = service.queryListDatabase(databaseSetting, (QueryDatabasesCondition) condition);
        } else {
            String tableName = parser.getTableName();
            if(StringUtils.isBlank(tableName)) {
                throw new RuntimeException("表名不能为空");
            }
            pageResult = service.getIndexes(databaseSetting, TableConfigurationBuilder.builder().tableName(tableName).build());
        }
        return pageResult;
    }

    private List<Map> handleResult(List<Map> rows, String field) {

        List<Map> resultList = new ArrayList<>();

        if (CollectionUtils.isEmpty(rows)) {
            return resultList;
        }

        for (Map row : rows) {
            Object o = row.get(field);
            if (o == null) {
                Collection values = row.values();
                o = values.iterator().next();
            }

            Map result = new HashMap(1);
            result.put(field, o);
            resultList.add(result);
        }


        return resultList;

    }

    /**
     * 扁平化 show index 结果
     *
     * @param pageResult
     * @return
     */
    private List<Map> handleShowIndexesResult(PageResult pageResult) {
        IndexesResolver resolver = IndexesResolver.resolver(pageResult);
        List<IndexesResolver.IndexesResult> analysis = resolver.analysis();
        List<Map> indexes = new ArrayList<>();
        for (IndexesResolver.IndexesResult indexesResult : analysis) {
            String indexName = indexesResult.getIndexName();
            Boolean unique = indexesResult.getUnique();
            Boolean primaryKey = indexesResult.getPrimaryKey();
            LinkedHashMap<String, Sort> columns = indexesResult.getColumns();
            int position = 1;
            for (Map.Entry<String, Sort> entry : columns.entrySet()) {
                String column = entry.getKey();
                Sort sort = entry.getValue();
                Map<String, Object> columnMap = MapUtil.builder(new LinkedHashMap<String, Object>())
                        .put("indexName", indexName)
                        .put("unique", unique)
                        .put("primaryKey", primaryKey)
                        .put("columnName", column)
                        .put("columnPosition", position++)
                        .put("collation", sort == null ? null : sort.name())
                        .build();
                indexes.add(columnMap);
            }
        }
        return indexes;
    }

}
