package com.meiya.whalex.jdbc.parser;

import com.meiya.whalex.db.builder.TableConfigurationBuilder;
import com.meiya.whalex.db.entity.DatabaseSetting;
import com.meiya.whalex.db.entity.PageResult;
import com.meiya.whalex.db.entity.TableSetting;
import com.meiya.whalex.db.module.DbModuleService;
import com.meiya.whalex.interior.db.search.condition.AggFunctionType;
import com.meiya.whalex.interior.db.search.in.AggFunction;
import com.meiya.whalex.interior.db.search.in.Aggs;
import com.meiya.whalex.sql.module.RdbmsModuleService;
import com.meiya.whalex.interior.db.search.in.QueryParamCondition;
import com.meiya.whalex.jdbc.DatResultSet;
import com.meiya.whalex.sql2dsl.parser.QuerySqlToDslParser;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * 查询sql解析
 *
 * @author 蔡荣桂
 * @date 2022/06/27
 * @project whalex-dat-sql
 */
public class QuerySqlParser implements ISqlParser<DatResultSet>{

    private String sql;

    private Object[] params;

    DbModuleService service;

    DatabaseSetting databaseSetting;

    //最多查几条 等于0 不做限制
    private int maxRows;

    public QuerySqlParser(String sql, Object[] params, DbModuleService service, DatabaseSetting databaseSetting, int maxRows) {
        this.sql = sql;
        this.params = params;
        this.service = service;
        this.databaseSetting = databaseSetting;
        if(maxRows < 0) {
            maxRows = 0;
        }
        this.maxRows = maxRows;
    }


    private PageResult _executeBySql() throws Exception {
        RdbmsModuleService rdbmsModuleService = (RdbmsModuleService) service;

        List<Object> paramList = new ArrayList<>();
        if(params != null) {
            paramList.addAll(Arrays.asList(params));
        }

        PageResult pageResult = rdbmsModuleService.queryBySql(databaseSetting, sql, paramList);
        return pageResult;
    }

    private DatResultSet executeBySql() throws Exception {
        PageResult pageResult = _executeBySql();
        List<Map> rows = pageResult.getRows();

        List<String> schema = new ArrayList<>();

        if(!rows.isEmpty()) {
            Set<String> keySet = rows.get(0).keySet();
            schema.addAll(keySet);
        }

        if(maxRows > 0 && rows.size() > maxRows) {
            rows = rows.subList(0, maxRows);
        }

        return new DatResultSet(rows, schema);
    }


    public DatResultSet execute() throws Exception {

        // 使用复杂sql能力，不需要解析
        if(service instanceof RdbmsModuleService) {
           return executeBySql();
        }


        QuerySqlToDslParser parser = new QuerySqlToDslParser(sql, maxRows, params);
        QueryParamCondition queryParamCondition = parser.handle();


        TableSetting tableSetting = TableConfigurationBuilder.builder().tableName(parser.getTableName()).build();

        PageResult pageResult = null;
        List<String> select = queryParamCondition.getSelect();
        Aggs aggs = queryParamCondition.getAggs();
        List<AggFunction> aggFunctionList = queryParamCondition.getAggFunctionList();
        //count(1)或count(*)的情况
        if(CollectionUtils.isEmpty(select) && aggs == null && CollectionUtils.isNotEmpty(aggFunctionList)) {
            boolean countFlag = true;
            for (AggFunction aggFunction : aggFunctionList) {
                String field = aggFunction.getField();
                AggFunctionType aggFunctionType = aggFunction.getAggFunctionType();
                if(!(aggFunctionType == AggFunctionType.COUNT
                        && ("1".equalsIgnoreCase(field) || "".equalsIgnoreCase(field)))) {
                    countFlag = false;
                    break;
                }
            }
            //进行统计查询
            if(countFlag) {
                queryParamCondition.setAggFunctionList(null);
                pageResult = service.statisticalLine(databaseSetting, tableSetting, queryParamCondition);
                long total = pageResult.getTotal();
                Map<String, Object> aggsMap = new HashMap<>();
                Map<String, Object> aggregationsMap = new HashMap<>();
                for (AggFunction aggFunction : aggFunctionList) {
                    String functionName = aggFunction.getFunctionName();
                    Map<String, Object> valueMap = new HashMap<>();
                    valueMap.put("value", (int)total);
                    aggregationsMap.put(functionName, valueMap);
                }
                aggsMap.put("aggregations", aggregationsMap);
                Map<String, Object> row = new HashMap<>();
                row.put("aggs", aggsMap);
                pageResult.setRows(Arrays.asList(row));
                //结果解析
                List<Map> result = parser.handleResult(pageResult);
                //获取schema
                List<String> schema = parser.getSchema(result);
                return new DatResultSet(result, schema);

            }
        }

        pageResult = service.queryList(databaseSetting, tableSetting, queryParamCondition);

        //结果解析
        List<Map> result = parser.handleResult(pageResult);

        //获取schema
        List<String> schema = parser.getSchema(result);


        return new DatResultSet(result, schema);
    }

    @Override
    public PageResult executeAndGetPageResult() throws Exception {
        // 使用复杂sql能力，不需要解析
        if(service instanceof RdbmsModuleService) {
            return _executeBySql();
        }

        QuerySqlToDslParser parser = new QuerySqlToDslParser(sql, maxRows, params);
        QueryParamCondition queryParamCondition = parser.handle();

        TableSetting tableSetting = TableConfigurationBuilder.builder().tableName(parser.getTableName()).build();


        PageResult pageResult = null;
        List<String> select = queryParamCondition.getSelect();
        Aggs aggs = queryParamCondition.getAggs();
        List<AggFunction> aggFunctionList = queryParamCondition.getAggFunctionList();
        //count(1)或count(*)的情况
        if(CollectionUtils.isEmpty(select) && aggs == null && CollectionUtils.isNotEmpty(aggFunctionList)) {
            boolean countFlag = true;
            for (AggFunction aggFunction : aggFunctionList) {
                String field = aggFunction.getField();
                AggFunctionType aggFunctionType = aggFunction.getAggFunctionType();
                if(!(aggFunctionType == AggFunctionType.COUNT
                        && ("1".equalsIgnoreCase(field) || "".equalsIgnoreCase(field)))) {
                    countFlag = false;
                    break;
                }
            }
            //进行统计查询
            if(countFlag) {
                queryParamCondition.setAggFunctionList(null);
                pageResult = service.statisticalLine(databaseSetting, tableSetting, queryParamCondition);
                long total = pageResult.getTotal();
                Map<String, Object> aggsMap = new HashMap<>();
                Map<String, Object> aggregationsMap = new HashMap<>();
                for (AggFunction aggFunction : aggFunctionList) {
                    String functionName = aggFunction.getFunctionName();
                    Map<String, Object> valueMap = new HashMap<>();
                    valueMap.put("value", (int)total);
                    aggregationsMap.put(functionName, valueMap);
                }
                aggsMap.put("aggregations", aggregationsMap);
                Map<String, Object> row = new HashMap<>();
                row.put("aggs", aggsMap);
                pageResult.setRows(Arrays.asList(row));
                //结果解析
                List<Map> result = parser.handleResult(pageResult);
                parser.getSchema(result);
                pageResult.setRows(result);
                return pageResult;
            }
        }

        pageResult = service.queryList(databaseSetting, tableSetting, queryParamCondition);

        //结果解析
        List<Map> result = parser.handleResult(pageResult);
        parser.getSchema(result);
        pageResult.setRows(result);

        return pageResult;
    }

}
