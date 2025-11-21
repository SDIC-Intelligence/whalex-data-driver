package com.meiya.whalex.sql2dsl.parser;

import com.meiya.whalex.interior.db.search.condition.AggFunctionType;
import com.meiya.whalex.interior.db.search.condition.AggOpType;
import com.meiya.whalex.interior.db.search.condition.Rel;
import com.meiya.whalex.interior.db.search.condition.Sort;
import com.meiya.whalex.interior.db.search.in.*;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.List;

public class QuerySqlToDslParserTest {

    @Test
    public void queryTest() throws SqlParseException {

        String sql  = "select * from test where id = 1";
        QuerySqlToDslParser parser = new QuerySqlToDslParser(sql,  new Object[0]);
        QueryParamCondition condition = parser.handle();
        Assert.assertEquals("test", parser.getTableName());
        List<Where> whereList = condition.getWhere();
        Where where = whereList.get(0);
        Assert.assertEquals("id", where.getField());
        Assert.assertEquals(Rel.EQ, where.getType());
        Assert.assertEquals("1", where.getParam().toString());

    }

    @Test
    public void queryGroupTest() throws SqlParseException {

        String sql  = "select `firstClass.keyword`, count(`firstClass.keyword`) as c from opt_alarm_message group by `firstClass.keyword`";
        QuerySqlToDslParser parser = new QuerySqlToDslParser(sql,  new Object[0]);
        QueryParamCondition condition = parser.handle();
        Assert.assertEquals("opt_alarm_message", parser.getTableName());
        List<Aggs> aggList = condition.getAggList();
        Assert.assertNotNull(aggList);
        Aggs aggs = aggList.get(0);
        Assert.assertEquals(aggs.getField(), "firstClass.keyword");
        List<AggFunction> aggFunctions = aggs.getAggFunctions();
        Assert.assertNotNull(aggFunctions);

        List<String> select = condition.getSelect();

        Assert.assertNotNull(select);

        Assert.assertEquals(select.get(0), "firstClass.keyword");

        AggFunction aggFunction = aggFunctions.get(0);
        Assert.assertEquals(aggFunction.getField(), "firstClass.keyword");
        Assert.assertEquals(aggFunction.getAggFunctionType(), AggFunctionType.COUNT);

    }

    @Test
    public void queryGroupAndOrderTest() throws SqlParseException {

        String sql  = "SELECT `logSource.keyword` ,SUM(processPackageNum) AS `sum_of_processPackageNum` FROM data_package_copl_dtc_mt_city GROUP BY `logSource.keyword` ORDER BY `sum_of_processPackageNum` ASC LIMIT 2";
        QuerySqlToDslParser parser = new QuerySqlToDslParser(sql,  new Object[0]);
        QueryParamCondition condition = parser.handle();
        Assert.assertEquals("data_package_copl_dtc_mt_city", parser.getTableName());
        List<Aggs> aggList = condition.getAggList();
        Assert.assertNotNull(aggList);
        Aggs aggs = aggList.get(0);
        Assert.assertEquals(aggs.getField(), "logSource.keyword");

        List<String> select = condition.getSelect();

        Assert.assertNotNull(select);

        Assert.assertEquals(select.get(0), "logSource.keyword");


        List<AggFunction> aggFunctions = aggs.getAggFunctions();
        Assert.assertNotNull(aggFunctions);

        AggFunction aggFunction = aggFunctions.get(0);
        Assert.assertEquals(aggFunction.getField(), "processPackageNum");
        Assert.assertEquals(aggFunction.getAggFunctionType(), AggFunctionType.SUM);

        List<Order> orders = aggs.getOrders();

        Assert.assertNotNull(orders);
        Order order = orders.get(0);
        Assert.assertEquals("sum_of_processPackageNum", order.getField());
        Assert.assertEquals(Sort.ASC, order.getSort());

    }

    @Test
    public void queryGroupAndOrderTest2() throws SqlParseException {

        String sql  = "SELECT `logSource.keyword` ,SUM(processPackageNum) AS `sum_of_processPackageNum` FROM data_package_copl_dtc_mt_city GROUP BY `logSource.keyword` ORDER BY `logSource.keyword` DESC";
        QuerySqlToDslParser parser = new QuerySqlToDslParser(sql,  new Object[0]);
        QueryParamCondition condition = parser.handle();
        Assert.assertEquals("data_package_copl_dtc_mt_city", parser.getTableName());
        List<Aggs> aggList = condition.getAggList();
        Assert.assertNotNull(aggList);
        Aggs aggs = aggList.get(0);
        Assert.assertEquals(aggs.getField(), "logSource.keyword");

        List<String> select = condition.getSelect();

        Assert.assertNotNull(select);

        Assert.assertEquals(select.get(0), "logSource.keyword");


        List<AggFunction> aggFunctions = aggs.getAggFunctions();
        Assert.assertNotNull(aggFunctions);

        AggFunction aggFunction = aggFunctions.get(0);
        Assert.assertEquals(aggFunction.getField(), "processPackageNum");
        Assert.assertEquals(aggFunction.getAggFunctionType(), AggFunctionType.SUM);

        List<Order> orders = aggs.getOrders();

        Assert.assertNotNull(orders);
        Order order = orders.get(0);
        Assert.assertEquals("logSource.keyword", order.getField());
        Assert.assertEquals(Sort.DESC, order.getSort());

    }

    @Test
    public void queryFunctionTest() throws SqlParseException {

        String sql  = "select count(`processPackageNum`) as c, count(`processPackageNum`) as b from data_package_copl_dtc_mt_city";
        QuerySqlToDslParser parser = new QuerySqlToDslParser(sql,  new Object[0]);
        QueryParamCondition condition = parser.handle();
        Assert.assertEquals("data_package_copl_dtc_mt_city", parser.getTableName());
        List<AggFunction> aggFunctions = condition.getAggFunctionList();
        Assert.assertNotNull(aggFunctions);

        AggFunction aggFunction = aggFunctions.get(0);
        Assert.assertEquals(aggFunction.getField(), "processPackageNum");
        Assert.assertEquals(aggFunction.getAggFunctionType(), AggFunctionType.COUNT);
        Assert.assertEquals(aggFunction.getFunctionName(), "c");

        AggFunction aggFunction2 = aggFunctions.get(1);
        Assert.assertEquals(aggFunction2.getField(), "processPackageNum");
        Assert.assertEquals(aggFunction2.getAggFunctionType(), AggFunctionType.COUNT);
        Assert.assertEquals(aggFunction2.getFunctionName(), "b");

    }

    @Test
    public void queryDateHistogram() throws Exception {
        String sql = "select count(1)  as ros_alarm_num ,date_histogram(dataContent.alarmTime, '%Y-%m-%d', 1, `day`) from  `uwe_resource_*` GROUP BY date_histogram(dataContent.alarmTime, '%Y-%m-%d', 1, `day`,desc) LIMIT  11";
        QuerySqlToDslParser parser = new QuerySqlToDslParser(sql,  new Object[0]);
        QueryParamCondition condition = parser.handle();
        Assert.assertEquals("uwe_resource_*", parser.getTableName());
    }

    @Test
    public void queryDateFormat() throws Exception {
        String sql = "select count(`dataContent.alarmTime`) as ros_alarm_num ,DATE_FORMAT(`dataContent.alarmTime`, '%Y-%m-%d') as `alarmTime_date_format` from `uwe_resource_*` GROUP BY DATE_FORMAT(`dataContent.alarmTime`, '%Y-%m-%d') ORDER BY `alarmTime_date_format` LIMIT  11";
        QuerySqlToDslParser parser = new QuerySqlToDslParser(sql,  new Object[0]);
        QueryParamCondition condition = parser.handle();
        Assert.assertEquals("uwe_resource_*", parser.getTableName());
        List<Aggs> aggList = condition.getAggList();
        Assert.assertNotNull(aggList);
        Aggs aggs = aggList.get(0);
        Assert.assertEquals(aggs.getField(), "dataContent.alarmTime");
        Assert.assertEquals(aggs.getType(), AggOpType.DATE_HISTOGRAM);
        Assert.assertEquals(aggs.getFormat(), "yyyy-MM-dd");
        Assert.assertEquals(aggs.getAggName(), "alarmTime_date_format");


        List<AggFunction> aggFunctions = aggs.getAggFunctions();
        Assert.assertNotNull(aggFunctions);

        AggFunction aggFunction = aggFunctions.get(0);
        Assert.assertEquals(aggFunction.getField(), "dataContent.alarmTime");
        Assert.assertEquals(aggFunction.getAggFunctionType(), AggFunctionType.COUNT);

        List<Order> orders = aggs.getOrders();

        Assert.assertNotNull(orders);
        Order order = orders.get(0);
        Assert.assertEquals("dataContent.alarmTime", order.getField());
        Assert.assertEquals(Sort.ASC, order.getSort());
    }

    @Test
    public void queryDateFormat2() throws Exception {
        String sql = "select count(`dataContent.alarmTime`) as ros_alarm_num ,DATE_FORMAT(`dataContent.alarmTime`, '%Y-%m-%d') as `alarmTime_date_format` from `uwe_resource_*` GROUP BY DATE_FORMAT(`dataContent.alarmTime`, '%Y-%m-%d') ORDER BY DATE_FORMAT(`dataContent.alarmTime`, '%Y-%m-%d') LIMIT  11";
        QuerySqlToDslParser parser = new QuerySqlToDslParser(sql,  new Object[0]);
        QueryParamCondition condition = parser.handle();
        Assert.assertEquals("uwe_resource_*", parser.getTableName());
        List<Aggs> aggList = condition.getAggList();
        Assert.assertNotNull(aggList);
        Aggs aggs = aggList.get(0);
        Assert.assertEquals(aggs.getField(), "dataContent.alarmTime");
        Assert.assertEquals(aggs.getType(), AggOpType.DATE_HISTOGRAM);
        Assert.assertEquals(aggs.getFormat(), "yyyy-MM-dd");
        Assert.assertEquals(aggs.getAggName(), "alarmTime_date_format");


        List<AggFunction> aggFunctions = aggs.getAggFunctions();
        Assert.assertNotNull(aggFunctions);

        AggFunction aggFunction = aggFunctions.get(0);
        Assert.assertEquals(aggFunction.getField(), "dataContent.alarmTime");
        Assert.assertEquals(aggFunction.getAggFunctionType(), AggFunctionType.COUNT);

        List<Order> orders = aggs.getOrders();

        Assert.assertNotNull(orders);
        Order order = orders.get(0);
        Assert.assertEquals("dataContent.alarmTime", order.getField());
        Assert.assertEquals(Sort.ASC, order.getSort());
    }

    @Test
    public void queryDateFormat3() throws Exception {
        String sql = "select count(`dataContent.alarmTime`) as ros_alarm_num ,DATE_FORMAT(`dataContent.alarmTime`, '%Y-%m-%d') from `uwe_resource_*` GROUP BY DATE_FORMAT(`dataContent.alarmTime`, '%Y-%m-%d') ORDER BY DATE_FORMAT(`dataContent.alarmTime`, '%Y-%m-%d') LIMIT  11";
        QuerySqlToDslParser parser = new QuerySqlToDslParser(sql,  new Object[0]);
        QueryParamCondition condition = parser.handle();
        Assert.assertEquals("uwe_resource_*", parser.getTableName());
        List<Aggs> aggList = condition.getAggList();
        Assert.assertNotNull(aggList);
        Aggs aggs = aggList.get(0);
        Assert.assertEquals(aggs.getField(), "dataContent.alarmTime");
        Assert.assertEquals(aggs.getType(), AggOpType.DATE_HISTOGRAM);
        Assert.assertEquals(aggs.getFormat(), "yyyy-MM-dd");
        Assert.assertEquals(aggs.getAggName(), "DATE_FORMAT_dataContent_alarmTime");


        List<AggFunction> aggFunctions = aggs.getAggFunctions();
        Assert.assertNotNull(aggFunctions);

        AggFunction aggFunction = aggFunctions.get(0);
        Assert.assertEquals(aggFunction.getField(), "dataContent.alarmTime");
        Assert.assertEquals(aggFunction.getAggFunctionType(), AggFunctionType.COUNT);

        List<Order> orders = aggs.getOrders();

        Assert.assertNotNull(orders);
        Order order = orders.get(0);
        Assert.assertEquals("dataContent.alarmTime", order.getField());
        Assert.assertEquals(Sort.ASC, order.getSort());
    }

    @Test
    public void queryDateFormatHaving() throws Exception {
        String sql = "select count(`dataContent.alarmTime`) as ros_alarm_num ,DATE_FORMAT(`dataContent.alarmTime`, '%Y-%m-%d') AS `data_format` from `uwe_resource_*` GROUP BY DATE_FORMAT(`dataContent.alarmTime`, '%Y-%m-%d') HAVING `data_format` > '2023-11-10' ORDER BY DATE_FORMAT(`dataContent.alarmTime`, '%Y-%m-%d') LIMIT 11";
        QuerySqlToDslParser parser = new QuerySqlToDslParser(sql,  new Object[0]);
        QueryParamCondition condition = parser.handle();
        Assert.assertEquals("uwe_resource_*", parser.getTableName());
        List<Aggs> aggList = condition.getAggList();
        Assert.assertNotNull(aggList);
        Aggs aggs = aggList.get(0);
        Assert.assertEquals(aggs.getField(), "dataContent.alarmTime");
        Assert.assertEquals(aggs.getType(), AggOpType.DATE_HISTOGRAM);
        Assert.assertEquals(aggs.getFormat(), "yyyy-MM-dd");
        Assert.assertEquals(aggs.getAggName(), "data_format");


        List<AggFunction> aggFunctions = aggs.getAggFunctions();
        Assert.assertNotNull(aggFunctions);

        AggFunction aggFunction = aggFunctions.get(0);
        Assert.assertEquals(aggFunction.getField(), "dataContent.alarmTime");
        Assert.assertEquals(aggFunction.getAggFunctionType(), AggFunctionType.COUNT);
        Assert.assertEquals(aggFunction.getFunctionName(), "ros_alarm_num");

        List<Order> orders = aggs.getOrders();

        Assert.assertNotNull(orders);
        Order order = orders.get(0);
        Assert.assertEquals("dataContent.alarmTime", order.getField());
        Assert.assertEquals(Sort.ASC, order.getSort());

        Where having = aggs.getHaving();
        Assert.assertNotNull(having);

        Assert.assertEquals(having.getField(), "data_format");
        Assert.assertEquals(having.getParam(), "2023-11-10");
        Assert.assertEquals(having.getType(), Rel.GT);
    }

    @Test
    public void queryDateFormatHaving2() throws Exception {
        String sql = "select count(`dataContent.alarmTime`) as ros_alarm_num ,DATE_FORMAT(`dataContent.alarmTime`, '%Y-%m-%d') AS `data_format` from `uwe_resource_*` GROUP BY DATE_FORMAT(`dataContent.alarmTime`, '%Y-%m-%d') HAVING `ros_alarm_num` > 10 ORDER BY DATE_FORMAT(`dataContent.alarmTime`, '%Y-%m-%d') LIMIT 11";
        QuerySqlToDslParser parser = new QuerySqlToDslParser(sql,  new Object[0]);
        QueryParamCondition condition = parser.handle();
        Assert.assertEquals("uwe_resource_*", parser.getTableName());
        List<Aggs> aggList = condition.getAggList();
        Assert.assertNotNull(aggList);
        Aggs aggs = aggList.get(0);
        Assert.assertEquals(aggs.getField(), "dataContent.alarmTime");
        Assert.assertEquals(aggs.getType(), AggOpType.DATE_HISTOGRAM);
        Assert.assertEquals(aggs.getFormat(), "yyyy-MM-dd");
        Assert.assertEquals(aggs.getAggName(), "data_format");


        List<AggFunction> aggFunctions = aggs.getAggFunctions();
        Assert.assertNotNull(aggFunctions);

        AggFunction aggFunction = aggFunctions.get(0);
        Assert.assertEquals(aggFunction.getField(), "dataContent.alarmTime");
        Assert.assertEquals(aggFunction.getAggFunctionType(), AggFunctionType.COUNT);
        Assert.assertEquals(aggFunction.getFunctionName(), "ros_alarm_num");

        List<Order> orders = aggs.getOrders();

        Assert.assertNotNull(orders);
        Order order = orders.get(0);
        Assert.assertEquals("dataContent.alarmTime", order.getField());
        Assert.assertEquals(Sort.ASC, order.getSort());

        Where having = aggs.getHaving();
        Assert.assertNotNull(having);

        Assert.assertEquals("ros_alarm_num", having.getField());
        Assert.assertEquals(10, ((BigDecimal)having.getParam()).intValue());
        Assert.assertEquals(having.getType(), Rel.GT);
    }

    @Test
    public void queryDateFormatHaving3() throws Exception {
        String sql = "select count(`dataContent.alarmTime`) as ros_alarm_num ,DATE_FORMAT(`dataContent.alarmTime`, '%Y-%m-%d') AS `data_format` from `uwe_resource_*` GROUP BY DATE_FORMAT(`dataContent.alarmTime`, '%Y-%m-%d') HAVING (`data_format` > '2023-11-10' AND `data_format` < '2024-11-10') AND ros_alarm_num > 10 ORDER BY DATE_FORMAT(`dataContent.alarmTime`, '%Y-%m-%d') LIMIT 11";
        QuerySqlToDslParser parser = new QuerySqlToDslParser(sql,  new Object[0]);
        QueryParamCondition condition = parser.handle();
        Assert.assertEquals("uwe_resource_*", parser.getTableName());
        List<Aggs> aggList = condition.getAggList();
        Assert.assertNotNull(aggList);
        Aggs aggs = aggList.get(0);
        Assert.assertEquals(aggs.getField(), "dataContent.alarmTime");
        Assert.assertEquals(aggs.getType(), AggOpType.DATE_HISTOGRAM);
        Assert.assertEquals(aggs.getFormat(), "yyyy-MM-dd");
        Assert.assertEquals(aggs.getAggName(), "data_format");


        List<AggFunction> aggFunctions = aggs.getAggFunctions();
        Assert.assertNotNull(aggFunctions);

        AggFunction aggFunction = aggFunctions.get(0);
        Assert.assertEquals(aggFunction.getField(), "dataContent.alarmTime");
        Assert.assertEquals(aggFunction.getAggFunctionType(), AggFunctionType.COUNT);
        Assert.assertEquals(aggFunction.getFunctionName(), "ros_alarm_num");

        List<Order> orders = aggs.getOrders();

        Assert.assertNotNull(orders);
        Order order = orders.get(0);
        Assert.assertEquals("dataContent.alarmTime", order.getField());
        Assert.assertEquals(Sort.ASC, order.getSort());

        Where having = aggs.getHaving();
        Assert.assertNotNull(having);

        List<Where> params = having.getParams();

        Assert.assertNotNull(params);

        Where where = params.get(0);

        List<Where> params1 = where.getParams();
        Assert.assertNotNull(params1);
        Where where1 = params1.get(0);
        Where where2 = params1.get(1);

        Assert.assertEquals(where1.getField(), "data_format");
        Assert.assertEquals(where1.getParam(), "2023-11-10");
        Assert.assertEquals(where1.getType(), Rel.GT);

        Assert.assertEquals(where2.getField(), "data_format");
        Assert.assertEquals(where2.getParam(), "2024-11-10");
        Assert.assertEquals(where2.getType(), Rel.LT);

        Where where3 = params.get(1);

        Assert.assertEquals("ros_alarm_num", where3.getField());
        Assert.assertEquals(10, ((BigDecimal)where3.getParam()).intValue());
        Assert.assertEquals(where3.getType(), Rel.GT);
    }
}
