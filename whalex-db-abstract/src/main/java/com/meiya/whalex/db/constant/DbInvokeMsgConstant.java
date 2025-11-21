package com.meiya.whalex.db.constant;

/**
 * 组件执行信息常量
 *
 * @author 黄河森
 * @date 2020/4/18
 * @project whale-cloud-platformX
 */
public interface DbInvokeMsgConstant {

    /**
     * 原生查询语句
     */
    String QUERY_SQL_STR = "QUERY_SQL_STR";

    /**
     * 数据库地址
     */
    String DATABASE_ADDR = "DATABASE_ADDR";

    /**
     * 数据库名
     */
    String DATABASE_NAME = "DATABASE_NAME";

    /**
     * 执行表名
     */
    String TABLE_NAME = "TABLE_NAME";

    /**
     * 查询耗时
     */
    String QUERY_EXECUTE_COST = "QUERY_EXECUTE_COST";

    /**
     * 获取数据库配置组装耗时
     */
    String GET_DATABASE_COST = "GET_DATABASE_COST";

    /**
     * 获取表配置组装耗时
     */
    String GET_TABLE_COST = "GET_TABLE_COST";

    /**
     * 获取参数组装耗时
     */
    String GET_CONDITION_COST = "GET_CONDITION_COST";

    /**
     * 获取数据库连接创建耗时
     */
    String GET_DB_CONNECT_COST = "GET_DB_CONNECT_COST";

    /**
     * 全链路耗时
     */
    String LINK_COST = "LINK_COST";

}
