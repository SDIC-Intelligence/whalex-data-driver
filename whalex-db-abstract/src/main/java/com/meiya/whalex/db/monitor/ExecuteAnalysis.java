package com.meiya.whalex.db.monitor;

import cn.hutool.core.util.NumberUtil;
import cn.hutool.core.util.ObjectUtil;
import com.meiya.whalex.db.constant.DbInvokeMsgConstant;
import com.meiya.whalex.util.concurrent.ThreadLocalUtil;
import lombok.Data;

/**
 * @author 黄河森
 * @date 2021/8/4
 * @project whalex-data-driver-back
 */
@Data
public class ExecuteAnalysis {

    private String statement;

    private long invokeDbCost;
    private long databaseCost;
    private long tableSettingCost;
    private long paramConditionCost;
    private long getDbConnectCost;
    private long linkCost;

    private String remoteDbAddr;

    private String tableName;

    private String database;

    private ExecuteAnalysis() {
        Object o = ThreadLocalUtil.get(DbInvokeMsgConstant.QUERY_SQL_STR);
        statement = ObjectUtil.toString(o);
        Object o1 = ThreadLocalUtil.get(DbInvokeMsgConstant.DATABASE_ADDR);
        remoteDbAddr = ObjectUtil.toString(o1);
        Object o2 = ThreadLocalUtil.get(DbInvokeMsgConstant.DATABASE_NAME);
        database = ObjectUtil.toString(o2);
        Object o3 = ThreadLocalUtil.get(DbInvokeMsgConstant.TABLE_NAME);
        tableName = ObjectUtil.toString(o3);
        Object o4 = ThreadLocalUtil.get(DbInvokeMsgConstant.QUERY_EXECUTE_COST);
        if (!ObjectUtil.isNull(o4) && NumberUtil.isLong(String.valueOf(o4))) {
            invokeDbCost = Long.valueOf(String.valueOf(o4));
        }
        Object o5 = ThreadLocalUtil.get(DbInvokeMsgConstant.GET_DATABASE_COST);
        if (!ObjectUtil.isNull(o5) && NumberUtil.isLong(String.valueOf(o5))) {
            databaseCost = Long.valueOf(String.valueOf(o5));
        }
        Object o6 = ThreadLocalUtil.get(DbInvokeMsgConstant.GET_TABLE_COST);
        if (!ObjectUtil.isNull(o6) && NumberUtil.isLong(String.valueOf(o6))) {
            tableSettingCost = Long.valueOf(String.valueOf(o6));
        }
        Object o7 = ThreadLocalUtil.get(DbInvokeMsgConstant.GET_CONDITION_COST);
        if (!ObjectUtil.isNull(o7) && NumberUtil.isLong(String.valueOf(o7))) {
            paramConditionCost = Long.valueOf(String.valueOf(o7));
        }
        Object o8 = ThreadLocalUtil.get(DbInvokeMsgConstant.GET_DB_CONNECT_COST);
        if (!ObjectUtil.isNull(o8) && NumberUtil.isLong(String.valueOf(o8))) {
            getDbConnectCost = Long.valueOf(String.valueOf(o8));
        }
        Object o9 = ThreadLocalUtil.get(DbInvokeMsgConstant.GET_DB_CONNECT_COST);
        if (!ObjectUtil.isNull(o9) && NumberUtil.isLong(String.valueOf(o9))) {
            linkCost = Long.valueOf(String.valueOf(o9));
        }
        ThreadLocalUtil.remove();
    }

    public static ExecuteAnalysis execute() {
        return new ExecuteAnalysis();
    }

    @Override
    public String toString() {
        return "当前任务执行分析: {" +
                "statement='" + statement + '\'' +
                ", remoteDbAddr='" + remoteDbAddr + '\'' +
                ", tableName='" + tableName + '\'' +
                ", database='" + database + '\'' +
                ", 解析数据库配置耗时=" + databaseCost + "ms" +
                ", 解析表配置耗时=" + tableSettingCost + "ms" +
                ", 数据库连接创建耗时=" + getDbConnectCost + "ms" +
                ", 参数解析耗时=" + paramConditionCost + "ms" +
                ", 全链路总耗时=" + linkCost + "ms" +
                '}';
    }
}
