package com.meiya.whalex.db.module.dwh;

import com.meiya.whalex.db.entity.dwh.HiveKeywordHandler;
import com.meiya.whalex.sql.function.SqlFunctionParser;
import com.meiya.whalex.sql.function.dwh.*;
import com.meiya.whalex.sql.module.AbstractSqlParseHandler;
import com.meiya.whalex.sql.module.PrecompileSqlStatement;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Hive SQL 接口语法解析
 *
 * @author 黄河森
 * @date 2023/8/9
 * @project whalex-data-driver
 */
public class HiveSqlParseHandler extends AbstractSqlParseHandler {

    /**
     * 动态参数累加器（?）
     * 用来统计?在当前出现的次数，用于解决limit 10 offset 0 转换成 limit 0, 10 时，参数需要交换位置
     */
    private int dynamicParamAccumulator;

    /**
     * 参数位置交换索引映射
     */
    private Map<Integer, Integer> paramPositionSwapMap;


    /**
     * 函数解析器容器
     */
    private static Map<String, SqlFunctionParser> customFunctionParserMap = new ConcurrentHashMap<>();

    /**
     * 注册函数解析器
     */
    static {
        registerFunctionParser(customFunctionParserMap, new HiveDateDiffSqlFunctionParser());
        registerFunctionParser(customFunctionParserMap, new HiveDateDiff2SqlFunctionParser());
        registerFunctionParser(customFunctionParserMap, new HiveNowSqlFunctionParser());
        registerFunctionParser(customFunctionParserMap, new HiveJsonQuerySqlFunctionParser());
        registerFunctionParser(customFunctionParserMap, new HiveJsonValueSqlFunctionParser());
        registerFunctionParser(customFunctionParserMap, new HiveJsonExistsSqlFunctionParser());
        registerFunctionParser(customFunctionParserMap, new HiveJsonObjectSqlFunctionParser());
        registerFunctionParser(customFunctionParserMap, new HiveJsonContainsSqlFunctionParser());
        registerFunctionParser(customFunctionParserMap, new HiveJsonArraySqlFunctionParser());
        registerFunctionParser(customFunctionParserMap, new HiveJsonArraySqlFunctionParser());
        registerFunctionParser(customFunctionParserMap, new HiveDateAddSqlFunctionParser());
        registerFunctionParser(customFunctionParserMap, new HiveCurDateSqlFunctionParser());
        registerFunctionParser(customFunctionParserMap, new HiveIfNullSqlFunctionParser());
        registerFunctionParser(customFunctionParserMap, new HiveTruncateSqlFunctionParser());
        registerFunctionParser(customFunctionParserMap, new HiveGroupConcatSqlFunctionParser());
        registerFunctionParser(customFunctionParserMap, new HiveDateFormatSqlFunctionParser());
        registerFunctionParser(customFunctionParserMap, new HiveFormatSqlFunctionParser());
        registerFunctionParser(customFunctionParserMap, new HiveConvertSqlFunctionParser());
        registerFunctionParser(customFunctionParserMap, new HiveCastSqlFunctionParser());
        registerFunctionParser(customFunctionParserMap, new HiveLeftSqlFunctionParser());
        registerFunctionParser(customFunctionParserMap, new HiveMidSqlFunctionParser());
        registerFunctionParser(customFunctionParserMap, new HivePositionSqlFunctionParser());
        registerFunctionParser(customFunctionParserMap, new HiveRightSqlFunctionParser());
        registerFunctionParser(customFunctionParserMap, new HiveStrcmpSqlFunctionParser());
        registerFunctionParser(customFunctionParserMap, new HiveAtan2SqlFunctionParser());
        registerFunctionParser(customFunctionParserMap, new HiveCotSqlFunctionParser());
        registerFunctionParser(customFunctionParserMap, new HiveDivSqlFunctionParser());
        registerFunctionParser(customFunctionParserMap, new HiveAddDateSqlFunctionParser());
        registerFunctionParser(customFunctionParserMap, new HiveAddTimeSqlFunctionParser());
        registerFunctionParser(customFunctionParserMap, new HiveCurrentTimeSqlFunctionParser());
        registerFunctionParser(customFunctionParserMap, new HiveCurTimeSqlFunctionParser());
        registerFunctionParser(customFunctionParserMap, new HiveDateSubSqlFunctionParser());
        registerFunctionParser(customFunctionParserMap, new HiveDayNameSqlFunctionParser());
        registerFunctionParser(customFunctionParserMap, new HiveDayOfYearSqlFunctionParser());
        registerFunctionParser(customFunctionParserMap, new HiveFromDaysSqlFunctionParser());
        registerFunctionParser(customFunctionParserMap, new HiveLocalTimeSqlFunctionParser());
        registerFunctionParser(customFunctionParserMap, new HiveLocalTimestampSqlFunctionParser());
        registerFunctionParser(customFunctionParserMap, new HiveMakeDateSqlFunctionParser());
        registerFunctionParser(customFunctionParserMap, new HiveMakeTimeSqlFunctionParser());
        registerFunctionParser(customFunctionParserMap, new HiveMonthNameSqlFunctionParser());
        registerFunctionParser(customFunctionParserMap, new HivePeriodAddSqlFunctionParser());
        registerFunctionParser(customFunctionParserMap, new HivePeriodDiffSqlFunctionParser());
        registerFunctionParser(customFunctionParserMap, new HiveSecToTimeSqlFunctionParser());
        registerFunctionParser(customFunctionParserMap, new HiveSubDateSqlFunctionParser());
        registerFunctionParser(customFunctionParserMap, new HiveSubTimeSqlFunctionParser());
        registerFunctionParser(customFunctionParserMap, new HiveSysDateSqlFunctionParser());
        registerFunctionParser(customFunctionParserMap, new HiveTimeFormatSqlFunctionParser());
        registerFunctionParser(customFunctionParserMap, new HiveTimeToSecSqlFunctionParser());
        registerFunctionParser(customFunctionParserMap, new HiveTimestampDiff2SqlFunctionParser());
        registerFunctionParser(customFunctionParserMap, new HiveToDaysSqlFunctionParser());
        registerFunctionParser(customFunctionParserMap, new HiveWeekSqlFunctionParser());
        registerFunctionParser(customFunctionParserMap, new HiveWeekDaySqlFunctionParser());
        registerFunctionParser(customFunctionParserMap, new HiveYearWeekSqlFunctionParser());
    }

    public HiveSqlParseHandler() {
        super(new HiveKeywordHandler());
        this.paramPositionSwapMap = new HashMap<>();
    }

    @Override
    public PrecompileSqlStatement createPrecompileSqlStatement(String sql) {
        return new HivePrecompileSqlStatement(sql, paramPositionSwapMap);
    }

    @Override
    protected SqlFunctionParser getCustomFuncHandler(String funcName) {

        SqlFunctionParser sqlFunctionParser = customFunctionParserMap.get(funcName);
        if(sqlFunctionParser != null) {
            return sqlFunctionParser;
        }

        return super.getCustomFuncHandler(funcName);
    }
}
