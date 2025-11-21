package com.meiya.whalex.db.module.ani;

import com.google.common.collect.ImmutableList;
import com.meiya.whalex.db.entity.ani.BaseOracleDatabaseInfo;
import com.meiya.whalex.db.entity.ani.KeywordConstant;
import com.meiya.whalex.sql.function.SqlFunctionParser;
import com.meiya.whalex.sql.function.ani.OracleAddDateFunctionParser;
import com.meiya.whalex.sql.function.ani.OracleAddTimeFunctionParser;
import com.meiya.whalex.sql.function.ani.OracleCharLengthSqlFunctionParser;
import com.meiya.whalex.sql.function.ani.OracleCharacterLengthSqlFunctionParser;
import com.meiya.whalex.sql.function.ani.OracleCurDateFunctionParser;
import com.meiya.whalex.sql.function.ani.OracleCurrentDateFunctionParser;
import com.meiya.whalex.sql.function.ani.OracleCurrentTimeFunctionParser;
import com.meiya.whalex.sql.function.ani.OracleCurrentTimestampFunctionParser;
import com.meiya.whalex.sql.function.ani.OracleCurtimeFunctionParser;
import com.meiya.whalex.sql.function.ani.OracleDateAddFunctionParser;
import com.meiya.whalex.sql.function.ani.OracleDateDiff2FunctionParser;
import com.meiya.whalex.sql.function.ani.OracleDateDiffFunctionParser;
import com.meiya.whalex.sql.function.ani.OracleDateFormatFunctionParser;
import com.meiya.whalex.sql.function.ani.OracleDateSubFunctionParser;
import com.meiya.whalex.sql.function.ani.OracleDayNameFunctionParser;
import com.meiya.whalex.sql.function.ani.OracleDayOfMonthFunctionParser;
import com.meiya.whalex.sql.function.ani.OracleDayOfWeekFunctionParser;
import com.meiya.whalex.sql.function.ani.OracleDayOfYearFunctionParser;
import com.meiya.whalex.sql.function.ani.OracleDivSqlFunctionParser;
import com.meiya.whalex.sql.function.ani.OracleExtractFunctionParser;
import com.meiya.whalex.sql.function.ani.OracleFormatFunctionParser;
import com.meiya.whalex.sql.function.ani.OracleFromDaysFunctionParser;
import com.meiya.whalex.sql.function.ani.OracleGroupConcatSqlFunctionParser;
import com.meiya.whalex.sql.function.ani.OracleHourFunctionParser;
import com.meiya.whalex.sql.function.ani.OracleIfFunctionParser;
import com.meiya.whalex.sql.function.ani.OracleIfNullFunctionParser;
import com.meiya.whalex.sql.function.ani.OracleIsNullFunctionParser;
import com.meiya.whalex.sql.function.ani.OracleLastDayFunctionParser;
import com.meiya.whalex.sql.function.ani.OracleLcaseFunctionParser;
import com.meiya.whalex.sql.function.ani.OracleLocaltimeFunctionParser;
import com.meiya.whalex.sql.function.ani.OracleLocaltimestampFunctionParser;
import com.meiya.whalex.sql.function.ani.OracleLocateFunctionParser;
import com.meiya.whalex.sql.function.ani.OracleLog10FunctionParser;
import com.meiya.whalex.sql.function.ani.OracleLog2FunctionParser;
import com.meiya.whalex.sql.function.ani.OracleLogFunctionParser;
import com.meiya.whalex.sql.function.ani.OracleMakeTimeFunctionParser;
import com.meiya.whalex.sql.function.ani.OracleMicrosecondFunctionParser;
import com.meiya.whalex.sql.function.ani.OracleMidFunctionParser;
import com.meiya.whalex.sql.function.ani.OracleMinuteFunctionParser;
import com.meiya.whalex.sql.function.ani.OracleMonthFunctionParser;
import com.meiya.whalex.sql.function.ani.OracleMonthNameFunctionParser;
import com.meiya.whalex.sql.function.ani.OracleNowSqlFunctionParser;
import com.meiya.whalex.sql.function.ani.OraclePiFunctionParser;
import com.meiya.whalex.sql.function.ani.OraclePositionFunctionParser;
import com.meiya.whalex.sql.function.ani.OraclePowFunctionParser;
import com.meiya.whalex.sql.function.ani.OracleQuarterFunctionParser;
import com.meiya.whalex.sql.function.ani.OracleRandFunctionParser;
import com.meiya.whalex.sql.function.ani.OracleSecondFunctionParser;
import com.meiya.whalex.sql.function.ani.OracleSpaceFunctionParser;
import com.meiya.whalex.sql.function.ani.OracleSplitFunctionParser;
import com.meiya.whalex.sql.function.ani.OracleSubDateFunctionParser;
import com.meiya.whalex.sql.function.ani.OracleSubStringFunctionParser;
import com.meiya.whalex.sql.function.ani.OracleSubTimeFunctionParser;
import com.meiya.whalex.sql.function.ani.OracleSysDateFunctionParser;
import com.meiya.whalex.sql.function.ani.OracleTimeFormatFunctionParser;
import com.meiya.whalex.sql.function.ani.OracleTimestampDiffFunctionParser;
import com.meiya.whalex.sql.function.ani.OracleToDaysFunctionParser;
import com.meiya.whalex.sql.function.ani.OracleTruncateSqlFunctionParser;
import com.meiya.whalex.sql.function.ani.OracleUcaseFunctionParser;
import com.meiya.whalex.sql.function.ani.OracleWeekDayFunctionParser;
import com.meiya.whalex.sql.function.ani.OracleWeekFunctionParser;
import com.meiya.whalex.sql.function.ani.OracleWeekOfYearFunctionParser;
import com.meiya.whalex.sql.function.ani.OracleYearFunctionParser;
import com.meiya.whalex.sql.function.ani.OracleYearWeekFunctionParser;
import com.meiya.whalex.sql.module.AbstractSqlParseHandler;
import com.meiya.whalex.sql.module.PrecompileSqlStatement;
import lombok.Data;
import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlIntervalOperator;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author 蔡荣桂
 * @date 2022/11/15
 * @project whalex-data-driver
 */
public class OracleSqlParseHandler extends AbstractSqlParseHandler {

    public final static String DOUBLE_QUOTATION_MARKS = "\"";

    public final static String SINGLE_QUOTATION_MARKS = "'";

    private final static String PAGE_TEMPLATE = "SELECT * FROM (SELECT A.*, ROWNUM ORACLE_PAGE_FIELD FROM (%s) A WHERE ROWNUM <= ?) WHERE ORACLE_PAGE_FIELD >= ?";

    /**
     * 函数解析器容器
     */
    private static Map<String, SqlFunctionParser> customFunctionParserMap = new ConcurrentHashMap<>();

    /**
     * 动态参数累加器（?）
     * 用来统计?在当前出现的次数，用于解决limit 10 offset 0 转换成 limit 0, 10 时，参数需要交换位置
     */
    private int dynamicParamAccumulator;

    List<LimitOffset> limitOffsets = new ArrayList<>();

    /**
     * 注册函数解析器
     */
    static {
        registerFunctionParser(customFunctionParserMap, new OracleIfFunctionParser());
        registerFunctionParser(customFunctionParserMap, new OracleIfNullFunctionParser());
        registerFunctionParser(customFunctionParserMap, new OracleLcaseFunctionParser());
        registerFunctionParser(customFunctionParserMap, new OracleUcaseFunctionParser());
        registerFunctionParser(customFunctionParserMap, new OracleTruncateSqlFunctionParser());
        registerFunctionParser(customFunctionParserMap, new OracleFormatFunctionParser());
        registerFunctionParser(customFunctionParserMap, new OracleSubStringFunctionParser());
        registerFunctionParser(customFunctionParserMap, new OracleCharLengthSqlFunctionParser());
        registerFunctionParser(customFunctionParserMap, new OracleCharacterLengthSqlFunctionParser());
        registerFunctionParser(customFunctionParserMap, new OracleLocateFunctionParser());
        registerFunctionParser(customFunctionParserMap, new OracleSpaceFunctionParser());
        registerFunctionParser(customFunctionParserMap, new OracleLogFunctionParser());
        registerFunctionParser(customFunctionParserMap, new OracleLog10FunctionParser());
        registerFunctionParser(customFunctionParserMap, new OracleLog2FunctionParser());
        registerFunctionParser(customFunctionParserMap, new OraclePiFunctionParser());
        registerFunctionParser(customFunctionParserMap, new OraclePowFunctionParser());
        registerFunctionParser(customFunctionParserMap, new OracleIsNullFunctionParser());
        registerFunctionParser(customFunctionParserMap, new OracleRandFunctionParser());
        registerFunctionParser(customFunctionParserMap, new OracleDivSqlFunctionParser());
        registerFunctionParser(customFunctionParserMap, new OracleMidFunctionParser());
        registerFunctionParser(customFunctionParserMap, new OraclePositionFunctionParser());
        registerFunctionParser(customFunctionParserMap, new OracleGroupConcatSqlFunctionParser());
        registerFunctionParser(customFunctionParserMap, new OracleTimestampDiffFunctionParser());
        registerFunctionParser(customFunctionParserMap, new OracleNowSqlFunctionParser());
        registerFunctionParser(customFunctionParserMap, new OracleDateDiffFunctionParser());
        registerFunctionParser(customFunctionParserMap, new OracleDateAddFunctionParser());
        registerFunctionParser(customFunctionParserMap, new OracleDateFormatFunctionParser());
        registerFunctionParser(customFunctionParserMap, new OracleAddDateFunctionParser());
        registerFunctionParser(customFunctionParserMap, new OracleAddTimeFunctionParser());
        registerFunctionParser(customFunctionParserMap, new OracleCurDateFunctionParser());
        registerFunctionParser(customFunctionParserMap, new OracleCurrentDateFunctionParser());
        registerFunctionParser(customFunctionParserMap, new OracleCurrentTimeFunctionParser());
        registerFunctionParser(customFunctionParserMap, new OracleCurrentTimestampFunctionParser());
        registerFunctionParser(customFunctionParserMap, new OracleCurtimeFunctionParser());
        registerFunctionParser(customFunctionParserMap, new OracleDateDiff2FunctionParser());
        registerFunctionParser(customFunctionParserMap, new OracleDateSubFunctionParser());
        registerFunctionParser(customFunctionParserMap, new OracleDayNameFunctionParser());
        registerFunctionParser(customFunctionParserMap, new OracleDayOfMonthFunctionParser());
        registerFunctionParser(customFunctionParserMap, new OracleDayOfWeekFunctionParser());
        registerFunctionParser(customFunctionParserMap, new OracleDayOfYearFunctionParser());
        registerFunctionParser(customFunctionParserMap, new OracleExtractFunctionParser());
        registerFunctionParser(customFunctionParserMap, new OracleFromDaysFunctionParser());
        registerFunctionParser(customFunctionParserMap, new OracleHourFunctionParser());
        registerFunctionParser(customFunctionParserMap, new OracleLastDayFunctionParser());
        registerFunctionParser(customFunctionParserMap, new OracleLocaltimestampFunctionParser());
        registerFunctionParser(customFunctionParserMap, new OracleLocaltimeFunctionParser());
        registerFunctionParser(customFunctionParserMap, new OracleMakeTimeFunctionParser());
        registerFunctionParser(customFunctionParserMap, new OracleMicrosecondFunctionParser());
        registerFunctionParser(customFunctionParserMap, new OracleMinuteFunctionParser());
        registerFunctionParser(customFunctionParserMap, new OracleMonthNameFunctionParser());
        registerFunctionParser(customFunctionParserMap, new OracleMonthFunctionParser());
        registerFunctionParser(customFunctionParserMap, new OracleQuarterFunctionParser());
        registerFunctionParser(customFunctionParserMap, new OracleSecondFunctionParser());
        registerFunctionParser(customFunctionParserMap, new OracleSubDateFunctionParser());
        registerFunctionParser(customFunctionParserMap, new OracleSubTimeFunctionParser());
        registerFunctionParser(customFunctionParserMap, new OracleSysDateFunctionParser());
        registerFunctionParser(customFunctionParserMap, new OracleTimeFormatFunctionParser());
        registerFunctionParser(customFunctionParserMap, new OracleToDaysFunctionParser());
        registerFunctionParser(customFunctionParserMap, new OracleWeekFunctionParser());
        registerFunctionParser(customFunctionParserMap, new OracleWeekDayFunctionParser());
        registerFunctionParser(customFunctionParserMap, new OracleWeekOfYearFunctionParser());
        registerFunctionParser(customFunctionParserMap, new OracleYearFunctionParser());
        registerFunctionParser(customFunctionParserMap, new OracleYearWeekFunctionParser());
        registerFunctionParser(customFunctionParserMap, new OracleSplitFunctionParser());
    }

    private BaseOracleDatabaseInfo database;


    public OracleSqlParseHandler(BaseOracleDatabaseInfo database) {
        super(new KeywordConstant());
        this.database = database;
    }

    @Override
    protected String sqlIntervalOperatorParse(SqlIntervalOperator operator, List<SqlNode> operandList) {
        SqlNode sqlNode0 = operandList.get(0);

        String number = sqlNodeParse(sqlNode0);

        String timeUnit = sqlNodeParse(operandList.get(1));

        String expr = null;

        if (!StringUtils.isNumeric(StringUtils.startsWith(number, "-") ? StringUtils.substringAfter(number, "-") : number)) {
            expr = number;
            number = "1";
        }

        if (StringUtils.isNotBlank(expr)) {
            if(isChildQuery(sqlNode0) || isAssemblyOperation(sqlNode0)) {
                expr = LEFT_BRACKET + expr + RIGHT_BRACKET;
            }
            return  operator.getName() + BLANK_SPACE + SINGLE_QUOTATION_MARKS + number + SINGLE_QUOTATION_MARKS + BLANK_SPACE + timeUnit + " * " + expr;
        } else {
            return operator.getName() + BLANK_SPACE + SINGLE_QUOTATION_MARKS + number + SINGLE_QUOTATION_MARKS + BLANK_SPACE + timeUnit;
        }
    }

    @Override
    protected SqlFunctionParser getCustomFuncHandler(String funcName) {

        SqlFunctionParser sqlFunctionParser = customFunctionParserMap.get(funcName);
        if(sqlFunctionParser != null) {
            return sqlFunctionParser;
        }

        return super.getCustomFuncHandler(funcName);
    }

    @Override
    public PrecompileSqlStatement createPrecompileSqlStatement(String sql) {
        return new OraclePrecompileSqlStatement(sql, limitOffsets, dynamicParamAccumulator);
    }

    /**
     * pg数据库字段，表名是区分大小写的
     * pg数据库操作时，表名前需要加上schema
     * @param from
     * @return
     */
    @Override
    protected String getTableName(SqlNode from) {

        //直接表名
        if(from instanceof SqlIdentifier)  {
            return _getTableName((SqlIdentifier) from);
        }

        //表名取了别名
        if(from instanceof SqlBasicCall) {
            return _getTableAliasName((SqlBasicCall) from);
        }

        return super.getTableName(from);
    }

    private String _getTableAliasName(SqlBasicCall sqlBasicCall) {

        List<SqlNode> operandList = sqlBasicCall.getOperandList();
        SqlNode sqlNode0 = operandList.get(0);
        //表名直接取别名
        if(sqlNode0 instanceof SqlIdentifier) {
            String tableName = _getTableName((SqlIdentifier) sqlNode0);
            SqlNode sqlNode1 = operandList.get(1);
            String alias = sqlNodeParse(sqlNode1);
//            String as = sqlBasicCall.getOperator().getName();
            return tableName + BLANK_SPACE + alias;
        }


        //放行不做处理
        return sqlBasicCallParse(sqlBasicCall);
    }

    private String _getTableName(SqlIdentifier sqlIdentifier) {
        String tableName = sqlIdentifierParse(sqlIdentifier);

        //如果已经存在schema，不用再加schema
        if(tableName.contains(".")) {
            return tableName;
        }

        //不区分大小写
        //不区分大小写
        if(database.isIgnoreCase()) {

            String schema = database.getSchema();
            // 特殊字符，数字开头
            if (containSpecialCharacter(tableName)) {
                tableName = DOUBLE_QUOTATION_MARKS + tableName + DOUBLE_QUOTATION_MARKS;
            }
            if (containSpecialCharacter(schema)) {
                schema = DOUBLE_QUOTATION_MARKS + schema + DOUBLE_QUOTATION_MARKS;
            }
            return schema + "." + tableName;
        }else {
            //区分大小写
            return DOUBLE_QUOTATION_MARKS + database.getSchema() + DOUBLE_QUOTATION_MARKS + "." + tableName;
        }
    }

    private boolean containSpecialCharacter(String str) {
        // 特殊字符，数字开头
        if (StringUtils.containsIgnoreCase(str, "-")
                || StringUtils.isNumeric(StringUtils.substring(str, 0, 1))) {
            return true;
        }
        return false;
    }


    /**
     * 取别名操作
     * @param operator
     * @param operandList
     * @return
     */
    @Override
    protected String sqlAsOperatorParse(SqlAsOperator operator, List<SqlNode> operandList) {
        String as = operator.getName();

        SqlNode sqlNode0 = operandList.get(0);

        String pre = sqlNodeParse(sqlNode0);

        String post = sqlNodeParse(operandList.get(1));

        if(isChildQuery(sqlNode0)) {
            return LEFT_BRACKET + pre + RIGHT_BRACKET + BLANK_SPACE + post;
        }

        return pre + BLANK_SPACE + as + BLANK_SPACE + post;
    }

    /**
     * pg数据库字段，表名是区分大小写的
     * @param sqlIdentifier
     * 包括字段，表名
     * @return
     */
    @Override
    protected String sqlIdentifierParse(SqlIdentifier sqlIdentifier) {
        StringBuilder sb = new StringBuilder();
        ImmutableList<String> names = sqlIdentifier.names;

        boolean contains = sqlIdentifier.toString().contains("*");

        for (int i = 0; i < names.size(); i++) {

            if(i != 0) {
                sb.append(".");
            }

            String name = names.get(i);

            //*字段需要特殊处理，因为*字段时是空字符串
            if(contains && StringUtils.isBlank(name)) {
                name = "*";
            }

            //区分大小写时，*字符串不做任何处理
            if(!database.isIgnoreCase() && !name.equals("*")){
                name = DOUBLE_QUOTATION_MARKS + name + DOUBLE_QUOTATION_MARKS;
            } else if(keyWordHandler.isKeyWord(name))  {
                //关键字处理
                name = keyWordHandler.handler(name);
            }

            sb.append(name);
        }
        return sb.toString();
    }

    @Override
    protected String sqlDynamicParamParse(SqlDynamicParam sqlDynamicParam) {
        dynamicParamAccumulator++;
        return super.sqlDynamicParamParse(sqlDynamicParam);
    }

    @Override
    protected String pageParse(String sql, String offset, String fetch) {

        if(offset == null && fetch == null) {
            return sql;
        }

        if(offset == null) {
            offset = "0";
        }

        if (fetch != null) {
            LimitOffset limitOffset = new LimitOffset();
            //计算分页位置
            if("?".equalsIgnoreCase(offset) && "?".equalsIgnoreCase(fetch)) {
                //已占位 limit ? offset ?
                int limitPos = dynamicParamAccumulator - 2;
                int offsetPos = dynamicParamAccumulator - 1;
                limitOffset.limitPos = limitPos;
                limitOffset.offsetPos = offsetPos;
            }else if(!"?".equalsIgnoreCase(offset) && !"?".equalsIgnoreCase(fetch)) {
                //没有占位 limit 10 offset 0;
                dynamicParamAccumulator++;
                dynamicParamAccumulator++;
                int limitPos = dynamicParamAccumulator - 2;
                int offsetPos = dynamicParamAccumulator - 1;
                limitOffset.limitPos = limitPos;
                limitOffset.offsetPos = offsetPos;
                limitOffset.hasLimitValue = true;
                limitOffset.hasOffsetValue = true;
                limitOffset.offsetValue = Integer.parseInt(offset);
                limitOffset.limitValue = Integer.parseInt(fetch);

            }else if("?".equalsIgnoreCase(offset) && !"?".equalsIgnoreCase(fetch)) {
                //部分占位 limit 10 offset ?
                dynamicParamAccumulator++;
                int limitPos = dynamicParamAccumulator - 2;
                int offsetPos = dynamicParamAccumulator - 1;
                limitOffset.limitPos = limitPos;
                limitOffset.offsetPos = offsetPos;
                limitOffset.hasLimitValue = true;
                limitOffset.limitValue = Integer.parseInt(fetch);
            }else if(!"?".equalsIgnoreCase(offset) && "?".equalsIgnoreCase(fetch)) {
                //部分占位 limit ? offset 0
                dynamicParamAccumulator++;
                int limitPos = dynamicParamAccumulator - 2;
                int offsetPos = dynamicParamAccumulator - 1;
                limitOffset.limitPos = limitPos;
                limitOffset.offsetPos = offsetPos;
                limitOffset.hasOffsetValue = true;
                limitOffset.offsetValue = Integer.parseInt(offset);
            }

            limitOffsets.add(limitOffset);
            return String.format(PAGE_TEMPLATE, sql);
        }

        return sql;
    }

    @Data
    static class LimitOffset {

        private int limitPos;

        private int limitValue;

        private boolean hasLimitValue;

        private int offsetPos;

        private int offsetValue;

        private boolean hasOffsetValue;
    }
}
