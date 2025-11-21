package com.meiya.whalex.db.module.ani;

import com.google.common.collect.ImmutableList;
import com.meiya.whalex.db.entity.ani.BasePostGreDatabaseInfo;
import com.meiya.whalex.db.entity.ani.KeywordConstant;
import com.meiya.whalex.sql.function.SqlFunctionParser;
import com.meiya.whalex.sql.function.ani.PostgreAddDateFunctionParser;
import com.meiya.whalex.sql.function.ani.PostgreCastSqlFunctionParser;
import com.meiya.whalex.sql.function.ani.PostgreConcatFunctionParser;
import com.meiya.whalex.sql.function.ani.PostgreCurDateFunctionParser;
import com.meiya.whalex.sql.function.ani.PostgreCurrentDateFunctionParser;
import com.meiya.whalex.sql.function.ani.PostgreCurrentTimestampFunctionParser;
import com.meiya.whalex.sql.function.ani.PostgreCurrenttimeFunctionParser;
import com.meiya.whalex.sql.function.ani.PostgreCurtimeFunctionParser;
import com.meiya.whalex.sql.function.ani.PostgreDateAddFunctionParser;
import com.meiya.whalex.sql.function.ani.PostgreDateDiff2FunctionParser;
import com.meiya.whalex.sql.function.ani.PostgreDateDiffFunctionParser;
import com.meiya.whalex.sql.function.ani.PostgreDateFormatFunctionParser;
import com.meiya.whalex.sql.function.ani.PostgreDateSubFunctionParser;
import com.meiya.whalex.sql.function.ani.PostgreDayNameFunctionParser;
import com.meiya.whalex.sql.function.ani.PostgreDayOfMonthFunctionParser;
import com.meiya.whalex.sql.function.ani.PostgreDayOfWeekFunctionParser;
import com.meiya.whalex.sql.function.ani.PostgreDayOfYearFunctionParser;
import com.meiya.whalex.sql.function.ani.PostgreExtractFunctionParser;
import com.meiya.whalex.sql.function.ani.PostgreFormatFunctionParser;
import com.meiya.whalex.sql.function.ani.PostgreGroupConcatSqlFunctionParser;
import com.meiya.whalex.sql.function.ani.PostgreHourFunctionParser;
import com.meiya.whalex.sql.function.ani.PostgreIfFunctionParser;
import com.meiya.whalex.sql.function.ani.PostgreIfNullFunctionParser;
import com.meiya.whalex.sql.function.ani.PostgreIsNullFunctionParser;
import com.meiya.whalex.sql.function.ani.PostgreJsonArraySqlFunctionParser;
import com.meiya.whalex.sql.function.ani.PostgreJsonContainsFunctionParser;
import com.meiya.whalex.sql.function.ani.PostgreJsonExistsSqlFunctionParser;
import com.meiya.whalex.sql.function.ani.PostgreJsonObjectFunctionParser;
import com.meiya.whalex.sql.function.ani.PostgreJsonValueSqlFunctionParser;
import com.meiya.whalex.sql.function.ani.PostgreLastDayFunctionParser;
import com.meiya.whalex.sql.function.ani.PostgreLcaseFunctionParser;
import com.meiya.whalex.sql.function.ani.PostgreLocaltimeFunctionParser;
import com.meiya.whalex.sql.function.ani.PostgreLocaltimestampFunctionParser;
import com.meiya.whalex.sql.function.ani.PostgreLog10FunctionParser;
import com.meiya.whalex.sql.function.ani.PostgreLog2FunctionParser;
import com.meiya.whalex.sql.function.ani.PostgreMakeTimeFunctionParser;
import com.meiya.whalex.sql.function.ani.PostgreMicrosecondFunctionParser;
import com.meiya.whalex.sql.function.ani.PostgreMidFunctionParser;
import com.meiya.whalex.sql.function.ani.PostgreMinuteFunctionParser;
import com.meiya.whalex.sql.function.ani.PostgreMonthFunctionParser;
import com.meiya.whalex.sql.function.ani.PostgreMonthNameFunctionParser;
import com.meiya.whalex.sql.function.ani.PostgreQuarterFunctionParser;
import com.meiya.whalex.sql.function.ani.PostgreRandFunctionParser;
import com.meiya.whalex.sql.function.ani.PostgreSecondFunctionParser;
import com.meiya.whalex.sql.function.ani.PostgreSpaceFunctionParser;
import com.meiya.whalex.sql.function.ani.PostgreSplitFunctionParser;
import com.meiya.whalex.sql.function.ani.PostgreStrToDateFunctionParser;
import com.meiya.whalex.sql.function.ani.PostgreSubDateFunctionParser;
import com.meiya.whalex.sql.function.ani.PostgreSysDateFunctionParser;
import com.meiya.whalex.sql.function.ani.PostgreTimeFormatFunctionParser;
import com.meiya.whalex.sql.function.ani.PostgreTimeToSecFunctionParser;
import com.meiya.whalex.sql.function.ani.PostgreTimestampDiffFunctionParser;
import com.meiya.whalex.sql.function.ani.PostgreToDaysFunctionParser;
import com.meiya.whalex.sql.function.ani.PostgreTruncateSqlFunctionParser;
import com.meiya.whalex.sql.function.ani.PostgreUcaseFunctionParser;
import com.meiya.whalex.sql.function.ani.PostgreWeekFunctionParser;
import com.meiya.whalex.sql.function.ani.PostgreWeekOfYearFunctionParser;
import com.meiya.whalex.sql.function.ani.PostgreWeekdayFunctionParser;
import com.meiya.whalex.sql.function.ani.PostgreYearFunctionParser;
import com.meiya.whalex.sql.module.AbstractSqlParseHandler;
import com.meiya.whalex.sql.module.DefaultPrecompileSqlStatement;
import com.meiya.whalex.sql.module.PrecompileSqlStatement;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlIntervalOperator;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author 蔡荣桂
 * @date 2022/11/15
 * @project whalex-data-driver
 */
public class PostgreSqlParseHandler extends AbstractSqlParseHandler {

    public final static String DOUBLE_QUOTATION_MARKS = "\"";

    public final static String SINGLE_QUOTATION_MARKS = "'";

    /**
     * 函数解析器容器
     */
    private static Map<String, SqlFunctionParser> customFunctionParserMap = new ConcurrentHashMap<>();

    /**
     * 注册函数解析器
     */
    static {
        registerFunctionParser(customFunctionParserMap, new PostgreIfFunctionParser());
        registerFunctionParser(customFunctionParserMap, new PostgreIfNullFunctionParser());
        registerFunctionParser(customFunctionParserMap, new PostgreLcaseFunctionParser());
        registerFunctionParser(customFunctionParserMap, new PostgreUcaseFunctionParser());
        registerFunctionParser(customFunctionParserMap, new PostgreTruncateSqlFunctionParser());
        registerFunctionParser(customFunctionParserMap, new PostgreWeekFunctionParser());
        registerFunctionParser(customFunctionParserMap, new PostgreWeekOfYearFunctionParser());
        registerFunctionParser(customFunctionParserMap, new PostgreWeekdayFunctionParser());
        registerFunctionParser(customFunctionParserMap, new PostgreQuarterFunctionParser());
        registerFunctionParser(customFunctionParserMap, new PostgreMakeTimeFunctionParser());
        registerFunctionParser(customFunctionParserMap, new PostgreYearFunctionParser());
        registerFunctionParser(customFunctionParserMap, new PostgreIsNullFunctionParser());
        registerFunctionParser(customFunctionParserMap, new PostgreMonthFunctionParser());
        registerFunctionParser(customFunctionParserMap, new PostgreMinuteFunctionParser());
        registerFunctionParser(customFunctionParserMap, new PostgreLocaltimestampFunctionParser());
        registerFunctionParser(customFunctionParserMap, new PostgreLocaltimeFunctionParser());
        registerFunctionParser(customFunctionParserMap, new PostgreHourFunctionParser());
        registerFunctionParser(customFunctionParserMap, new PostgreDayOfYearFunctionParser());
        registerFunctionParser(customFunctionParserMap, new PostgreExtractFunctionParser());
        registerFunctionParser(customFunctionParserMap, new PostgreDayOfWeekFunctionParser());
        registerFunctionParser(customFunctionParserMap, new PostgreDayOfMonthFunctionParser());
        registerFunctionParser(customFunctionParserMap, new PostgreCurtimeFunctionParser());
        registerFunctionParser(customFunctionParserMap, new PostgreCurrentTimestampFunctionParser());
        registerFunctionParser(customFunctionParserMap, new PostgreCurrenttimeFunctionParser());
        registerFunctionParser(customFunctionParserMap, new PostgreCurDateFunctionParser());
        registerFunctionParser(customFunctionParserMap, new PostgreCurrentDateFunctionParser());
        registerFunctionParser(customFunctionParserMap, new PostgreRandFunctionParser());
        registerFunctionParser(customFunctionParserMap, new PostgreLog2FunctionParser());
        registerFunctionParser(customFunctionParserMap, new PostgreLog10FunctionParser());
        registerFunctionParser(customFunctionParserMap, new PostgreSpaceFunctionParser());
        registerFunctionParser(customFunctionParserMap, new PostgreMidFunctionParser());
        registerFunctionParser(customFunctionParserMap, new PostgreGroupConcatSqlFunctionParser());
        registerFunctionParser(customFunctionParserMap, new PostgreSecondFunctionParser());
        registerFunctionParser(customFunctionParserMap, new PostgreMicrosecondFunctionParser());
        registerFunctionParser(customFunctionParserMap, new PostgreMonthNameFunctionParser());
        registerFunctionParser(customFunctionParserMap, new PostgreLastDayFunctionParser());
        registerFunctionParser(customFunctionParserMap, new PostgreDateDiffFunctionParser());
        registerFunctionParser(customFunctionParserMap, new PostgreDateSubFunctionParser());
        registerFunctionParser(customFunctionParserMap, new PostgreDayNameFunctionParser());
        registerFunctionParser(customFunctionParserMap, new PostgreDateAddFunctionParser());
        registerFunctionParser(customFunctionParserMap, new PostgreSubDateFunctionParser());
        registerFunctionParser(customFunctionParserMap, new PostgreAddDateFunctionParser());
        registerFunctionParser(customFunctionParserMap, new PostgreTimeToSecFunctionParser());
        registerFunctionParser(customFunctionParserMap, new PostgreTimestampDiffFunctionParser());
        registerFunctionParser(customFunctionParserMap, new PostgreDateFormatFunctionParser());
        registerFunctionParser(customFunctionParserMap, new PostgreDateDiff2FunctionParser());
        registerFunctionParser(customFunctionParserMap, new PostgreToDaysFunctionParser());
        registerFunctionParser(customFunctionParserMap, new PostgreJsonObjectFunctionParser());
        registerFunctionParser(customFunctionParserMap, new PostgreJsonContainsFunctionParser());
        registerFunctionParser(customFunctionParserMap, new PostgreJsonArraySqlFunctionParser());
        registerFunctionParser(customFunctionParserMap, new PostgreJsonValueSqlFunctionParser());
        registerFunctionParser(customFunctionParserMap, new PostgreJsonExistsSqlFunctionParser());
        registerFunctionParser(customFunctionParserMap, new PostgreTimeFormatFunctionParser());
        registerFunctionParser(customFunctionParserMap, new PostgreStrToDateFunctionParser());
        registerFunctionParser(customFunctionParserMap, new PostgreSysDateFunctionParser());
        registerFunctionParser(customFunctionParserMap, new PostgreFormatFunctionParser());
        registerFunctionParser(customFunctionParserMap, new PostgreConcatFunctionParser());
        registerFunctionParser(customFunctionParserMap, new PostgreSplitFunctionParser());
        registerFunctionParser(customFunctionParserMap, new PostgreCastSqlFunctionParser());
    }

    private BasePostGreDatabaseInfo database;


    public PostgreSqlParseHandler(BasePostGreDatabaseInfo database) {
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
            return  operator.getName() + BLANK_SPACE + SINGLE_QUOTATION_MARKS + number  + BLANK_SPACE + timeUnit + SINGLE_QUOTATION_MARKS + " * " + expr;
        } else {
            return operator.getName() + BLANK_SPACE + SINGLE_QUOTATION_MARKS + number + BLANK_SPACE + timeUnit + SINGLE_QUOTATION_MARKS;
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
        return new DefaultPrecompileSqlStatement(sql);
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
            String as = sqlBasicCall.getOperator().getName();
            return tableName + BLANK_SPACE + as + BLANK_SPACE + alias;
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

        //如果是pg_开头的表名，不需要加schema
        if(tableName.toLowerCase().startsWith("pg_")
                || tableName.toLowerCase().startsWith("\"pg_")) {
            return tableName;
        }

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

}
