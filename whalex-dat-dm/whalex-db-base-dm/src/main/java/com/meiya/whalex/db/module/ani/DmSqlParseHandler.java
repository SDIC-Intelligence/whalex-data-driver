package com.meiya.whalex.db.module.ani;

import com.google.common.collect.ImmutableList;
import com.meiya.whalex.db.entity.ani.BaseDmDatabaseInfo;
import com.meiya.whalex.db.entity.ani.BaseDmKeywordHandler;
import com.meiya.whalex.sql.function.SqlFunctionParser;
import com.meiya.whalex.sql.function.ani.DmAddDateFunctionParser;
import com.meiya.whalex.sql.function.ani.DmAddTimeFunctionParser;
import com.meiya.whalex.sql.function.ani.DmConvertFunctionParser;
import com.meiya.whalex.sql.function.ani.DmDateAddFunctionParser;
import com.meiya.whalex.sql.function.ani.DmDateDiff2FunctionParser;
import com.meiya.whalex.sql.function.ani.DmDateDiffFunctionParser;
import com.meiya.whalex.sql.function.ani.DmDateSubFunctionParser;
import com.meiya.whalex.sql.function.ani.DmDivFunctionParser;
import com.meiya.whalex.sql.function.ani.DmFormatFunctionParser;
import com.meiya.whalex.sql.function.ani.DmFromDaysFunctionParser;
import com.meiya.whalex.sql.function.ani.DmGroupConcatSqlFunctionParser;
import com.meiya.whalex.sql.function.ani.DmIfFunctionParser;
import com.meiya.whalex.sql.function.ani.DmJsonArraySqlFunctionParser;
import com.meiya.whalex.sql.function.ani.DmJsonContainsFunctionParser;
import com.meiya.whalex.sql.function.ani.DmJsonExistsFunctionParser;
import com.meiya.whalex.sql.function.ani.DmJsonObjectFunctionParser;
import com.meiya.whalex.sql.function.ani.DmJsonQueryFunctionParser;
import com.meiya.whalex.sql.function.ani.DmJsonValueFunctionParser;
import com.meiya.whalex.sql.function.ani.DmLog2FunctionParser;
import com.meiya.whalex.sql.function.ani.DmMakeDateFunctionParser;
import com.meiya.whalex.sql.function.ani.DmMakeTimeFunctionParser;
import com.meiya.whalex.sql.function.ani.DmMicrosecondFunctionParser;
import com.meiya.whalex.sql.function.ani.DmMidFunctionParser;
import com.meiya.whalex.sql.function.ani.DmPeriodDiffFunctionParser;
import com.meiya.whalex.sql.function.ani.DmPowFunctionParser;
import com.meiya.whalex.sql.function.ani.DmSubDateFunctionParser;
import com.meiya.whalex.sql.function.ani.DmSubTimeFunctionParser;
import com.meiya.whalex.sql.function.ani.DmTimeFormatFunctionParser;
import com.meiya.whalex.sql.function.ani.DmWeekFunctionParser;
import com.meiya.whalex.sql.function.ani.DmWeekOfYearFunctionParser;
import com.meiya.whalex.sql.function.ani.DmYearWeekFunctionParser;
import com.meiya.whalex.sql.module.AbstractSqlParseHandler;
import com.meiya.whalex.sql.module.PrecompileSqlStatement;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlIntervalOperator;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DmSqlParseHandler extends AbstractSqlParseHandler {

    public final static String DOUBLE_QUOTATION_MARKS = "\"";

    public final static String SINGLE_QUOTATION_MARKS = "'";

    private BaseDmDatabaseInfo database;

    /**
     * 函数解析器容器
     */
    private static Map<String, SqlFunctionParser> customFunctionParserMap = new ConcurrentHashMap<>();

    /**
     * 注册函数解析器
     */
    static {
        registerFunctionParser(customFunctionParserMap, new DmJsonValueFunctionParser());
        registerFunctionParser(customFunctionParserMap, new DmJsonObjectFunctionParser());
        registerFunctionParser(customFunctionParserMap, new DmJsonQueryFunctionParser());
        registerFunctionParser(customFunctionParserMap, new DmJsonArraySqlFunctionParser());
        registerFunctionParser(customFunctionParserMap, new DmJsonContainsFunctionParser());
        registerFunctionParser(customFunctionParserMap, new DmGroupConcatSqlFunctionParser());
        registerFunctionParser(customFunctionParserMap, new DmIfFunctionParser());
        registerFunctionParser(customFunctionParserMap, new DmDateDiffFunctionParser());
        registerFunctionParser(customFunctionParserMap, new DmDateAddFunctionParser());
        registerFunctionParser(customFunctionParserMap, new DmConvertFunctionParser());
        registerFunctionParser(customFunctionParserMap, new DmFormatFunctionParser());
        registerFunctionParser(customFunctionParserMap, new DmLog2FunctionParser());
        registerFunctionParser(customFunctionParserMap, new DmDivFunctionParser());
        registerFunctionParser(customFunctionParserMap, new DmMidFunctionParser());
        registerFunctionParser(customFunctionParserMap, new DmPowFunctionParser());
        registerFunctionParser(customFunctionParserMap, new DmAddDateFunctionParser());
        registerFunctionParser(customFunctionParserMap, new DmDateSubFunctionParser());
        registerFunctionParser(customFunctionParserMap, new DmFromDaysFunctionParser());
        registerFunctionParser(customFunctionParserMap, new DmMakeDateFunctionParser());
        registerFunctionParser(customFunctionParserMap, new DmMakeTimeFunctionParser());
        registerFunctionParser(customFunctionParserMap, new DmSubDateFunctionParser());
        registerFunctionParser(customFunctionParserMap, new DmSubTimeFunctionParser());
        registerFunctionParser(customFunctionParserMap, new DmAddTimeFunctionParser());
        registerFunctionParser(customFunctionParserMap, new DmDateDiff2FunctionParser());
        registerFunctionParser(customFunctionParserMap, new DmWeekOfYearFunctionParser());
        registerFunctionParser(customFunctionParserMap, new DmWeekFunctionParser());
        registerFunctionParser(customFunctionParserMap, new DmYearWeekFunctionParser());
        registerFunctionParser(customFunctionParserMap, new DmJsonExistsFunctionParser());
        registerFunctionParser(customFunctionParserMap, new DmPeriodDiffFunctionParser());
        registerFunctionParser(customFunctionParserMap, new DmMicrosecondFunctionParser());
        registerFunctionParser(customFunctionParserMap, new DmTimeFormatFunctionParser());
    }


    public DmSqlParseHandler(BaseDmDatabaseInfo database) {
        super(new BaseDmKeywordHandler());
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
        return new DmPrecompileSqlStatement(sql);
    }

    /**
     * dm数据库字段，表名是区分大小写的
     * dm数据库操作时，表名前需要加上schema
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

        //如果已经有schema，不需要再加上schema
        if(tableName.contains(".")) {
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
     * dm数据库字段，表名是区分大小写的
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
