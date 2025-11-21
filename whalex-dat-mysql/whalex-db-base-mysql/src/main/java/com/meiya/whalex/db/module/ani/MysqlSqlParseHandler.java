package com.meiya.whalex.db.module.ani;

import com.meiya.whalex.db.entity.ani.BaseMysqlKeywordHandler;
import com.meiya.whalex.sql.function.SqlFunctionParser;
import com.meiya.whalex.sql.function.ani.MysqlConvertFunctionParser;
import com.meiya.whalex.sql.function.ani.MysqlDateDiffFunctionParser;
import com.meiya.whalex.sql.function.ani.MysqlDivSqlFunctionParser;
import com.meiya.whalex.sql.function.ani.MysqlJsonArraySqlFunctionParser;
import com.meiya.whalex.sql.function.ani.MysqlJsonExistsFunctionParser;
import com.meiya.whalex.sql.function.ani.MysqlJsonObjectFunctionParser;
import com.meiya.whalex.sql.function.ani.MysqlJsonQueryFunctionParser;
import com.meiya.whalex.sql.function.ani.MysqlJsonValueFunctionParser;
import com.meiya.whalex.sql.function.ani.MysqlSplitFunctionParser;
import com.meiya.whalex.sql.function.ani.MysqlToTimestampFunctionParser;
import com.meiya.whalex.sql.function.ani.MysqlTruncSqlFunctionParser;
import com.meiya.whalex.sql.module.AbstractSqlParseHandler;
import com.meiya.whalex.sql.module.PrecompileSqlStatement;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlPostfixOperator;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MysqlSqlParseHandler extends AbstractSqlParseHandler {


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
    protected static Map<String, SqlFunctionParser> customFunctionParserMap = new ConcurrentHashMap<>();

    /**
     * 注册函数解析器
     */
    static {
        registerFunctionParser(customFunctionParserMap, new MysqlJsonValueFunctionParser());
        registerFunctionParser(customFunctionParserMap, new MysqlJsonObjectFunctionParser());
        registerFunctionParser(customFunctionParserMap, new MysqlJsonQueryFunctionParser());
        registerFunctionParser(customFunctionParserMap, new MysqlJsonArraySqlFunctionParser());
        registerFunctionParser(customFunctionParserMap, new MysqlDateDiffFunctionParser());
        registerFunctionParser(customFunctionParserMap, new MysqlConvertFunctionParser());
        registerFunctionParser(customFunctionParserMap, new MysqlJsonExistsFunctionParser());
        registerFunctionParser(customFunctionParserMap, new MysqlTruncSqlFunctionParser());
        registerFunctionParser(customFunctionParserMap, new MysqlDivSqlFunctionParser());
        registerFunctionParser(customFunctionParserMap, new MysqlToTimestampFunctionParser());
        registerFunctionParser(customFunctionParserMap, new MysqlSplitFunctionParser());
    }


    public MysqlSqlParseHandler() {
        super(new BaseMysqlKeywordHandler());
        this.paramPositionSwapMap = new HashMap<>();
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
        return new MysqlPrecompileSqlStatement(sql, paramPositionSwapMap);
    }

    @Override
    protected String sqlDynamicParamParse(SqlDynamicParam sqlDynamicParam) {
        dynamicParamAccumulator++;
        return super.sqlDynamicParamParse(sqlDynamicParam);
    }

    @Override
    protected String pageParse(String sql, String offset, String fetch) {

        //当分页参数都是？时，需要记录交换两个参数的位置
        if("?".equalsIgnoreCase(offset) && "?".equalsIgnoreCase(fetch)) {
            int index1 = dynamicParamAccumulator - 2;
            int index2 = dynamicParamAccumulator - 1;
            paramPositionSwapMap.put(index1, index2);
        }

        if(offset == null) {
            offset = "0";
        }

        if (fetch != null) {
            return sql + " LIMIT " + offset + ", " + fetch;
        }

        return sql;

    }

    /**
     * order by field NULLS LAST格式
     * DESC
     * IS NOT NULL
     * IS NULL 等参数操作
     * @param operator
     * @param operandList
     * @return
     */
    @Override
    protected String sqlPostfixOperatorParse(SqlPostfixOperator operator, List<SqlNode> operandList) {
        SqlKind kind = operator.getKind();
        //order by field NULLS LAST格式
        if(kind.equals(SqlKind.NULLS_FIRST)) {
            String orderByField = getOrderByField(operandList.get(0));
            return "IF(ISNULL("+orderByField+"), 0, 1), " + sqlNodeParse(operandList.get(0));
        }else if(kind.equals(SqlKind.NULLS_LAST)) {
            String orderByField = getOrderByField(operandList.get(0));
            return "IF(ISNULL("+orderByField+"), 0, 1) DESC, " + sqlNodeParse(operandList.get(0));
        }else {
            return super.sqlPostfixOperatorParse(operator, operandList);
        }
    }

    private String getOrderByField(SqlNode sqlNode) {
        SqlKind kind = sqlNode.getKind();
        if(kind.equals(SqlKind.DESCENDING)) {
            SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlNode;
            return sqlNodeParse(sqlBasicCall.getOperandList().get(0));
        }else if(kind.equals(SqlKind.IDENTIFIER)) {
            return sqlNodeParse(sqlNode);
        }
        throw new RuntimeException("未知的kind类型："  + kind);
    }

    public static void main(String[] args) {
        MysqlSqlParseHandler mysqlSqlParseHandler = new MysqlSqlParseHandler();
        System.out.println(mysqlSqlParseHandler.executeSqlParse("select * from aa where a = 1 and b = 2 or (c = 3 and d = 4) and x = 8").getSql());
    }
}
