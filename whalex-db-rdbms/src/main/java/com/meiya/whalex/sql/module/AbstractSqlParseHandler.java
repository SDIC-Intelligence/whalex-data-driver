package com.meiya.whalex.sql.module;

import cn.hutool.core.collection.CollectionUtil;
import com.google.common.collect.ImmutableList;
import com.meiya.whalex.DatSqlOrderBy;
import com.meiya.whalex.keyword.KeyWordHandler;
import com.meiya.whalex.sql.function.CastSqlFunctionParser;
import com.meiya.whalex.sql.function.ExtractSqlFunctionParser;
import com.meiya.whalex.sql.function.GroupConcatSqlFunctionParser;
import com.meiya.whalex.sql.function.JsonArraySqlFunctionParser;
import com.meiya.whalex.sql.function.JsonObjectSqlFunctionParser;
import com.meiya.whalex.sql.function.JsonQuerySqlFunctionParser;
import com.meiya.whalex.sql.function.PositionSqlFunctionParser;
import com.meiya.whalex.sql.function.SqlFunctionParser;
import com.meiya.whalex.sql.function.TrimSqlFunctionParser;
import com.meiya.whalex.sql.impl.DatSqlParserImpl;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.ddl.SqlCreateView;
import org.apache.calcite.sql.ddl.SqlDropView;
import org.apache.calcite.sql.fun.*;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.NlsString;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public abstract class AbstractSqlParseHandler implements SqlParseHandler{

    protected static final String BLANK_SPACE = " ";

    protected static final String LEFT_BRACKET = "(";

    protected static final String RIGHT_BRACKET = ")";

    protected static SqlParser.Config config = SqlParser.configBuilder()
            .setQuotedCasing(Casing.UNCHANGED)
            .setUnquotedCasing(Casing.UNCHANGED)
            .setParserFactory(DatSqlParserImpl.FACTORY)
            .setQuoting(Quoting.BACK_TICK)
            .build();

    /**
     * 函数解析器容器
     */
    private static Map<String, SqlFunctionParser> functionParserMap = new ConcurrentHashMap<>();

    /**
     * 注册函数解析器
     */
    static {
        registerFunctionParser(functionParserMap, new JsonObjectSqlFunctionParser());
        registerFunctionParser(functionParserMap, new JsonArraySqlFunctionParser());
        registerFunctionParser(functionParserMap, new JsonQuerySqlFunctionParser());
        registerFunctionParser(functionParserMap, new GroupConcatSqlFunctionParser());
        registerFunctionParser(functionParserMap, new TrimSqlFunctionParser());
        registerFunctionParser(functionParserMap, new CastSqlFunctionParser());
        registerFunctionParser(functionParserMap, new PositionSqlFunctionParser());
        registerFunctionParser(functionParserMap, new ExtractSqlFunctionParser());
    }

    protected KeyWordHandler keyWordHandler;

    public AbstractSqlParseHandler(KeyWordHandler keyWordHandler) {
        this.keyWordHandler = keyWordHandler;
    }

    /**
     * 注册自定义函数解析
     *
     * @param functionParser
     */
    protected static void registerFunctionParser(Map<String, SqlFunctionParser> functionParserMap, SqlFunctionParser functionParser) {
        com.meiya.whalex.sql.annotation.SqlFunction annotation = functionParser.getClass().getAnnotation(com.meiya.whalex.sql.annotation.SqlFunction.class);
        if (annotation != null) {
            String functionName = annotation.functionName();
            if (StringUtils.isBlank(functionName)) {
                log.warn("Sql Function Parser 注解未标注 functionName，无法解析! [{}]", functionParser.getClass().getName());
            }
            functionParserMap.put(functionName, functionParser);
        } else {
            log.warn("Sql Function Parser 未标注注解，无法解析! [{}]", functionParser.getClass().getName());
        }
    }


    @Override
    public PrecompileSqlStatement executeSqlParse(String sql) {
        return createPrecompileSqlStatement(querySqlParse(sql));
    }

    /**
     * 创建预编译sql结果
     * @param sql
     * @return
     */
    public abstract PrecompileSqlStatement createPrecompileSqlStatement(String sql);

    protected String querySqlParse(String sql) {

        SqlParser sqlParser = SqlParser.create(sql, config);
        SqlNode sqlNode = null;
        try {
            sqlNode = sqlParser.parseStmt();
        } catch (SqlParseException e) {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        }

        String newSql = sqlNodeParse(sqlNode);
        return newSql;
    }

    /**
     *
     * @param sqlNodeList
     * @return
     */
    protected String sqlNodeListParse(SqlNodeList sqlNodeList) {

        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < sqlNodeList.size(); i++) {
            if(i != 0) {
                sb.append(", ");
            }
            sb.append(sqlNodeParse(sqlNodeList.get(i)));
        }

        return sb.toString();
    }


    protected String sqlNodeParse(SqlNode sqlNode) {

        if(sqlNode instanceof SqlBasicCall)  {
            return sqlBasicCallParse((SqlBasicCall) sqlNode);
        }

        if(sqlNode instanceof SqlSelect) {
            return sqlSelectParse((SqlSelect) sqlNode);
        }

        if(sqlNode instanceof SqlNodeList) {
            return sqlNodeListParse((SqlNodeList) sqlNode);
        }

        if(sqlNode instanceof SqlLiteral) {
            return sqlLiteralParse((SqlLiteral) sqlNode);
        }

        if(sqlNode instanceof SqlIdentifier) {
            return sqlIdentifierParse((SqlIdentifier) sqlNode);
        }

        if(sqlNode instanceof SqlJoin) {
            return sqlJoinParse((SqlJoin)sqlNode);
        }

        if(sqlNode instanceof SqlCase) {
            return sqlCaseParse((SqlCase)sqlNode);
        }

        if(sqlNode instanceof SqlOrderBy) {
            return sqlOrderByParse((SqlOrderBy)sqlNode);
        }

        if(sqlNode instanceof SqlIntervalQualifier)  {
            return sqlIntervalQualifierParse((SqlIntervalQualifier)sqlNode);
        }

        if(sqlNode instanceof SqlDynamicParam) {
            return sqlDynamicParamParse((SqlDynamicParam)sqlNode);
        }

        if(sqlNode instanceof SqlUpdate) {
            return sqlUpdateParse((SqlUpdate)sqlNode);
        }

        if(sqlNode instanceof SqlDataTypeSpec) {
            return sqlDataTypeSpecParse((SqlDataTypeSpec)sqlNode);
        }

        if(sqlNode instanceof SqlDelete) {
            return sqlDeleteParse((SqlDelete)sqlNode);
        }

        if(sqlNode instanceof SqlInsert) {
            return sqlInsertParse((SqlInsert)sqlNode);
        }

        if(sqlNode instanceof SqlCreateView) {
            return sqlCreateViewParse((SqlCreateView)sqlNode);
        }

        if(sqlNode instanceof SqlDropView) {
            return sqlDropViewParse((SqlDropView)sqlNode);
        }

        if (sqlNode instanceof SqlWindow) {
            return sqlWindowParse((SqlWindow)sqlNode);
        }

        throw new RuntimeException("未知的类型：" + sqlNode.getClass().getSimpleName());
    }

    /**
     * 窗口函数处理
     *
     * @param sqlNode
     * @return
     */
    private String sqlWindowParse(SqlWindow sqlNode) {
        SqlNodeList partitionList = sqlNode.getPartitionList();
        SqlNodeList orderList = sqlNode.getOrderList();
        String partitionFragment = sqlNodeListParse(partitionList);
        String orderFragment = sqlNodeListParse(orderList);

        StringBuilder windowSqlSb = new StringBuilder();
        if (StringUtils.isNotBlank(partitionFragment)) {
            windowSqlSb.append("PARTITION BY ").append(partitionFragment);
        }

        if(StringUtils.isNotBlank(orderFragment)) {
            windowSqlSb.append(" ORDER BY ").append(orderFragment);
        }
        return windowSqlSb.toString();
    }

    /**
     * 删除视图
     * @param sqlDropView
     * @return
     */
    private String sqlDropViewParse(SqlDropView sqlDropView) {
        String tableName = getTableName(sqlDropView.name);
        return "DROP VIEW " + tableName;
    }

    /**
     * 新建视图
     * @param sqlCreateView
     * @return
     */
    private String sqlCreateViewParse(SqlCreateView sqlCreateView) {

        StringBuilder sb = new StringBuilder();
        String tableName = getTableName(sqlCreateView.name);
        SqlNodeList columnList = sqlCreateView.columnList;
        boolean replace = sqlCreateView.getReplace();

        if (replace) {
            sb.append("CREATE OR REPLACE VIEW");
        } else {
            sb.append("CREATE VIEW");
        }

        sb.append(BLANK_SPACE).append(tableName);
        if(CollectionUtil.isNotEmpty(columnList)) {
            String columns = sqlNodeListParse(columnList);
            sb.append(LEFT_BRACKET).append(columns).append(RIGHT_BRACKET);
        }
        sb.append(BLANK_SPACE).append("AS").append(BLANK_SPACE).append(sqlNodeParse(sqlCreateView.query));

        return sb.toString();
    }

    /**
     * 目前只支持insert into tableName(column...) select格式
     * @param sqlNode
     * @return
     */
    private String sqlInsertParse(SqlInsert sqlNode) {

        StringBuilder sb = new StringBuilder();
        //获取表名
        String tableName = getTableName(sqlNode.getTargetTable());

        sb.append("INSERT INTO").append(BLANK_SPACE)
                .append(tableName);
        SqlNodeList targetColumnList = sqlNode.getTargetColumnList();
        if (CollectionUtil.isNotEmpty(targetColumnList)) {
            sb.append(LEFT_BRACKET)
                    .append(sqlNodeListParse(targetColumnList))
                    .append(RIGHT_BRACKET);
        }
        sb.append(BLANK_SPACE)
                .append(sqlNodeParse(sqlNode.getSource()));
        return sb.toString();
    }

    protected String sqlDeleteParse(SqlDelete sqlNode) {

        StringBuilder sb = new StringBuilder();

        //获取表名
        String tableName = getTableName(sqlNode.getTargetTable());

        sb.append("DELETE").append(BLANK_SPACE).append("FROM").append(BLANK_SPACE).append(tableName);

        //获取条件语句
        SqlNode where = sqlNode.getCondition();
        if (where != null) {
            sb.append(BLANK_SPACE).append("WHERE").append(BLANK_SPACE).append(sqlNodeParse(where));
        }

        return sb.toString();

    }

    protected String sqlUpdateParse(SqlUpdate sqlNode) {

        StringBuilder sb = new StringBuilder();

        //获取表名
        String tableName = getTableName(sqlNode.getTargetTable());

        //获取修改的列
        List<String> columns = getSqlNodeList(sqlNode.getTargetColumnList());

        //获取修改的列值
        List<String> values = getSqlNodeList(sqlNode.getSourceExpressionList());


        sb.append("UPDATE").append(BLANK_SPACE).append(tableName).append(BLANK_SPACE).append("SET").append(BLANK_SPACE);

        for (int i = 0; i < columns.size(); i++) {
            sb.append(columns.get(i)).append(" = ").append(values.get(i));

            if(i != columns.size() - 1)  {
                sb.append(", ");
            }
        }

        //where
        SqlNode where = sqlNode.getCondition();
        if (where != null) {
            sb.append(BLANK_SPACE).append("WHERE").append(BLANK_SPACE).append(sqlNodeParse(where));
        }

        return sb.toString();
    }

    /**
     * 获取集合列表
     * @param sqlNodeList
     * @return
     */
    protected List<String> getSqlNodeList(SqlNodeList sqlNodeList) {

        List<String> list = new ArrayList<>(sqlNodeList.size());

        for (SqlNode sqlNode : sqlNodeList) {
            String sqlNodeParse = sqlNodeParse(sqlNode);
            if (sqlNode instanceof SqlSelect || sqlNode instanceof SqlOrderBy) {
                sqlNodeParse = "(" + sqlNodeParse + ")";
            }
            list.add(sqlNodeParse);
        }
        return list;
    }

    /**
     * 数据类型
     * @param sqlDataTypeSpec
     * @return
     */
    protected String sqlDataTypeSpecParse(SqlDataTypeSpec sqlDataTypeSpec) {
        SqlIdentifier typeName = sqlDataTypeSpec.getTypeName();
        return typeName.names.get(0);
    }

    /**
     * ? 占位符
     * @param sqlDynamicParam
     * @return
     */
    protected String sqlDynamicParamParse(SqlDynamicParam sqlDynamicParam) {
        return "?";
    }

    /**
     * 时间单位 second, day month
     * @param sqlIntervalQualifier
     * @return
     */
    protected String sqlIntervalQualifierParse(SqlIntervalQualifier sqlIntervalQualifier) {
        return sqlIntervalQualifier.timeUnitRange.name();
    }

    /**
     * 带有排序关键字的SQL解析
     *
     * @param sqlOrderBy
     * @return
     */
    protected String sqlOrderByParse(SqlOrderBy sqlOrderBy) {
        StringBuilder sb = new StringBuilder();

        SqlNode sqlNode = sqlOrderBy.query;
        sb.append(sqlNodeParse(sqlNode));

        //order by
        SqlNodeList orderList = sqlOrderBy.orderList;

        if (CollectionUtils.isNotEmpty(orderList)) {
            sb.append(BLANK_SPACE).append("ORDER BY").append(BLANK_SPACE)
                    .append(sqlNodeListParse(orderList)).append(BLANK_SPACE);
        }

        String fetch = null;
        String offset = null;

        if (sqlOrderBy.offset != null) {
            offset = sqlNodeParse(sqlOrderBy.offset);
        }

        if (sqlOrderBy.fetch != null) {
            fetch = sqlNodeParse(sqlOrderBy.fetch);
        }

        String sql = pageParse(sb.toString().trim(), offset, fetch);

        if (sqlOrderBy instanceof DatSqlOrderBy) {
            if (((DatSqlOrderBy)sqlOrderBy).forUpdate) {
                sql = forUpdateParse(sql);
            }
        }

        return sql;
    }

    /**
     * 解析 select for update 语句
     *
     * @param sql
     * @return
     */
    protected String forUpdateParse(String sql) {
        StringBuilder sb = new StringBuilder(sql.trim());
        sb.append(BLANK_SPACE).append("FOR").append(BLANK_SPACE).append("UPDATE");
        return sb.toString();
    }

    /**
     * 分页操作
     * @param sql
     * @param offset
     * @param fetch
     * @return
     */
    protected String pageParse(String sql, String offset, String fetch) {

        StringBuilder sb = new StringBuilder();
        sb.append(sql);
        //分页参数处理
        if(StringUtils.isNotBlank(fetch)) {
            sb.append(BLANK_SPACE).append("LIMIT").append(BLANK_SPACE).append(fetch);
            if(StringUtils.isNotBlank(offset)) {
                sb.append(BLANK_SPACE).append("OFFSET").append(BLANK_SPACE).append(offset);
            }
        }

        return sb.toString();
    }

    /**
     * case 操作解析
     * @param sqlCase
     * @return
     */
    protected String sqlCaseParse(SqlCase sqlCase) {

        StringBuilder sb = new StringBuilder();
        // 操作名称
        String operaName = sqlCase.getOperator().getName();
        sb.append(operaName).append(BLANK_SPACE);
        // WHEN 操作条件
        List<SqlNode> whenList = sqlCase.getWhenOperands().getList();
        // THEN 操作结果
        List<SqlNode> thenList = sqlCase.getThenOperands().getList();
        // 拼接 WHEN 与 THEN
        for (int i = 0; i < whenList.size(); i++) {
            SqlNode whenSqlNode = whenList.get(i);
            String whenOperand = sqlNodeParse(whenSqlNode);
            sb.append("WHEN").append(BLANK_SPACE).append(whenOperand).append(BLANK_SPACE);
            SqlNode thenSqlNode = thenList.get(i);
            String thenOperand = sqlNodeParse(thenSqlNode);
            if (thenSqlNode instanceof SqlSelect) {
                // 子查询 添加 ()
                thenOperand = LEFT_BRACKET + thenOperand + RIGHT_BRACKET;
            }
            sb.append("THEN").append(BLANK_SPACE).append(thenOperand).append(BLANK_SPACE);
        }
        // 解析 ELSE 操作
        SqlNode elseOperand = sqlCase.getElseOperand();
        String callOperand = sqlNodeParse(elseOperand);
        if (!StringUtils.equalsIgnoreCase(callOperand, "NULL")) {
            sb.append(BLANK_SPACE).append("ELSE").append(BLANK_SPACE).append(callOperand);
        }
        sb.append(BLANK_SPACE).append("END");
        return sb.toString();

    }

    /**
     * 关联查询解析
     * @param sqlJoin
     * @return
     */
    protected String sqlJoinParse(SqlJoin sqlJoin) {

        StringBuilder sb = new StringBuilder();

        String left = getTableName(sqlJoin.getLeft());
        String right = getTableName(sqlJoin.getRight());

        JoinConditionType conditionType = sqlJoin.getConditionType();
        String name = conditionType.name();

        switch (name) {
            //内关联
            case "NONE":
                sb.append(left).append(", ").append(right);
                break;
            case "ON":
                String joinType = sqlJoin.getJoinType().name();
                SqlNode condition = sqlJoin.getCondition();

                sb.append(left).append(BLANK_SPACE).append(joinType)
                        .append(BLANK_SPACE).append("JOIN")
                        .append(BLANK_SPACE).append(right)
                        .append(BLANK_SPACE).append("ON")
                        .append(BLANK_SPACE).append(sqlNodeParse(condition));
                break;
        }
        return sb.toString();
    }



    protected String sqlBasicCallParse(SqlBasicCall sqlBasicCall) {

        SqlOperator operator = sqlBasicCall.getOperator();

        List<SqlNode> operandList = sqlBasicCall.getOperandList();

        if(operator instanceof SqlAsOperator) {
            return sqlAsOperatorParse((SqlAsOperator)operator, operandList);
        }

        if(operator instanceof SqlFunction) {
            return sqlFunctionParse(operator, operandList, sqlBasicCall.getFunctionQuantifier());
        }

        if(operator instanceof SqlBinaryOperator) {
            return sqlBinaryOperatorParse((SqlBinaryOperator)operator, operandList);
        }

        if(operator instanceof SqlPostfixOperator) {
            return sqlPostfixOperatorParse((SqlPostfixOperator)operator, operandList);
        }

        if(operator instanceof SqlIntervalOperator) {
            return sqlIntervalOperatorParse((SqlIntervalOperator)operator, operandList);
        }

        if(operator instanceof SqlBetweenOperator) {
            return sqlBetweenOperatorParse((SqlBetweenOperator)operator, operandList);
        }

        if(operator instanceof SqlInternalOperator) {
            return sqlInternalOperatorParse((SqlInternalOperator)operator, operandList);
        }

        if(operator instanceof SqlPrefixOperator) {
            return sqlPrefixOperatorParse((SqlPrefixOperator)operator, operandList);
        }

        if(operator instanceof SqlLikeOperator) {
            return sqlLikeOperatorParse((SqlLikeOperator)operator, operandList);
        }

        if (operator instanceof SqlRowOperator) {
            return sqlRowOperatorParse((SqlRowOperator) operator, operandList);
        }

        throw  new RuntimeException("未知的操作类型：" + operator.getClass().getSimpleName());
    }

    private String sqlLikeOperatorParse(SqlLikeOperator operator, List<SqlNode> operandList) {
        String operatorName = operator.getName();
        String pre = sqlNodeParse(operandList.get(0));
        String post = sqlNodeParse(operandList.get(1));
        return pre + BLANK_SPACE + operatorName + BLANK_SPACE + post;
    }

    /**
     * WHERE (a, b) IN ((xx, xx), (xx, xx)) 语法解析
     *
     * @param operator
     * @param operandList
     * @return
     */
    private String sqlRowOperatorParse(SqlRowOperator operator, List<SqlNode> operandList) {
        StringBuilder sb = new StringBuilder(LEFT_BRACKET);
        for (SqlNode node : operandList) {
            String name = sqlNodeParse(node);
            sb.append(name).append(",");
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.append(RIGHT_BRACKET);
        return sb.toString();
    }

    /**
     * - 1 等操作
     * @param operator
     * @param operandList
     * @return
     */
    protected String sqlPrefixOperatorParse(SqlPrefixOperator operator, List<SqlNode> operandList) {
        SqlNode sqlNode = operandList.get(0);
        String field = sqlNodeParse(sqlNode);
        // 判断是否为 - 号加数字
        if (operator.getKind() == SqlKind.MINUS_PREFIX && sqlNode.getKind() == SqlKind.LITERAL) {
            return  operator.getName() + field;
        }
        // 子SQL 中不包含 AND 操作符
        if(sqlNode.getKind() != SqlKind.AND
                // 操作符为 EXISTS 并且 子SQL 为 子表查询的则需要 ()
                && !(SqlKind.EXISTS.equals(operator.getKind()) && sqlNode instanceof SqlSelect)) {
            return  operator.getName() + BLANK_SPACE + field;
        }
        return  operator.getName() + BLANK_SPACE + LEFT_BRACKET + field + RIGHT_BRACKET;
    }

    /**
     * SEPARATOR 操作
     * @param operator
     * @param operandList
     * @return
     */
    protected String sqlInternalOperatorParse(SqlInternalOperator operator, List<SqlNode> operandList) {
        String separator = sqlNodeParse(operandList.get(0));
        return operator.getName() + BLANK_SPACE + separator;
    }

    /**
     * 区间操作
     * @param operator
     * @param operandList
     * @return
     */
    protected String sqlBetweenOperatorParse(SqlBetweenOperator operator, List<SqlNode> operandList) {
        String field = sqlNodeParse(operandList.get(0));
        String param1 = sqlNodeParse(operandList.get(1));
        String param2 = sqlNodeParse(operandList.get(2));
        return field + BLANK_SPACE + "BETWEEN" + BLANK_SPACE + param1 + BLANK_SPACE + "AND" + BLANK_SPACE + param2;
    }


    /**
     * INTERVAL操作
     * @param operator
     * @param operandList
     * @return
     */
    protected String sqlIntervalOperatorParse(SqlIntervalOperator operator, List<SqlNode> operandList) {

        SqlNode sqlNode0 = operandList.get(0);

        String number = sqlNodeParse(sqlNode0);

        String timeUnit = sqlNodeParse(operandList.get(1));

        if(isChildQuery(sqlNode0) || isAssemblyOperation(sqlNode0)) {
            return operator.getName() + BLANK_SPACE + LEFT_BRACKET + number + RIGHT_BRACKET + BLANK_SPACE + timeUnit;
        }

        return  operator.getName() + BLANK_SPACE  + number  + BLANK_SPACE + timeUnit;
    }

    /**
     * 判断是否组合操作
     *
     * @param sqlNode
     * @return
     */
    protected boolean isAssemblyOperation(SqlNode sqlNode) {
        if (sqlNode instanceof SqlBasicCall) {
            SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlNode;
            List<SqlNode> operandList = sqlBasicCall.getOperandList();
            return operandList.size() > 1;
        }
        return false;
    }


    /**
     * NULLS LAST
     * DESC
     * IS NOT NULL
     * IS NULL 等参数操作
     * @param operator
     * @param operandList
     * @return
     */
    protected String sqlPostfixOperatorParse(SqlPostfixOperator operator, List<SqlNode> operandList) {
        String field = sqlNodeParse(operandList.get(0));
        return field + BLANK_SPACE + operator.getName();
    }

    /**
     * = < > and or in 运算符操作
     * @param operator
     * @param operandList
     * @return
     */
    protected String sqlBinaryOperatorParse(SqlBinaryOperator operator, List<SqlNode> operandList) {
        String operatorName = operator.getName();
        SqlNode sqlNode = operandList.get(0);
        SqlNode sqlNode1 = operandList.get(1);
        String pre = sqlNodeParse(sqlNode);
        String post = sqlNodeParse(sqlNode1);

        //in,not int, some/any操作需要加括号
        if(operator.kind == SqlKind.IN || operator.kind == SqlKind.NOT_IN || operator.kind == SqlKind.SOME) {
            return pre + BLANK_SPACE + operatorName + BLANK_SPACE + LEFT_BRACKET + post + RIGHT_BRACKET;
        }

        //or操作需要加括号
        if(operator.kind == SqlKind.OR) {
            return LEFT_BRACKET +  pre + BLANK_SPACE + operatorName + BLANK_SPACE + post + RIGHT_BRACKET;
        }

        //如果是子查询，而且不是union操作需要加括号
        if(!(operator.kind == SqlKind.UNION)
                && isChildQuery(operandList.get(1))) {
            return pre + BLANK_SPACE + operatorName + BLANK_SPACE + LEFT_BRACKET + post + RIGHT_BRACKET;
        }else if(!(operator.kind == SqlKind.UNION) && isChildQuery(operandList.get(0))) {
            return LEFT_BRACKET + pre + RIGHT_BRACKET + BLANK_SPACE + operatorName + BLANK_SPACE + post ;
        }


        if(isNeedBracket(operatorName, sqlNode, false)) {
          pre = LEFT_BRACKET +  pre + RIGHT_BRACKET;
        }

        if(isNeedBracket(operatorName, sqlNode1, true)) {
            post = LEFT_BRACKET +  post + RIGHT_BRACKET;
        }

        return pre + BLANK_SPACE + operatorName + BLANK_SPACE + post;
    }

    //是否需要加括号
    private boolean isNeedBracket(String operatorName, SqlNode sqlNode, boolean isPost) {
        if (sqlNode instanceof SqlWindow) {
            return true;
        }
        if(!(sqlNode instanceof SqlBasicCall)) {
            return false;
        }
        SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlNode;
        String name = sqlBasicCall.getOperator().getName();
        //四则运算 如果是在运算符后面 都需要加括号
        if(isPost) {
            if("*".equalsIgnoreCase(operatorName) || "/".equalsIgnoreCase(operatorName)) {
                if("-".equalsIgnoreCase(name) || "+".equalsIgnoreCase(name)
                        || "*".equalsIgnoreCase(operatorName) || "/".equalsIgnoreCase(operatorName)) {
                    return true;
                }
            }
        }else{
            if("*".equalsIgnoreCase(operatorName) || "/".equalsIgnoreCase(operatorName)) {
                if("-".equalsIgnoreCase(name) || "+".equalsIgnoreCase(name)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * 函数处理
     * 函数的格式 func(a, b, c, ..) 或者 func()
     * @param operator
     * @param operandList 参数
     * @param functionQuantifier 关键字修饰参数， 如count(distinct name)
     * @return
     */
    protected String sqlFunctionParse(SqlOperator operator, List<SqlNode> operandList, SqlLiteral functionQuantifier) {

        //函数名
        String funcName = operator.getName();

        //参数
        List<String> paramList = new ArrayList<>(operandList.size());
        for (int i = 0; i < operandList.size(); i++) {
            SqlNode sqlNode = operandList.get(i);
            //参数是子查询，而且函数参数大于一个时，需要对子查询加括号
            if(isChildQuery(sqlNode) && operandList.size() > 1) {
                paramList.add(LEFT_BRACKET +  sqlNodeParse(sqlNode) + RIGHT_BRACKET);
            }else {
                paramList.add(sqlNodeParse(sqlNode));
            }
        }

        //关键字修饰参数
        String quantifier = null;
        if(functionQuantifier != null) {
            quantifier = sqlLiteralParse(functionQuantifier);
        }

        //获取用户自定义函数解析器
        SqlFunctionParser customFuncHandler = getCustomFuncHandler(funcName.toUpperCase());
        if(customFuncHandler != null) {
            return customFuncHandler.parseFunc(funcName.toUpperCase(),  paramList, quantifier);
        }

        //函数默认处理
        return parseFunc(funcName, paramList, quantifier);
    }

    /**
     * 此方法不可继承, 需要自定义函数入口在 getCustomFuncHandler(String funcName)
     * @param funcName
     * @param operandList
     * @param functionQuantifier
     * @return
     */
    private String parseFunc(String funcName, List<String> operandList, String functionQuantifier) {

        StringBuilder params = new StringBuilder();
        for (int i = 0; i < operandList.size(); i++) {
            if(i != 0) {
                params.append(", ");
            }
            params.append(operandList.get(i));
        }

        //存在关键字修饰参数
        if(functionQuantifier != null) {
            return funcName + LEFT_BRACKET + functionQuantifier + BLANK_SPACE + params.toString() + RIGHT_BRACKET;
        }

        return funcName + LEFT_BRACKET + params.toString() + RIGHT_BRACKET;
    }

    protected SqlFunctionParser getCustomFuncHandler(String funcName) {
        return functionParserMap.get(funcName);
    }

    /**
     * 取别名操作
     * @param operator
     * @param operandList
     * @return
     */
    protected String sqlAsOperatorParse(SqlAsOperator operator, List<SqlNode> operandList) {
        String as = operator.getName();

        SqlNode sqlNode0 = operandList.get(0);

        String pre = sqlNodeParse(sqlNode0);

        String post = sqlNodeParse(operandList.get(1));

        if(isChildQuery(sqlNode0)) {
            return LEFT_BRACKET + pre + RIGHT_BRACKET + BLANK_SPACE + as + BLANK_SPACE + post;
        }

        return pre + BLANK_SPACE + as + BLANK_SPACE + post;
    }

    protected boolean isChildQuery(SqlNode sqlNode) {

        SqlKind kind = sqlNode.getKind();

        if(kind == SqlKind.SELECT || kind == SqlKind.ORDER_BY || kind == SqlKind.UNION) {
            return true;
        }

        return false;
    }



    /**
     *
     * @param sqlIdentifier
     * 包括字段，表名
     * @return
     */
    protected String sqlIdentifierParse(SqlIdentifier sqlIdentifier) {
        StringBuilder sb = new StringBuilder();
        ImmutableList<String> names = sqlIdentifier.names;

        boolean contains = sqlIdentifier.toString().contains("*");

        for (int i = 0; i < names.size(); i++) {

            if(i != 0) {
                sb.append(".");
            }

            String name = names.get(i);

            //关键字处理
            if(keyWordHandler.isKeyWord(name))  {
                name = keyWordHandler.handler(name);
            }

            //*字段需要特殊处理，因为*字段时是空字符串
            if(contains && StringUtils.isBlank(name)) {
                name = "*";
            }
            sb.append(name);
        }
        return sb.toString();
    }

    /**
     *
     * @param sqlLiteral
     * 参数处理
     * @return
     */
    protected String sqlLiteralParse(SqlLiteral sqlLiteral) {


        Object value = sqlLiteral.getValue();

        //字符串
        if(value instanceof NlsString) {
            NlsString nlsString = (NlsString) value;
            return "'" + nlsString.getValue() + "'";
        }

        //空
        if(sqlLiteral.getTypeName() == SqlTypeName.NULL){
            return "NULL";
        }

        //Json参数
        if (value instanceof SqlJsonConstructorNullClause) {
            SqlJsonConstructorNullClause sqlJsonConstructorNullClause = (SqlJsonConstructorNullClause) value;
            return sqlJsonConstructorNullClause.sql;
        }
        if (value instanceof SqlJsonQueryEmptyOrErrorBehavior) {
            SqlJsonQueryEmptyOrErrorBehavior sqlJsonQueryEmptyOrErrorBehavior = (SqlJsonQueryEmptyOrErrorBehavior) value;
            return sqlJsonQueryEmptyOrErrorBehavior.name();
        }

        //INTERVAL参数 interval '-1' MONTH
        if (value instanceof SqlIntervalLiteral.IntervalValue) {
            SqlIntervalLiteral.IntervalValue intervalValue = (SqlIntervalLiteral.IntervalValue) value;
            String intervalLiteral = intervalValue.getIntervalLiteral();
            String timeUnit = sqlIntervalQualifierParse(intervalValue.getIntervalQualifier());
            value = "INTERVAL '" + intervalLiteral + "' " + timeUnit;
        }

        //默认，包括数字, 布尔等
        return value.toString();
    }

    /**
     *
     * @param sqlSelect
     * @return
     */
    protected String sqlSelectParse(SqlSelect sqlSelect) {

        StringBuilder sb = new StringBuilder();

        //select
        sb.append("SELECT").append(BLANK_SPACE);

        // SqlSelect没有提供直接获取keywordList的方法，所以需要用此方法获取 distinct等关键字
        SqlNodeList keywordList = (SqlNodeList) sqlSelect.getOperandList().get(0);
        if (CollectionUtil.isNotEmpty(keywordList)) {
            sb.append(sqlNodeListParse(keywordList)).append(BLANK_SPACE);
        }

        //字段
        SqlNodeList selectList = sqlSelect.getSelectList();
        sb.append(sqlNodeListParse(selectList)).append(BLANK_SPACE);

        //from
        SqlNode from = sqlSelect.getFrom();

        if (from != null) {
            sb.append("FROM").append(BLANK_SPACE).append(getTableName(from)).append(BLANK_SPACE);
        }

        //where
        SqlNode where = sqlSelect.getWhere();
        if (where != null) {
            sb.append("WHERE").append(BLANK_SPACE).append(sqlNodeParse(where)).append(BLANK_SPACE);
        }

        //group by
        SqlNodeList group = sqlSelect.getGroup();
        if (group != null) {
            sb.append("GROUP BY").append(BLANK_SPACE).append(sqlNodeListParse(group)).append(BLANK_SPACE);
        }

        //having
        SqlNode having = sqlSelect.getHaving();
        if (having != null) {
            sb.append("HAVING").append(BLANK_SPACE).append(sqlNodeParse(having));
        }

        String result = sb.toString().trim();

        return result;
    }

    /**
     * 获取表名
     * @param from
     * @return
     */
    protected String getTableName(SqlNode from) {
        return sqlNodeParse(from);
    }
}
