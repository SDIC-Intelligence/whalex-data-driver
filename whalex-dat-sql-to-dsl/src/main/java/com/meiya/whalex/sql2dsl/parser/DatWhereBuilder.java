package com.meiya.whalex.sql2dsl.parser;

import cn.hutool.core.collection.CollectionUtil;
import com.meiya.whalex.interior.db.builder.AndWhereBuilder;
import com.meiya.whalex.interior.db.builder.NestWhereBuilder;
import com.meiya.whalex.interior.db.builder.OrWhereBuilder;
import com.meiya.whalex.interior.db.search.in.Where;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.util.NlsString;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * dat条件构建
 *
 * @author 蔡荣桂
 * @date 2022/06/27
 * @project whalex-dat-sql
 */
public class DatWhereBuilder {

    private static Pattern pattern = Pattern.compile("^\\?(?<index>\\d+)$");

    private SqlNode whereSqlNode;

    private Object[] params;

    public static DatWhereBuilder create() {
        return new DatWhereBuilder();
    }

    public DatWhereBuilder whereSqlNode(SqlNode whereSqlNode, Object[] params) {
        this.whereSqlNode = whereSqlNode;
        this.params = params;
        return this;
    }

    public Where build() {

        if (whereSqlNode == null) {
            throw new RuntimeException("whereSqlNode不能为空");
        }

        SqlBasicCall sqlBasicCall = (SqlBasicCall) whereSqlNode;

        AndWhereBuilder andWhereBuilder = AndWhereBuilder.builder();
        handleWhere(sqlBasicCall, andWhereBuilder);
        Where build = andWhereBuilder.build();
        List<Where> andParams = build.getParams();
        if (CollectionUtil.isNotEmpty(andParams) && andParams.size() == 1) {
            return andParams.get(0);
        }
        return build;
    }


    private void handleWhere(SqlBasicCall sqlBasicCall, NestWhereBuilder whereBuilder) {

        String operator = sqlBasicCall.getOperator().getName();
        List<SqlNode> operandList = sqlBasicCall.getOperandList();
        SqlNode sqlNode0 = operandList.get(0);
        SqlNode sqlNode1 = operandList.get(1);

        NestWhereBuilder childWhereBuilder = null;
        if (operator.equalsIgnoreCase("and")) {
            childWhereBuilder = AndWhereBuilder.builder();
        } else if (operator.equalsIgnoreCase("or")) {
            childWhereBuilder = OrWhereBuilder.builder();
        }


        if (childWhereBuilder == null) {

            SqlIdentifier sqlIdentifier = (SqlIdentifier) sqlNode0;
            String attribute = sqlIdentifier.names.get(0);

            Object value = getValue(attribute, sqlNode1);


            // between
            if(operandList.size() > 2) {

                List<Object> values = new ArrayList<>();
                values.add(value);


                for (int i = 2; i < operandList.size(); i++) {
                    values.add(getValue(attribute, operandList.get(i)));
                }

                value = values;
            }

            handleOperator(whereBuilder, attribute, value, operator);

        } else {

            handleWhere((SqlBasicCall) sqlNode0, childWhereBuilder);

            handleWhere((SqlBasicCall) sqlNode1, childWhereBuilder);

            Where where = childWhereBuilder.build();

            if (whereBuilder instanceof AndWhereBuilder) {
                AndWhereBuilder andWhereBuilder = (AndWhereBuilder) whereBuilder;
                andWhereBuilder.and(where);
            } else {
                OrWhereBuilder orWhereBuilder = (OrWhereBuilder) whereBuilder;
                orWhereBuilder.or(where);
            }
        }
    }


    private Object getValue(String field, SqlNode sqlNode) {

        // 集合的处理方式
        if(sqlNode instanceof SqlNodeList) {
            SqlNodeList sqlNodeList = (SqlNodeList) sqlNode;
            return getValues(field, sqlNodeList);
        }

        SqlLiteral sqlLiteral = (SqlLiteral) sqlNode;

        Object value = sqlLiteral.getValue();

        if (value instanceof NlsString) {

            NlsString nlsString = (NlsString) value;

            String valueString = nlsString.getValue();

            //有参数时，判断是不是占位符参数，如果是，将占位符替换成对应的参数
            Matcher matcher = pattern.matcher(valueString);
            boolean matches = matcher.matches();
            if(matches) {
                //没有参数，直接返回
                if(params == null || params.length == 0) {
                    throw new RuntimeException("sql where条件: " + field + " 占位符未匹配参数集合!");
                }
                Integer index = Integer.parseInt(matcher.group("index"));
                if(params.length < index) {
                    throw new RuntimeException("sql where条件: " + field + " 占位符未匹配参数集合[" + (index - 1) + "]!");
                }
                value = params[index - 1];
            } else {
                return valueString;
            }
        }
        return value;
    }

    private Object getValues(String field, SqlNodeList sqlNodeList) {
        List<Object> values = new ArrayList<>();
        for (SqlNode sqlNode : sqlNodeList) {
            Object value = getValue(field, sqlNode);
            values.add(value);
        }
        return values;
    }

    private void handleOperator(NestWhereBuilder whereBuilder, String attribute, Object value, String operator) {
        switch (operator.toLowerCase()) {
            case "=":
                whereBuilder.eq(attribute, value);
                break;
            case ">=":
                whereBuilder.gte(attribute, value);
                break;
            case "<=":
                whereBuilder.lte(attribute, value);
                break;
            case ">":
                whereBuilder.gt(attribute, value);
                break;
            case "<":
                whereBuilder.lt(attribute, value);
                break;
            case "like":
                whereBuilder.like(attribute, value.toString());
                break;
            case "in":
                whereBuilder.in(attribute).values(((List)value).toArray());
                break;
            case "!=":
            case "<>":
                whereBuilder.ne(attribute, value);
                break;
            case "between asymmetric":
                List params = (List) value;
                whereBuilder.gte(attribute, params.get(0)).lte(attribute, params.get(1));
                break;
        }
    }
}
