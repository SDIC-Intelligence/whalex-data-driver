package com.meiya.whalex.sql2dsl.parser;

import com.meiya.whalex.db.builder.UpdateRecordBuilder;
import com.meiya.whalex.db.entity.UpdateParamCondition;
import com.meiya.whalex.interior.db.search.in.Where;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.util.NlsString;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 更新数据sql解析
 *
 * @author 蔡荣桂
 * @date 2022/06/27
 * @project whalex-dat-sql
 */
public class UpdateSqlToDslParser extends AbstractSqlToDslParser<UpdateParamCondition> {

    private static Pattern pattern = Pattern.compile("^\\?(?<index>\\d+)$");

    private String sql;

    private String tableName;

    private List<String> columns;

    private List<Object> values;

    private Where where;

    private Object[] params;

    public UpdateSqlToDslParser(String sql, Object[] params) {
        this.sql = sql;
        this.params = params;
    }

    @Override
    public UpdateParamCondition handle() throws SqlParseException {
        SqlParser sqlParser = SqlParser.create(getSqlConvertPlaceholders(sql), config);
        SqlNode sqlNode = sqlParser.parseStmt();
        _handle(sqlNode);

        if (StringUtils.isBlank(tableName)) {
            throw new RuntimeException("表名不能为空");
        }

        if (CollectionUtils.isEmpty(columns)
                || CollectionUtils.isEmpty(values)
                || columns.size() != values.size()) {
            throw new RuntimeException("字段或值不能为空，或者字段和值的长度不一致");
        }


        UpdateRecordBuilder updateRecordBuilder = UpdateRecordBuilder.builder();

        Map<String, Object> params = new HashMap<>();
        for (int i = 0; i < columns.size(); i++) {
            params.put(columns.get(i), values.get(i));
        }

        updateRecordBuilder.update(params);
        updateRecordBuilder.where(where);

        return updateRecordBuilder.build();
    }

    @Override
    public String getTableName() {
        return tableName;
    }

    public void _handle(SqlNode sqlNode) throws SqlParseException {


        if (!(sqlNode instanceof SqlUpdate)) {
            throw new RuntimeException("不是update类型的sql");
        }

        SqlUpdate sqlUpdate = (SqlUpdate) sqlNode;

        //表名
        tableName = getTableName(sqlUpdate.getTargetTable());

        //字段
        columns = getColumns(sqlUpdate.getTargetColumnList());

        //值
        values = getValues(sqlUpdate.getSourceExpressionList());


        //where 条件
        SqlNode whereSqlNode = sqlUpdate.getCondition();

        if (whereSqlNode != null) {

            where = DatWhereBuilder
                    .create()
                    .whereSqlNode(whereSqlNode, params)
                    .build();

        }
    }

    private List<Object> getValues(SqlNodeList sqlNodeList) {

        List<Object> values = new ArrayList<>();

        for (SqlNode valueSqlNode : sqlNodeList) {
            Object value = getValue(valueSqlNode);
            values.add(value);
        }

        return values;
    }


    private Object getValue(SqlNode sqlNode) {

        SqlLiteral sqlLiteral = (SqlLiteral) sqlNode;

        Object value = sqlLiteral.getValue();

        if (value instanceof NlsString) {

            NlsString nlsString = (NlsString) value;
            String valueString = nlsString.getValue();

            value = valueString;

            //没有参数，直接返回
            if (params == null || params.length == 0) {
                return valueString;
            }

            //有参数时，判断是不是占位符参数，如果是，将占位符替换成对应的参数
            Matcher matcher = pattern.matcher(valueString);
            boolean matches = matcher.matches();
            if (matches) {
                Integer index = Integer.parseInt(matcher.group("index"));
                value = params[index - 1];
            }

        }


        return value;
    }

    private List<String> getColumns(SqlNodeList sqlNodeList) {
        List<String> columns = new ArrayList<>();

        for (SqlNode sqlNode : sqlNodeList) {
            String column = getNames(sqlNode).get(0);
            columns.add(column);
        }

        return columns;
    }

    private String getTableName(SqlNode sqlNode) {
        List<String> names = getNames(sqlNode);
        if (names.size() == 2) {
            return names.get(1);
        } else {
            return names.get(0);
        }
    }

    private List<String> getNames(SqlNode sqlNode) {
        SqlIdentifier sqlIdentifier = (SqlIdentifier) sqlNode;
        return sqlIdentifier.names;
    }

}
