package com.meiya.whalex.sql2dsl.parser;

import com.meiya.whalex.db.builder.DeleteRecordBuilder;
import com.meiya.whalex.db.entity.DelParamCondition;
import com.meiya.whalex.interior.db.search.in.Where;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

/**
 * sql转dsl
 *
 * @author 蔡荣桂
 * @date 2023/02/02
 * @project whalex-dat-sql
 */
public class DeleteSqlToDslParser extends AbstractSqlToDslParser<DelParamCondition> {

    private String sql;

    private String tableName;

    private Where where;

    private Object[] params;

    public DeleteSqlToDslParser(String sql, Object[] params) {
        this.sql = sql;
        this.params = params;
    }

    @Override
    public DelParamCondition handle() throws SqlParseException {

        SqlParser sqlParser = SqlParser.create(getSqlConvertPlaceholders(sql), config);
        SqlNode sqlNode = sqlParser.parseStmt();

        if (!(sqlNode instanceof SqlDelete)) {
            throw new RuntimeException("不是delete类型的sql");
        }

        SqlDelete sqlDelete = (SqlDelete) sqlNode;

        //表名
        tableName = getTableName(sqlDelete.getTargetTable());


        //where 条件
        SqlNode whereSqlNode = sqlDelete.getCondition();

        if (whereSqlNode != null) {

            where = DatWhereBuilder
                    .create()
                    .whereSqlNode(whereSqlNode, params)
                    .build();

        }

        if (StringUtils.isBlank(tableName)) {
            throw new RuntimeException("表名不能为空");
        }


        DeleteRecordBuilder deleteRecordBuilder = DeleteRecordBuilder.builder();

        deleteRecordBuilder.where(where);

        return deleteRecordBuilder.build();
    }

    @Override
    public String getTableName() {
        return tableName;
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
