package com.meiya.whalex.sql2dsl.parser;

import com.meiya.whalex.db.builder.AlterTableBuilder;
import com.meiya.whalex.interior.db.operation.in.AlterTableParamCondition;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.commons.lang.StringUtils;

/**
 * sql转dsl
 *
 * @author 蔡荣桂
 * @date 2023/02/02
 * @project whalex-dat-sql
 */
public class CommentSqlToDslParser implements ISqlToDslParser<AlterTableParamCondition> {

    private String sql;

    private String tableName;

    private String tableComment;

    private String columnName;

    private String fieldComment;

    public CommentSqlToDslParser(String sql) {
        this.sql = sql;
    }

    @Override
    public AlterTableParamCondition handle() throws SqlParseException {

        //COMMENT ON COLUMN tableName.column IS '123'
        //COMMENT ON table tableName IS '123'
        String[] split = sql.split("\\s+");

        if (!split[0].equalsIgnoreCase("comment")
                || !split[1].equalsIgnoreCase("on")
                || (!split[2].equalsIgnoreCase("column") && !split[2].equalsIgnoreCase("table"))
                || !split[4].equalsIgnoreCase("is")) {
            throw new RuntimeException("不是comment类型的sql: " + sql);
        }

        AlterTableBuilder alterTableBuilder = AlterTableBuilder.builder();

        String word = split[3];

        if(split[2].equalsIgnoreCase("table")) {
            if (word.startsWith("`") && word.endsWith("`")) {
                word = word.substring(1, word.length() - 1);
            }
            tableName = word;
            tableComment = split[5];

            if (tableComment.startsWith("'") && tableComment.endsWith("'")) {
                tableComment = tableComment.substring(1, tableComment.length() - 1);
            }
            alterTableBuilder.tableComment(tableComment);
        }else {
            String[] tableNameAndColumn = word.split("\\.");
            if (tableNameAndColumn.length != 2 && tableNameAndColumn.length != 3) {
                throw new RuntimeException("不是comment类型的sql: " + sql);
            }

            for (int i = tableNameAndColumn.length - 1; i >= 0; i--) {
                String str = tableNameAndColumn[i];
                if (str.startsWith("`") && str.endsWith("`")) {
                    str = str.substring(1, str.length() - 1);
                    tableNameAndColumn[i] = str;
                }
            }

            int length = tableNameAndColumn.length;

            tableName = tableNameAndColumn[length - 2];

            columnName = tableNameAndColumn[length - 1];

            fieldComment = split[5];

            if (fieldComment.startsWith("'") && fieldComment.endsWith("'")) {
                fieldComment = fieldComment.substring(1, fieldComment.length() - 1);
            }

            if (StringUtils.isBlank(columnName)) {
                throw new RuntimeException("列名不能为空");
            }

            if (StringUtils.isBlank(tableName)) {
                throw new RuntimeException("表名不能为空");
            }
            AlterTableBuilder.ColumnBuilder columnBuilder = alterTableBuilder.updateColumn(null);
            columnBuilder.fieldName(columnName);
            columnBuilder.fieldComment(fieldComment);
        }

        return alterTableBuilder.build();
    }

    @Override
    public String getTableName() {
        return tableName;
    }

}
