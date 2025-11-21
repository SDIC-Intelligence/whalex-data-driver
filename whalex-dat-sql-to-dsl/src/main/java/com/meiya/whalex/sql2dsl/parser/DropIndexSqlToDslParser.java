package com.meiya.whalex.sql2dsl.parser;

import com.meiya.whalex.db.entity.IndexParamCondition;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.commons.lang.StringUtils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * sql转dsl
 *
 * @author 蔡荣桂
 * @date 2023/02/02
 * @project whalex-dat-sql
 */
public class DropIndexSqlToDslParser implements ISqlToDslParser<IndexParamCondition> {

    private static final Pattern compile = Pattern
            .compile("drop index \\s*(?<indexName>[^\\s]+) \\s*on (?<tableName>[^\\s]+)",
                    Pattern.CASE_INSENSITIVE);
    private String sql;

    private String tableName;

    private String indexName;

    public DropIndexSqlToDslParser(String sql) {
        this.sql = sql;
    }

    @Override
    public IndexParamCondition handle() throws SqlParseException {

        Matcher matcher = compile.matcher(sql);
        if(!matcher.matches()) {
            throw new RuntimeException("未知的sql:" + sql);
        }

        //索引名
        indexName = matcher.group("indexName");

        //表名
        tableName = matcher.group("tableName");

        if(StringUtils.isBlank(tableName)) {
            throw new RuntimeException("表名不能为空");
        }

        if(StringUtils.isBlank(indexName)) {
            throw new RuntimeException("索引名不能为空");
        }

        if(tableName.startsWith("`") && tableName.endsWith("`")) {
            tableName = tableName.substring(1, tableName.length() - 1);
        }

        if(indexName.startsWith("`") && indexName.endsWith("`")) {
            indexName = indexName.substring(1, indexName.length() - 1);
        }

        IndexParamCondition indexParamCondition = IndexParamCondition.builder()
                .indexName(indexName)
                .build();

        return indexParamCondition;
    }

    @Override
    public String getTableName() {
        return tableName;
    }

}
