package com.meiya.whalex.sql2dsl.parser;


import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.util.StrUtil;
import com.meiya.whalex.db.constant.IndexType;
import com.meiya.whalex.db.entity.IndexParamCondition;
import com.meiya.whalex.interior.db.search.condition.Sort;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.commons.lang.StringUtils;

import java.util.*;

/**
 * 创建索引sql解析
 *
 * @author 蔡荣桂
 * @date 2022/06/27
 * @project whalex-dat-sql
 */
public class CreateIndexSqlToDslParser extends AbstractSqlToDslParser<IndexParamCondition> {

    private String sql;

    private String tableName;

    private IndexType indexType;

    private String indexName;

    private List<IndexParamCondition.IndexColumn> fields;

    public CreateIndexSqlToDslParser(String sql) {
        this.sql = sql;
    }

    @Override
    public IndexParamCondition handle() throws SqlParseException {

        char[] chars = sql.toCharArray();

        Queue<String> queue = parserWord(chars);

        // create [unique] index
        String create = queue.poll();
        if (!StringUtils.equalsIgnoreCase(create, "create")) {
            throw new RuntimeException("未知的sql:" + sql + ", 无法识别关键字[" + create + "], " +
                    "支持关键字[CREATE]");
        }

        // 解析判断是 unique | index
        String word = queue.element();
        if (!StringUtils.equalsIgnoreCase(word, "index")) {
            indexType = getIndexType(queue);
        }

        // index 关键字
        String index = queue.poll();
        if (!StringUtils.equalsIgnoreCase(index, "index")) {
            throw new RuntimeException("未知的sql:" + sql + ", 无法识别关键字[" + index + "], " +
                    "支持关键字[INDEX]");
        }

        // 获取索引名称
        indexName = getIndexName(queue);

        // ON 关键字
        String on = queue.poll();
        if (!StringUtils.equalsIgnoreCase(on, "on")) {
            throw new RuntimeException("未知的sql:" + sql + ", 无法识别关键字[" + on + "], " +
                    "支持关键字[ON]");
        }

        // 获取表名
        tableName = getTableName(queue);

        // 解析索引字段
        fields = getColumns(queue);

        if (StringUtils.isBlank(tableName)) {
            throw new RuntimeException("表名不能为空");
        }

        if (StringUtils.isBlank(this.indexName)) {
            throw new RuntimeException("索引名不能为空");
        }

        if (CollectionUtil.isEmpty(fields)) {
            throw new RuntimeException("索引字段不能为空");
        }


        IndexParamCondition indexParamCondition = IndexParamCondition.builder()
                .indexType(indexType)
                .indexName(indexName)
                .columns(fields)
                .build();

        return indexParamCondition;

    }

    /**
     * 获取索引类型
     *
     * @param queue
     */
    private IndexType getIndexType(Queue<String> queue) {
        String type = queue.poll();
        try {
            IndexType indexType = IndexType.valueOf(type.toUpperCase());
            return indexType;
        } catch (IllegalArgumentException e) {
            throw new RuntimeException("未知的sql:" + sql + ", 无法识别关键字[" + type + "], " +
                    "支持关键字[UNIQUE]");
        }
    }

    /**
     * 获取索引表名
     *
     * @param queue
     * @return
     */
    private String getIndexName(Queue<String> queue) {
        String indexName = queue.poll();
        return removeQuoting(indexName);
    }

    /**
     * 获取索引表名
     *
     * @param queue
     * @return
     */
    private String getTableName(Queue<String> queue) {

        String tableName = queue.poll();

        String[] names = tableName.split("\\.");

        if (names.length == 2) {
            tableName = names[1];
        } else {
            tableName = names[0];
        }

        return removeQuoting(tableName);
    }

    /**
     * 解析索引字段
     *
     * @param queue
     * @return
     */
    private List<IndexParamCondition.IndexColumn> getColumns(Queue<String> queue) {
        List<IndexParamCondition.IndexColumn> indexColumnList = new ArrayList<>();
        String word = queue.poll();
        if (!word.equalsIgnoreCase("(")) {
            throw new RuntimeException("未知的sql:" + sql + ", 解析索引字段缺少(, 在" + word + "附近");
        }
        while (!queue.isEmpty()) {
            word = queue.element();
            //结束标识
            if (word.equalsIgnoreCase(")")) {
                queue.poll();
                return indexColumnList;
            }

            if(isColumnEnd(word)) {
                queue.poll();
            } else {
                // 字段处理
                indexColumnList.add(getColumn(queue));
            }
        }
        return indexColumnList;
    }

    /**
     * 获取所有字段
     *
     * @param queue
     */
    private IndexParamCondition.IndexColumn getColumn(Queue<String> queue) {

        IndexParamCondition.IndexColumn indexColumn = new IndexParamCondition.IndexColumn();

        String fieldName = queue.poll();

        fieldName = removeQuoting(fieldName);

        indexColumn.setColumn(fieldName);

        if (isColumnEnd(queue.element())) {
            return indexColumn;
        }

        String word = queue.poll();
        if (StrUtil.equalsAnyIgnoreCase(word, Sort.ASC.getName(), Sort.DESC.getName())) {
            Sort sort = Sort.parse(word.toUpperCase());
            indexColumn.setSort(sort);
            return indexColumn;
        }

        throw new RuntimeException("未知的sql:" + sql + ", 解析索引字段, 未知的排序类型[" + word + "]" +
                ", 支持排序类型[ASC, DESC]");
    }

    private boolean isColumnEnd(String word) {
        if (word.equalsIgnoreCase(",") || word.equalsIgnoreCase(")")) {
            return true;
        }
        return false;
    }

    @Override
    public String getTableName() {
        return tableName;
    }

}
