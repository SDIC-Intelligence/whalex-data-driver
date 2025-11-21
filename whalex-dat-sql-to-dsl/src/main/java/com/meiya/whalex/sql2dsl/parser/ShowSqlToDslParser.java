package com.meiya.whalex.sql2dsl.parser;

import cn.hutool.core.util.EnumUtil;
import com.meiya.whalex.interior.db.operation.in.QueryDatabasesCondition;
import com.meiya.whalex.interior.db.operation.in.QueryTablesCondition;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.commons.lang3.StringUtils;

import java.util.Queue;

/**
 * show tables
 * show database
 * show index
 *
 * @author 蔡荣桂
 * @date 2023/02/02
 * @project whalex-dat-sql
 */
public class ShowSqlToDslParser<T> extends AbstractSqlToDslParser<T> {

    private String sql;

    private ShowType type;

    private String where;

    private String databaseName;

    private String tableName;

    public ShowSqlToDslParser(String sql) {
        this.sql = sql;
    }

    @Override
    public T handle() throws SqlParseException {

        if (StringUtils.isBlank(sql)) {
            throw new RuntimeException("sql不能为空");
        }

        char[] chars = sql.toCharArray();

        Queue<String> queue = parserWord(chars);

        // show 关键字
        String show = queue.poll();
        if (!org.apache.commons.lang.StringUtils.equalsIgnoreCase(show, "show")) {
            throw new RuntimeException("未知的sql:" + sql + ",解析show语句, 无法识别关键字[" + show + "], " +
                    "支持关键字[SHOW]");
        }

        // 解析 show sql 类型
        String type = queue.poll();
        // 类型校验
        ShowType showType = typeValid(type);

        switch (showType) {
            case INDEX:
                parseShowIndexes(queue);
                break;
            case TABLES:
                parseShowTables(queue);
                break;
            case DATABASES:
                parseShowDatabases(queue);
                break;
        }

        if (type == null) {
            throw new RuntimeException("未知的sql:" + sql + ",解析show语句, 无法识别关键字[" + type + "], " +
                    "支持关键字[INDEX, TABLES, DATABASES]");
        }

        //表列表
        if (this.type.equals(ShowType.TABLES)) {
            QueryTablesCondition queryTablesCondition = new QueryTablesCondition();
            queryTablesCondition.setTableMatch(where);
            queryTablesCondition.setDatabaseName(databaseName);
            return (T) queryTablesCondition;
        } else if (this.type.equals(ShowType.DATABASES)) {
            //数据库列表
            QueryDatabasesCondition queryDatabasesCondition = new QueryDatabasesCondition();
            queryDatabasesCondition.setDatabaseMatch(where);
            return (T) queryDatabasesCondition;
        }
        return null;
    }

    @Override
    public String getTableName() {
        return this.tableName;
    }

    /**
     * 解析 show tables
     *
     * @param queue
     */
    private void parseShowTables(Queue<String> queue) {
        type = ShowType.TABLES;
        String op = queue.poll();
        if (StringUtils.equalsIgnoreCase(op, "like")) {
            // show tables like '%'
            String where = queue.poll();
            if (StringUtils.isBlank(where)) {
                throw new RuntimeException("未知的sql:" + sql + ",解析show tables语句, like后面不能是空值");
            }
            this.where = StringUtils.replace(where, "'", "");
        } else if (StringUtils.equalsIgnoreCase(op, "in")) {
            // show tables in database '%'
            String database = queue.poll();
            this.databaseName = removeQuoting(database);
            String where = queue.poll();
            if (StringUtils.isNotBlank(where)) {
                this.where = StringUtils.replace(where, "'", "");
            }
        } else if (StringUtils.isNotBlank(op)) {
            throw new RuntimeException("未知的sql:" + sql + ",解析show tables语句, 无法识别关键字[" + op + "], " +
                    "支持关键字[LIKE, IN]");
        }
    }

    /**
     * 解析 show database
     *
     * @param queue
     */
    private void parseShowDatabases(Queue<String> queue) {
        type = ShowType.DATABASES;
        String op = queue.poll();

        if (StringUtils.equalsIgnoreCase(op, "like")) {
            // show databases like '%'
            String where = queue.poll();
            if (StringUtils.isBlank(where)) {
                throw new RuntimeException("未知的sql:" + sql + ",解析show databases语句, like后面不能是空值");
            }
            this.where = StringUtils.replace(where, "'", "");
        } else if (StringUtils.isNotBlank(op)) {
            throw new RuntimeException("未知的sql:" + sql + ",解析show databases语句, 无法识别关键字[" + op + "], " +
                    "支持关键字[LIKE, IN]");
        }
    }

    /**
     * 解析 show index from table
     *
     * @param queue
     */
    private void parseShowIndexes(Queue<String> queue) {
        type = ShowType.INDEX;
        String op = queue.poll();

        if (StringUtils.equalsIgnoreCase(op, "from")) {
            // show index from table
            tableName = getTableName(queue);
        } else if (StringUtils.isNotBlank(op)) {
            throw new RuntimeException("未知的sql:" + sql + ",解析show index语句, 无法识别关键字[" + op + "], " +
                    "支持关键字[FROM]");
        }
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
     * 校验 SHOW SQL 类型
     *
     * @param type
     */
    private ShowType typeValid(String type) {
        try {
            ShowType showType = ShowType.valueOf(type.toUpperCase());
            return showType;
        } catch (IllegalArgumentException e) {
            throw new RuntimeException("未知的查询类型：" + type);
        }
    }

    /**
     * show sql 类型
     */
    enum ShowType {
        TABLES,
        DATABASES,
        INDEX
    }

}
