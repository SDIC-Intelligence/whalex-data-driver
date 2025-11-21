package com.meiya.whalex.sql2dsl.parser;

import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.commons.lang3.StringUtils;

import java.util.Queue;

/**
 * 创建表sql解析
 *
 * @author 蔡荣桂
 * @date 2023/05/05
 * @project whalex-dat-sql
 */
public class DescribeSqlToDslParser extends AbstractSqlToDslParser {


    private String sql;

    private String tableName;

    public DescribeSqlToDslParser(String sql) {
        this.sql = sql;
    }


    @Override
    public Object handle() throws SqlParseException {

        /**
         * describe formatted test.stu
         */

        char[] chars = sql.toCharArray();

        Queue<String> queue = parserWord(chars);

        String describe = queue.poll();
        String formatted = queue.poll();
        if(!describe.equalsIgnoreCase("describe")
                || !formatted.equalsIgnoreCase("formatted")) {
            throw new RuntimeException("未知的sql:" + sql + ", 解析查询表信息, 无法识别关键字[" + describe + " " + formatted + "], " +
                    "支持关键字[DESCRIBE FORMATTED]");
        }


        tableName = queue.poll();
        if(StringUtils.isBlank(tableName)) {
            throw new RuntimeException("表名不能为空");
        }

        if(!queue.isEmpty()) {
            throw new RuntimeException("未知的sql:" + sql + ", 未知的结束符[" + queue.element() + "]");
        }
        return null;
    }

    @Override
    public String getTableName() {
        return tableName;
    }

}
