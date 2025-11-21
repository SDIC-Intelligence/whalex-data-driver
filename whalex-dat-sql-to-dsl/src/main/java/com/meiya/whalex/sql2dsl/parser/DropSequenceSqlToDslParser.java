package com.meiya.whalex.sql2dsl.parser;

import com.meiya.whalex.db.entity.DropSequenceBean;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.commons.lang3.StringUtils;

import java.util.Queue;

/**
 * 创建表sql解析
 *
 * @author 蔡荣桂
 * @date 2022/06/27
 * @project whalex-dat-sql
 */
public class DropSequenceSqlToDslParser extends AbstractSqlToDslParser<DropSequenceBean> {

    private String sql;


    public DropSequenceSqlToDslParser(String sql) {
        this.sql = sql;
    }


    @Override
    public DropSequenceBean handle() throws SqlParseException {

        char[] chars = sql.toCharArray();

        Queue<String> queue = parserWord(chars);
        String drop = queue.poll();
        String sequence = queue.poll();
        if(!drop.equalsIgnoreCase("drop")
                || !sequence.equalsIgnoreCase("sequence")) {
            throw new RuntimeException("未知的sql:" + sql + ", 解析删除序列, 无法识别关键字[" + drop + " " + sequence + "], " +
                    "支持关键字[DROP SEQUENCE]");
        }

        DropSequenceBean dropSequenceBean = new DropSequenceBean();

        String name = queue.poll();

        if(StringUtils.isBlank(name)) {
            throw new RuntimeException("序列名称不能为空");
        }


        dropSequenceBean.setSequenceName(name);

        if(!queue.isEmpty()) {
            throw new RuntimeException("未知的sql:" + sql + ", 未知的结束符[" + queue.element() + "]");
        }

        return dropSequenceBean;
    }

    @Override
    public String getTableName() {
        return null;
    }



}
