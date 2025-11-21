package com.meiya.whalex.sql2dsl.parser;

import com.meiya.whalex.db.entity.CreateSequenceBean;
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
public class CreateSequenceSqlToDslParser extends AbstractSqlToDslParser<CreateSequenceBean> {


    private String sql;

    public CreateSequenceSqlToDslParser(String sql) {
        this.sql = sql;
    }


    @Override
    public CreateSequenceBean handle() throws SqlParseException {

        /**
         * create sequence 序列名
         * [increment by n] --每次增加的值
         * [start with n] --初始值
         * [{maxvalue/minvalue n|nomaxvalue/nominvalue}] --最大/最小值 | 不设最大/最小值
         * [{cycle|nocycle}] --cycle 代表循环， nocycle 代表不循环
         * [{cache n|nocache}] --cache定义存放序列的内存块大小, nocache不对序列进行内存缓冲
         *
         */

        char[] chars = sql.toCharArray();

        Queue<String> queue = parserWord(chars);

        if(!queue.poll().equalsIgnoreCase("create")
                || !queue.poll().equalsIgnoreCase("sequence")) {
            throw new RuntimeException("未知的sql: " + sql);
        }


        String name = queue.poll();
        if(StringUtils.isBlank(name)) {
            throw new RuntimeException("序列名称不能为空");
        }

        CreateSequenceBean createSequenceBean = new CreateSequenceBean();
        createSequenceBean.setSequenceName(name);

        while (!queue.isEmpty()) {
            wordHandler(queue, createSequenceBean);
        }

        return createSequenceBean;
    }

    @Override
    public String getTableName() {
        return null;
    }

    private void wordHandler(Queue<String> queue, CreateSequenceBean createSequenceBean) {
        String word = queue.poll();

        //每次增加的值
        if ("increment".equalsIgnoreCase(word)) {
            String by = queue.poll();
            if (!by.equalsIgnoreCase("by")) {
                throw new RuntimeException("未知的sql:" + sql + ", 无法识别关键字[" + by + "], " +
                        "支持关键字[BY]");
            }
            Integer incrementBy = new Integer(queue.poll());
            createSequenceBean.setIncrementBy(incrementBy);
            return;
        }

        //初始值
        if ("start".equalsIgnoreCase(word)) {
            String with = queue.poll();
            if (!with.equalsIgnoreCase("with")) {
                throw new RuntimeException("未知的sql:" + sql + ", 无法识别关键字[" + with + "], " +
                        "支持关键字[WITH]");
            }
            Integer startWith = new Integer(queue.poll());
            createSequenceBean.setStartWith(startWith);
            return;
        }

        //最大值
        if ("maxvalue".equalsIgnoreCase(word)) {
            Integer maxValue = new Integer(queue.poll());
            createSequenceBean.setMaxValue(maxValue);
            return;
        }

        //不设最大值
        if ("nomaxvalue".equalsIgnoreCase(word)) {
            createSequenceBean.setNoMaxValue(true);
            return;
        }

        //最小值
        if ("minvalue".equalsIgnoreCase(word)) {
            Integer minvalue = new Integer(queue.poll());
            createSequenceBean.setMinValue(minvalue);
            return;
        }

        //不设最小值
        if ("nominvalue".equalsIgnoreCase(word)) {
            createSequenceBean.setNoMinValue(true);
            return;
        }

        //循环
        if ("cycle".equalsIgnoreCase(word)) {
            createSequenceBean.setCycle(true);
            return;
        }

        //不循环
        if ("nocycle".equalsIgnoreCase(word)) {
            createSequenceBean.setCycle(false);
            return;
        }

        //缓存大小
        if ("cache".equalsIgnoreCase(word)) {
            Integer cache = new Integer(queue.poll());
            createSequenceBean.setCache(cache);
            return;
        }

        //不设置缓存
        if ("nocache".equalsIgnoreCase(word)) {
            createSequenceBean.setNoCache(true);
            return;
        }

        throw new RuntimeException("未知的sql:" + sql + ", 无法识别关键字[" + word + "], " +
                "支持关键字[INCREMENT, START, MAXVALUE, MINVALUE, NOMINVALUE, CYCLE, NOCYCLE, CACHE, NOCACHE]");
    }

}
