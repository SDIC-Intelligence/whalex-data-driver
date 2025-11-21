package com.meiya.whalex.sql2dsl.parser;

import com.meiya.whalex.sql2dsl.entity.DatColumn;
import org.apache.calcite.sql.parser.SqlParseException;

import java.util.Queue;

public class ColumnSqlHandler extends AbstractSqlToDslParser<Object>{

    private String sql;

    public ColumnSqlHandler(String sql) {
        this.sql = sql;
    }

    public DatColumn getColumn(Queue<String> queue) {

        DatColumn column = new DatColumn();

        String fieldName = queue.poll();

        column.name = removeQuoting(fieldName);

        return handleColumn(column, queue);
    }

    public DatColumn handleColumn(DatColumn column, Queue<String> queue) {
        //字段类型处理
        handleDataType(column, queue);

        if (queue.isEmpty() ||  isColumnEnd(queue.element())) {
            return column;
        }

        String word = queue.poll();

        // 是否无符号
        if (word.equalsIgnoreCase("unsigned")) {
            column.unsigned = true;
            if (isColumnEnd(queue.element())) {
                return column;
            }
            word = queue.poll();
        }

        //支持约束条件， 注释可以无序
        while(!queue.isEmpty()) {

            if (word.equalsIgnoreCase("null")) { //约束处理 NULL
                column.nullable = true;
            } else if (word.equalsIgnoreCase("not")) { //约束处理 NOT NULL
                word = queue.poll();
                if (!word.equalsIgnoreCase("null")) {
                    throw new RuntimeException("未知的sql:" + sql + ", 解析字段[" + column.name + "]约束条件[NOT NULL], 无法识别" + word);
                }

                column.nullable = false;
            } else if (word.equalsIgnoreCase("default")) {  //约束处理 DEFAULT '0'

                word = queue.poll();

                StringBuilder defaultValueSb = new StringBuilder();

                if (word.startsWith("'") && word.endsWith("'")) {
                    column.defaultValue = word.substring(1, word.length() - 1);
                }else {
                    defaultValueSb.append(word);
                    while(true) {
                        if(queue.isEmpty()) {
                            break;
                        }
                        String element = queue.element();
                        if(element.equalsIgnoreCase("(")) {
                            String function = getFunction(queue);
                            defaultValueSb.append(function);
                        }else if(element.equalsIgnoreCase("null")
                                || element.equalsIgnoreCase("not")
                                || element.equalsIgnoreCase("comment")
                                || element.equalsIgnoreCase("auto_increment")
                                || element.equalsIgnoreCase("on")){
                            break;
                        }else if(queue.isEmpty() || isColumnEnd(element)) {
                            break;
                        }else {
                            word = queue.poll();
                            defaultValueSb.append(" ").append(word);
                        }
                    }
                    column.defaultValue = defaultValueSb.toString();

                }
            }else if (word.equalsIgnoreCase("auto_increment")) {
                column.autoIncrement = true;
            }else if (word.equalsIgnoreCase("comment")) {

                word = queue.poll();
                column.comment = word.substring(1, word.length() - 1);
            }else if(word.equalsIgnoreCase("CHARACTER")) {
                queue.poll();
                queue.poll();
            }else if(word.equalsIgnoreCase("COLLATE")){
                queue.poll();
            }else if(word.equalsIgnoreCase("on")) {
                word = queue.poll();
                if(!word.equalsIgnoreCase("update")) {
                    throw new RuntimeException("未知的sql:" + sql + ", 解析字段[" + column.name + "]约束条件[ON UPDATE], 无法识别" + word);
                }
                StringBuilder onUpdateBuilder = new StringBuilder();
                onUpdateBuilder.append(queue.poll());
                String element = queue.element();
                if(element.equalsIgnoreCase("(")) {
                    String function = getFunction(queue);
                    onUpdateBuilder.append(function);
                }
                column.onUpdate = onUpdateBuilder.toString();
            } else if(word.equalsIgnoreCase("after")){
                // 字段名 直接过滤掉
                queue.poll();
            }else {
                throw new RuntimeException("未知的sql:" + sql + ", 解析字段[" + column.name + "]约束条件, 无法识别" + word);
            }
            if (queue.isEmpty() || isColumnEnd(queue.element())) {
                return column;
            }
            word = queue.poll();
        }

        throw new RuntimeException("未知的sql:" + sql + ", 解析字段[" + column.name + "], 无法识别结束符" + word);
    }

    private boolean isColumnEnd(String word) {
        if (word.equalsIgnoreCase(",") || word.equalsIgnoreCase(")")) {
            return true;
        }
        return false;
    }

    private String getFunction(Queue<String> queue) {

        StringBuilder funcBuilder = new StringBuilder();
        int funcNum = 0;
        while(true) {
            String element = queue.element();
            if(element.equalsIgnoreCase("(")) {
                funcNum++;
                funcBuilder.append(queue.poll());
            }else if(element.equalsIgnoreCase(")")) {
                funcNum--;
                funcBuilder.append(queue.poll());
                if(funcNum == 0) {
                    break;
                }
            }else {
                funcBuilder.append(queue.poll());
            }
        }

        return funcBuilder.toString();
    }

    public void handleDataType(DatColumn column, Queue<String> queue) {

        String dataType = queue.poll();

        if(queue.isEmpty()) {
            _handleDataType(column, dataType);
            return;
        }

        String word = queue.element();
        //数据类型有括号，int(10), float(4,2)
        if (word.equalsIgnoreCase("(")) {
            _handleDataTypeHasBrackets(column, dataType, queue);
            return;
        }

        //如果是数字类型（int8, float4, double8）需要处理长度; datetime, timestamp不需要处理
        _handleDataType(column, dataType);
    }

    /**
     * 数据类型有括号，int(10), float(4,2)
     *
     * @param column
     * @param dataType
     * @param queue
     */
    private void _handleDataTypeHasBrackets(DatColumn column, String dataType, Queue<String> queue) {

        String word = queue.poll();

        if (!word.equalsIgnoreCase("(")) {
            throw new RuntimeException("未知的sql:" + sql + ", 解析字段["+ column.name + "]数据类型, 无法识别" + word);
        }

        column.dataType = dataType;

        column.length = Integer.valueOf(queue.poll());

        word = queue.poll();

        if (word.equalsIgnoreCase(",")) {

            column.decimalPointLength = Integer.valueOf(queue.poll());
            word = queue.poll();
        }

        if (!word.equalsIgnoreCase(")")) {
            throw new RuntimeException("未知的sql:" + sql + ", 解析字段["+ column.name + "]数据类型, 无法识别" + word);
        }

        // 长度 + 数组类型
        if (!queue.isEmpty() && "[]".equalsIgnoreCase(queue.element())) {
            word = queue.poll();
            column.dataType = column.dataType + word;
        }

    }

    /**
     * 如果是数字类型（int8, float4, double8）需要处理长度; datetime, timestamp不需要处理
     *
     * @param column
     * @param dataType
     */
    private void _handleDataType(DatColumn column, String dataType) {


        if (dataType.toLowerCase().startsWith("int")) {
            handleNumberType(column, dataType, "int");
            return;
        }

        if (dataType.toLowerCase().startsWith("float")) {
            handleNumberType(column, dataType, "float");
            return;
        }

        if (dataType.toLowerCase().startsWith("double")) {
            handleNumberType(column, dataType, "double");
            return;
        }

        column.dataType = dataType;

    }

    private void handleNumberType(DatColumn column, String dataType, String numberType) {

        column.dataType = dataType.substring(0, numberType.length());

        if (dataType.length() > numberType.length()) {
            column.length = Integer.parseInt(dataType.substring(numberType.length()));
        }

    }

    @Override
    public Object handle() throws SqlParseException {
        return null;
    }

    @Override
    public String getTableName() {
        return null;
    }
}
