package com.meiya.whalex.sql.function;


import java.util.List;

/**
 * 基础函数解析器
 *
 * @author 黄河森
 * @date 2022/8/4
 * @package com.meiya.whalex.sql.function
 * @project whalex-data-driver
 */
public interface SqlFunctionParser {

    /**
     * 解析函数，并拼接成完整SQL片段
     *
     * @param funcName
     * @param operandStrList
     * @param functionQuantifierName
     * @return
     */
    String parseFunc(String funcName, List<String> operandStrList, String functionQuantifierName);
}
