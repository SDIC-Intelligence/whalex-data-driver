package com.meiya.whalex.sql.function.ani;

import com.meiya.whalex.sql.annotation.SqlFunction;
import com.meiya.whalex.sql.function.SqlFunctionParser;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

/**
 * @author 黄河森
 * @date 2022/8/5
 * @package com.meiya.whalex.sql.function
 * @project whalex-data-driver
 */
@SqlFunction(functionName = "GROUP_CONCAT")
public class DmGroupConcatSqlFunctionParser implements SqlFunctionParser {
    @Override
    public String parseFunc(String funcName, List<String> operandStrList, String functionQuantifierName) {


        //达梦需将 GROUP_CONCAT(`name` order by `name` desc SEPARATOR ',') 转化成 LISTAGG("name",',') WITHIN GROUP(ORDER BY "name" desc)

        //第1个参数 -> 字段
        String fieldName = operandStrList.get(0);
        //连接符(默认是逗号)
        String separator = "','";
        //排序(默认升序)
        String orderBy = fieldName;
        int paramsCount = operandStrList.size();

        switch (paramsCount) {
            case 2:
                //第2个参数 -> 连接符  或  排序
                String param2 = operandStrList.get(1);
                if(param2.contains("SEPARATOR")) {
                    //连接符
                    separator = param2.replaceAll("SEPARATOR", "").trim();
                }else {
                    //排序
                    orderBy = param2;
                }
                break;
            case 3:
                //参数个数3个 -> 字段 + 排序 + 连接符
                //排序
                orderBy = operandStrList.get(1);
                //连接符
                separator =operandStrList.get(2).replaceAll("SEPARATOR", "").trim();

                break;
        }

        StringBuilder sb = new StringBuilder();

        sb.append("LISTAGG(");
        if (StringUtils.isNotBlank(functionQuantifierName)) {
            sb.append(functionQuantifierName).append(" ");
        }

        sb.append(fieldName).append(",").append(separator)
                .append(") WITHIN GROUP(ORDER BY ").append(orderBy).append(")");
        return sb.toString();
    }
}
