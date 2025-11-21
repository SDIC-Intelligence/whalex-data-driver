package com.meiya.whalex.sql.function;

import com.meiya.whalex.sql.annotation.SqlFunction;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

/**
 * @author 黄河森
 * @date 2022/8/5
 * @package com.meiya.whalex.sql.function
 * @project whalex-data-driver
 */
@SqlFunction(functionName = "GROUP_CONCAT")
public class GroupConcatSqlFunctionParser implements SqlFunctionParser {
    @Override
    public String parseFunc(String funcName, List<String> operandStrList, String functionQuantifierName) {

        //完整语句  GROUP_CONCAT(distinct `name` order by `name` desc SEPARATOR ',')
        StringBuilder sb = new StringBuilder();

        if (StringUtils.isNotBlank(functionQuantifierName)) {
            sb.append(functionQuantifierName).append(" ");
        }

        int paramsCount = operandStrList.size();

        switch (paramsCount) {
            case 1:
                //参数个数1个 -> 字段
                sb.append(operandStrList.get(0));
                break;
            case 2:
                //参数个数2个 -> 字段 + 连接符  或  字段 + 排序
                sb.append(operandStrList.get(0)).append(" ");
                String param2 = operandStrList.get(1);
                if(param2.contains("SEPARATOR")) {
                    sb.append(param2);
                }else {
                    sb.append("ORDER BY ").append(param2);
                }
                break;
            case 3:
                //参数个数3个 -> 字段 + 排序 + 连接符
                sb.append(operandStrList.get(0)).append(" ")
                        .append("ORDER BY ").append(operandStrList.get(1)).append(" ")
                        .append(operandStrList.get(2));
                break;
            default:
                throw new RuntimeException("GROUP_CONCAT未知的参数");
        }

        return funcName + "(" + sb.toString()  + ")";

    }
}
