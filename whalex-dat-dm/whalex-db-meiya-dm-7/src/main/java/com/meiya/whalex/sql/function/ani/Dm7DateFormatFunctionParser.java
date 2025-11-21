package com.meiya.whalex.sql.function.ani;

import com.meiya.whalex.sql.annotation.SqlFunction;
import com.meiya.whalex.sql.function.SqlFunctionParser;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

/**
 * @author 黄河森
 * @date 2022/8/5
 * @package com.meiya.whalex.db.util.param.impl.ani
 * @project whalex-data-driver
 */
@SqlFunction(functionName = "DATE_FORMAT")
public class Dm7DateFormatFunctionParser implements SqlFunctionParser {
    @Override
    public String parseFunc(String funcName, List<String> operandStrList, String functionQuantifierName) {
        StringBuilder sb = new StringBuilder();
        String format = operandStrList.get(1);
        String fmt = StringUtils.replaceEach(format, new String[]{"%Y","%y","%m","%M","%d","%H","%h","%I","%i","%S","%s"}, new String[]{"YYYY","YY","MM","MONTH","DD","HH24","HH","HH","MI","SS","SS"});
        sb.append(operandStrList.get(0)).append(",").append(fmt);
        return  "TO_CHAR(" + sb.toString() + ")";
    }
}
