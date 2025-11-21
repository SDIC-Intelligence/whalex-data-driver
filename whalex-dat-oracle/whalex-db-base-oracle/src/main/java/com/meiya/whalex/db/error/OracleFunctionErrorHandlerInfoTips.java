package com.meiya.whalex.db.error;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

@Slf4j
public class OracleFunctionErrorHandlerInfoTips {

    public static void main(String[] args) {
        findInSet();
    }

    public static void printTips(String errorMessage)  {
        if(StringUtils.isNotBlank(errorMessage)) {
            if(errorMessage.contains("FIND_IN_SET")) {
                findInSet();
            }else if(errorMessage.contains("QUARTER")) {
                quarter();
            }else if(errorMessage.contains("TIMESTAMPDIFF")){
                timestampDiff();
            }else if(errorMessage.contains("STRCMP")) {
                strcmp();
            }else if(errorMessage.contains("RADIANS")) {
                radians();
            }else if(errorMessage.contains("DEGREES")) {
                degrees();
            }else if(errorMessage.contains("RIGHT")) {
                right();
            }else if(errorMessage.contains("LEFT")) {
                left();
            }else if(errorMessage.contains("REPEAT")) {
                repeat();
            }else if(errorMessage.contains("CONCAT_WS")) {
                concatWs();
            }else if(errorMessage.contains("SUBSTRING_INDEX")) {
                substringIndex();
            }else if(errorMessage.contains("SEC_TO_TIME")) {
                secToTime();
            }else if(errorMessage.contains("YEARWEEK")) {
                yearWeek();
            }
        }
    }

    public static void yearWeek() {
        log.warn("oracle不支持yearweek函数，可执行以下sql，自定义yearweek函数：\n" +
                "CREATE OR REPLACE FUNCTION yearweek(p_date in TIMESTAMP)\n" +
                "\tRETURN VARCHAR2 IS\n" +
                "BEGIN\n" +
                "\treturn TO_CHAR(p_date, 'YYYY') || TO_CHAR(p_date, 'WW');\n" +
                "END;");

    }

    public static void secToTime() {
        log.warn("oracle不支持secToTime函数，可执行以下sql，自定义secToTime函数：\n" +
                "CREATE OR REPLACE FUNCTION sec_to_time(p_num in NUMBER)\n" +
                "\tRETURN VARCHAR2 IS\n" +
                "BEGIN\n" +
                "\tIF TRUNC(p_num/3600) < 10 THEN\n" +
                "\t\treturn TO_CHAR(TRUNC(p_num/3600), 'FM00') || ':' || TO_CHAR(TRUNC(MOD(p_num, 3600)/60), 'FM00') || ':' || TO_CHAR(MOD(p_num, 60), 'FM00');\n" +
                "\tELSE \n" +
                "\t\treturn TRUNC(p_num/3600) || ':' || TO_CHAR(TRUNC(MOD(p_num, 3600)/60), 'FM00') || ':' || TO_CHAR(MOD(p_num, 60), 'FM00');\n" +
                "\tEND IF;\n" +
                "END;");

    }

    public static void substringIndex() {
        log.warn("oracle不支持SUBSTRING_INDEX函数，可执行以下sql，自定义SUBSTRING_INDEX函数：\n" +
                "CREATE OR REPLACE FUNCTION SUBSTRING_INDEX(str in VARCHAR2, delim in VARCHAR2, num in INTEGER)\n" +
                "\tRETURN VARCHAR2\n" +
                "IS\n" +
                "\tlen INTEGER:=0;\n" +
                "\tpiv_str varchar2(500) := str;\n" +
                "\tindexPos INTEGER := 0;\n" +
                "\tdelimLen INTEGER := LENGTH(delim);\n" +
                "\thitCount INTEGER := 0;\n" +
                "\ttmpNum INTEGER:=0;\n" +
                "BEGIN\n" +
                "\t\n" +
                "\tIF num = 0 THEN\n" +
                "\t\treturn NULL;\n" +
                "\tEND IF;\n" +
                "\t--正方向时，计算出位置，截取\n" +
                "\tIF num > 0 THEN\n" +
                "\t\tLOOP\n" +
                "\t\t\tindexPos := INSTR(piv_str, delim);\n" +
                "\t\t  IF indexPos > 0 THEN\t\n" +
                "\t\t\t\tpiv_str := SUBSTR(piv_str, indexPos + delimLen);\n" +
                "\t\t\t\tlen := len + indexPos + delimLen - 1;\n" +
                "\t\t\t\thitCount := hitCount + 1;\n" +
                "\t\t\t\tIF hitCount = num THEN\n" +
                "\t\t\t\t\tEXIT;\n" +
                "\t\t\t\tEND IF;\n" +
                "\t\t\tELSE\n" +
                "\t\t\t  return str;\n" +
                "\t\t\tEND IF;\n" +
                "\t\tEND LOOP;\n" +
                "\t\tRETURN SUBSTR(str, 1, len - delimLen);\n" +
                "\tELSE\n" +
                "\t  -- 反方向时，先统计出现的次数，再转成正方向，最后反向截取\n" +
                " \t  LOOP\n" +
                " \t\t\t--统计有在字符串中出现多少次\n" +
                " \t\t\tindexPos := INSTR(piv_str, delim);\n" +
                " \t\t  IF indexPos > 0 THEN\t\n" +
                " \t\t\t\tpiv_str := SUBSTR(piv_str, indexPos + delimLen);\n" +
                " \t\t\t\thitCount := hitCount + 1;\n" +
                " \t\t\tELSE\n" +
                " \t\t\t  EXIT;\n" +
                " \t\t\tEND IF;\n" +
                " \t\tEND LOOP;\n" +
                "\t\t--超出总个数，返回原字符串\n" +
                " \t\ttmpNum := hitCount + num;\n" +
                " \t\tIF tmpNum < 0 THEN\n" +
                " \t\t\treturn str;\n" +
                " \t\tELSE\n" +
                " \t\t  --变成正方向出现次数\n" +
                " \t\t\ttmpNum := tmpNum + 1;\n" +
                " \t\t\tpiv_str := str;\n" +
                " \t\t\thitCount := 0;\n" +
                " \t\t  LOOP\n" +
                " \t\t\t\tindexPos := INSTR(piv_str, delim);\n" +
                " \t\t\t\tIF indexPos > 0 THEN\t\n" +
                " \t\t\t\t\tpiv_str := SUBSTR(piv_str, indexPos + delimLen);\n" +
                " \t\t\t\t\tlen := len + indexPos + delimLen - 1;\n" +
                " \t\t\t\t\thitCount := hitCount + 1;\n" +
                " \t\t\t\t\tIF hitCount = tmpNum THEN\n" +
                " \t\t\t\t\t\tEXIT;\n" +
                " \t\t\t\t\tEND IF;\n" +
                "\t\t\t\tELSE\n" +
                "\t\t\t\t\treturn str;\n" +
                " \t\t\t\tEND IF;\n" +
                " \t\t\tEND LOOP;\n" +
                "\t\tEND IF;\n" +
                "\t\tRETURN SUBSTR(str, len + 1);\n" +
                "\tEND IF;\n" +
                "End;");

    }

    public static  void concatWs(){
        log.warn("oracle不支持CONCAT_WS函数，可执行以下sql，自定义CONCAT_WS函数：\n" +
                "CREATE OR REPLACE FUNCTION concat_ws(separator in varchar2, str1 in varchar2, str2 in varchar2)\n" +
                "RETURN varchar2 IS \n" +
                "BEGIN\n" +
                "\treturn CONCAT(CONCAT(str1, separator), str2);\n" +
                "END;");
    }

    public static  void repeat(){
        log.warn("oracle不支持repeat函数，可执行以下sql，自定义repeat函数：\n" +
                "CREATE OR REPLACE FUNCTION repeat(str in varchar2, len in NUMBER)\n" +
                "\treturn VARCHAR2\n" +
                "IS\n" +
                "\tresult VARCHAR2(255);\n" +
                "BEGIN\n" +
                "\tfor i in 1 .. len loop\n" +
                "\t  result := concat(result, str);\n" +
                "\tend loop;\n" +
                "\treturn result;\n" +
                "END;");
    }

    public static  void left(){
        log.warn("oracle不支持left函数，可执行以下sql，自定义left函数：\n" +
                "CREATE OR REPLACE FUNCTION left(str in varchar2, len in NUMBER)\n" +
                "\treturn varchar2 IS\n" +
                "BEGIN\n" +
                "\treturn SUBSTR(str, 1, len);\n" +
                "END;");
    }

    public static  void right(){
        log.warn("oracle不支持right函数，可执行以下sql，自定义right函数：\n" +
                "CREATE OR REPLACE FUNCTION right(str in varchar2, len in NUMBER)\n" +
                "\treturn varchar2 IS\n" +
                "BEGIN\n" +
                "\treturn SUBSTR(str, -len);\n" +
                "END;");
    }

    public static  void degrees(){
        log.warn("oracle不支持degrees函数，可执行以下sql，自定义degrees函数：\n" +
                "CREATE OR REPLACE FUNCTION degrees(radians in NUMBER)\n" +
                "\treturn NUMBER IS\n" +
                "BEGIN\n" +
                "\treturn 180/ACOS(-1)*radians;\n" +
                "END;\n");
    }

    public static  void radians(){
        log.warn("oracle不支持radians函数，可执行以下sql，自定义radians函数：\n" +
                "CREATE OR REPLACE FUNCTION radians(degrees in NUMBER)\n" +
                "\treturn NUMBER IS\n" +
                "BEGIN\n" +
                "\treturn ACOS(-1)/180*degrees;\n" +
                "END;");
    }

    public static  void strcmp(){
        log.warn("oracle不支持STRCMP函数，可执行以下sql，自定义STRCMP函数：\n" +
                "CREATE OR REPLACE\n" +
                "FUNCTION STRCMP(str1 in varchar2, str2 in varchar2)\n" +
                "RETURN integer IS \n" +
                "BEGIN\n" +
                "IF str1 < str2 THEN\n" +
                "\tRETURN -1;\n" +
                "END IF;\n" +
                "IF str1 = str2 THEN\n" +
                "\tRETURN 0;\n" +
                "ELSE \n" +
                "\tRETURN 1;\n" +
                "END IF;\n" +
                "END;");
    }

    public static void timestampDiff() {
        log.warn("oracle不支持TIMESTAMPDIFF函数，可执行以下sql，自定义TIMESTAMPDIFF函数：\n" +
                "CREATE OR REPLACE FUNCTION TIMESTAMPDIFF(p_unit in VARCHAR2, p_start_date in TIMESTAMP, p_end_date in TIMESTAMP) \n" +
                "\tRETURN NUMBER IS\n" +
                "\tv_diff NUMBER;\n" +
                "\tv_unit VARCHAR2(30) := UPPER(p_unit);\n" +
                "\tv_day NUMBER := 0;\n" +
                "\tv_hour NUMBER := 0;\n" +
                "\tv_minute NUMBER := 0;\n" +
                "\tv_second NUMBER := 0;\n" +
                "BEGIN\n" +
                "\tCASE v_unit\n" +
                "\t\tWHEN 'YEAR' THEN\n" +
                "\t\t\tv_diff := EXTRACT(YEAR FROM p_end_date) - EXTRACT(YEAR FROM p_start_date);\n" +
                "\t\tWHEN 'MONTH' THEN\n" +
                "\t\t\tv_diff := (EXTRACT(YEAR FROM p_end_date) - EXTRACT(YEAR FROM p_start_date)) * 12 + EXTRACT(MONTH FROM p_end_date) - EXTRACT(MONTH FROM p_start_date);\n" +
                " \t\tWHEN 'DAY' THEN\n" +
                " \t\t\tv_diff := EXTRACT(DAY FROM(p_end_date - p_start_date));\n" +
                "\t\tWHEN 'HOUR' THEN\n" +
                "\t\t\tv_day := EXTRACT(DAY FROM(p_end_date - p_start_date));\n" +
                "\t\t\tv_hour := EXTRACT(HOUR FROM(p_end_date - p_start_date));\n" +
                "\t\t\tv_diff := v_day * 24 + v_hour;\n" +
                "\t\tWHEN 'MINUTE' THEN\n" +
                "\t\t\tv_day := EXTRACT(DAY FROM(p_end_date - p_start_date));\n" +
                "\t\t\tv_hour := EXTRACT(HOUR FROM(p_end_date - p_start_date));\n" +
                "\t\t\tv_minute := EXTRACT(MINUTE FROM(p_end_date - p_start_date));\n" +
                "\t\t\tv_diff := (v_day * 24 + v_hour) * 60 + v_minute;\n" +
                "\t\tWHEN 'SECOND' THEN\n" +
                "\t\t\tv_day := EXTRACT(DAY FROM(p_end_date - p_start_date));\n" +
                "\t\t\tv_hour := EXTRACT(HOUR FROM(p_end_date - p_start_date));\n" +
                "\t\t\tv_minute := EXTRACT(MINUTE FROM(p_end_date - p_start_date));\n" +
                "\t\t\tv_second := EXTRACT(SECOND FROM(p_end_date - p_start_date));\n" +
                "\t\t\tv_diff := TRUNC(((v_day * 24 + v_hour) * 60 + v_minute) * 60 + v_second, 0);\n" +
                "\t\tELSE\n" +
                "\t\t\tRAISE_APPLICATION_ERROR(-20001, 'Invalid unit: ' || p_unit);\n" +
                "\tEND CASE;\n" +
                "\tRETURN v_diff;\n" +
                "END;");
    }

    public static void findInSet() {

        log.warn("oracle不支持FIND_IN_SET函数，可执行以下sql，自定义FIND_IN_SET函数：\n" +
                "CREATE OR REPLACE \n" +
                "FUNCTION FIND_IN_SET(piv_str1 varchar2, piv_str2 varchar2, p_sep varchar2 := ',') \n" +
                "RETURN NUMBER IS \n" +
                "l_idx number:=0; -- 用于计算 piv_str2 中分隔符的位置\n" +
                "str varchar2(500); -- 根据分隔符截取的子字符串 \n" +
                "piv_str varchar2(500) := piv_str2; -- 将 piv_str2 赋值给 piv_str \n" +
                "res number:=0; -- 返回结果 \n" +
                "BEGIN \n" +
                "-- 如果 piv_str 中没有分割符，直接判断 piv_str1 和 piv_str 是否相等，相等 res=1 \n" +
                "IF instr(piv_str, p_sep, 1) = 0 THEN \n" +
                "\tIF piv_str = piv_str1 THEN \n" +
                "\t\tres:= 1; \n" +
                "\tEND IF; \n" +
                "ELSE \n" +
                "\t-- 循环按分隔符截取 piv_str\n" +
                "\tLOOP\n" +
                "\t\tl_idx := instr(piv_str,p_sep);\n" +
                "\t\t-- 位置\n" +
                "\t\tres:=res+1; \n" +
                "\t\t-- 当 piv_str 中还有分隔符时 \n" +
                "\t\tIF l_idx > 0 THEN \n" +
                "\t\t\t-- 截取第一个分隔符前的字段str \n" +
                "\t\t\tstr:= substr(piv_str,1,l_idx- 1); \n" +
                "\t\t\t-- 判断 str 和 piv_str1 是否相等，相等 res=1 并结束循环判断 \n" +
                "\t\t\tIF str = piv_str1 THEN  \n" +
                "\t\t\t\tEXIT; \n" +
                "\t\t\tEND IF; \n" +
                "\t\t\tpiv_str := substr(piv_str,l_idx+length(p_sep)); \n" +
                "\t\tELSE\n" +
                "\t\t\t-- 当截取后的 piv_str 中不存在分割符时，判断 piv_str 和 piv_str1 是否相等，相 等 res=1 \n" +
                "\t\t\tIF piv_str != piv_str1 THEN \n" +
                "\t\t\t\tres:=0; \n" +
                "\t\t\tEND IF; \n" +
                "\t\t\t-- 无论最后是否相等，都跳出循环\n" +
                "\t\t\tEXIT; \n" +
                "\t\tEND IF; \n" +
                "\tEND LOOP; \n" +
                "-- 结束循环 \n" +
                "END IF; \n" +
                "-- 返回 res \n" +
                "RETURN res; \n" +
                "END;");

    }

    public static void quarter() {

        log.warn("oracle不支持QUARTER函数，可执行以下sql，自定义QUARTER函数：\n" +
                "CREATE OR REPLACE FUNCTION quarter(p_date in TIMESTAMP)\n" +
                "\tRETURN NUMBER IS\n" +
                "\tv_month NUMBER;\n" +
                "BEGIN\n" +
                "\tv_month := EXTRACT(month FROM p_date);\n" +
                "\tIF v_month IN (3, 4, 5) THEN\n" +
                "\t\treturn 1;\n" +
                "\tELSIF v_month IN (6, 7, 8) THEN\n" +
                "\t\treturn 2;\n" +
                "\tELSIF v_month IN (9, 10, 11) THEN\n" +
                "\t\treturn 3;\n" +
                "\tELSE\n" +
                "\t\treturn 4;\n" +
                "\tEND IF;\n" +
                "END;");

    }



}
