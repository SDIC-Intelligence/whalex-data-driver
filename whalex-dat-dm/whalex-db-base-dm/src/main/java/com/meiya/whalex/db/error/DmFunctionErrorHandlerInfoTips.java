package com.meiya.whalex.db.error;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

@Slf4j
public class DmFunctionErrorHandlerInfoTips {

    public static void printTips(String errorMessage)  {
        if(StringUtils.isNotBlank(errorMessage)) {
            if(errorMessage.contains("FIND_IN_SET")) {
                findInSet();
            }else if(errorMessage.contains("PERIOD_ADD")) {
                periodAdd();
            }else if(errorMessage.contains("TIMEDIFF")){
                timeDiff();
            }else if(errorMessage.contains("STRCMP")) {
                strcmp();
            }
        }
    }

    public static  void strcmp(){
        log.warn("达梦默认不支持STRCMP函数，可执行以下sql，自定义STRCMP函数：\n" +
                "CREATE OR REPLACE\n" +
                "FUNCTION SYSDBA.STRCMP(str1 varchar2, str2 varchar2)\n" +
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
                "END STRCMP");
    }

    public static void timeDiff() {
        log.warn("达梦默认不支持FIND_IN_SET函数，可执行以下sql，自定义TIMEDIFF函数：\n" +
                "CREATE OR REPLACE \n" +
                "FUNCTION SYSDBA.TIMEDIFF(time1 varchar2, time2 varchar2)\n" +
                "RETURN varchar2 IS \n" +
                "_hour integer:=0;\n" +
                "_minute integer:=0;\n" +
                "_second integer:=0;\n" +
                "res varchar2;\n" +
                "BEGIN\n" +
                "_second := timestampdiff(second, time2, time1);\n" +
                "_hour := _second / 3600;\n" +
                "_second := abs(mod(_second, 3600));\n" +
                "_minute := _second / 60;\n" +
                "_second := mod(_second, 60);\n" +
                "res := CONCAT(_hour, ':', _minute, ':', _second);\n" +
                "RETURN res;\n" +
                "END TIMEDIFF");
    }

    public static void findInSet() {

        log.warn("达梦默认不支持FIND_IN_SET函数，可执行以下sql，自定义FIND_IN_SET函数：\n" +
                "CREATE OR REPLACE \n" +
                "FUNCTION SYSDBA.FIND_IN_SET(piv_str1 varchar2, piv_str2 varchar2, p_sep varchar2 := ',') \n" +
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
                "END FIND_IN_SET");

    }

    public static void periodAdd() {

        log.warn("达梦默认不支持PERIOD_ADD函数，可执行以下sql，自定义PERIOD_ADD函数：\n" +
                "CREATE OR REPLACE\n" +
                "FUNCTION SYSDBA.PERIOD_ADD(piv_str1 varchar2, piv_str2 varchar2) \n" +
                "RETURN varchar2 IS\n" +
                "monthStr varchar2;\n" +
                "yearVar integer;\n" +
                "monthVar integer;\n" +
                "totalMonth integer;\n" +
                "BEGIN \n" +
                "-- 分割出年份和月份 \n" +
                "yearVar := piv_str1 / 100;\n" +
                "monthVar := mod(piv_str1, 100); \n" +
                "-- 总月份\n" +
                "totalMonth := monthVar + piv_str2 + yearVar * 12 ;\n" +
                "-- 计算年\n" +
                "yearVar := totalMonth / 12;\n" +
                "-- 计算月\n" +
                "monthVar :=  mod(totalMonth, 12);\n" +
                "\n" +
                "IF monthVar = 0 THEN\n" +
                "   monthVar := 12;\n" +
                "   yearVar := yearVar - 1;\n" +
                "END IF;\n" +
                "-- 补0 \n" +
                "IF monthVar < 10 THEN\n" +
                "\tmonthStr := CONCAT('0', monthVar);\n" +
                "ELSE\n" +
                "    monthStr := monthVar;\n" +
                "END IF;\n" +
                "RETURN CONCAT(yearVar, monthStr);\n" +
                "END PERIOD_ADD");

    }



}
