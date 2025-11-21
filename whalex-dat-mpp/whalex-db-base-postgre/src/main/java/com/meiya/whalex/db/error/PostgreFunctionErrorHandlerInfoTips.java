package com.meiya.whalex.db.error;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

@Slf4j
public class PostgreFunctionErrorHandlerInfoTips {


    public static void main(String[] args) {
        substringIndex();
    }

    public static void printTips(String errorMessage)  {
        if(StringUtils.isNotBlank(errorMessage)) {
            errorMessage = errorMessage.toUpperCase();
            if(errorMessage.contains("MAKEDATE")) {
                makeDay();
            }else if(errorMessage.contains("YEARWEEK")){
                yearWeek();
            }else if(errorMessage.contains("STRCMP")) {
                strcmp();
            }else if(errorMessage.contains("LOCATE")) {
                locate();
            }else if(errorMessage.contains("FIND_IN_SET")) {
                findInSet();
            }else if(errorMessage.contains("INSTR")) {
                instr();
            }else if(errorMessage.contains("SUBSTRING_INDEX")) {
                substringIndex();
            }else if(errorMessage.contains("SUBTIME")) {
                subTime();
            }else if(errorMessage.contains("PERIOD_ADD")) {
                periodAdd();
            }else if(errorMessage.contains("PERIOD_DIFF")) {
                periodDiff();
            }else if(errorMessage.contains("ADDTIME")) {
                addTime();
            }else if(errorMessage.contains("TIMEDIFF")) {
                timeDiff();
            }else if(errorMessage.contains("TIMESTAMPDIFF")) {
                timestampdiff();
            }else if(errorMessage.contains("SEC_TO_TIME")) {
                secToTime();
            }else if(errorMessage.contains("FROM_DAYS")) {
                fromDays();
            }else if(errorMessage.contains("JSON_EXISTS")) {
                jsonExists();
            }
        }
    }

    private static void jsonExists() {
        String funcName = "JSON_EXISTS";
        String sql = "CREATE OR REPLACE FUNCTION JSON_EXISTS(json_str json, key_str VARCHAR)\n" +
                "\tRETURNS VARCHAR\n" +
                "\tLANGUAGE plpgsql\n" +
                "AS\n" +
                "$$\n" +
                "DECLARE\n" +
                "BEGIN\n" +
                "\tRETURN json_str::jsonb ? key_str;\n" +
                "End;\n" +
                "$$;";
        printTips(funcName, sql);
    }

    private static void fromDays() {
        String funcName = "FROM_DAYS";
        String sql = "CREATE OR REPLACE FUNCTION FROM_DAYS(dayNum INTEGER)\n" +
                "\tRETURNS VARCHAR\n" +
                "\tLANGUAGE plpgsql\n" +
                "AS\n" +
                "$$\n" +
                "DECLARE\n" +
                "\ttmpDate DATE;\n" +
                "BEGIN\n" +
                "\tIF dayNum < 0 THEN\n" +
                "\t\tRETURN NULL;\n" +
                "\tEND IF;\n" +
                "\tIF dayNum >= 366 THEN\n" +
                "\tRETURN '0001-01-01'::DATE + (dayNum - 366);\n" +
                "\tELSE\n" +
                "\t\ttmpDate := '2000-01-01'::DATE + dayNum;\n" +
                "\t\tRETURN CONCAT('0000', TO_CHAR(tmpDate, '-MM-DD'));\n" +
                "\tEND IF;\n" +
                "End;\n" +
                "$$;";
        printTips(funcName, sql);
    }

    private static void secToTime() {
        String funcName = "SEC_TO_TIME";
        String sql = "CREATE OR REPLACE FUNCTION SEC_TO_TIME(num INTEGER)\n" +
                "\tRETURNS VARCHAR\n" +
                "\tLANGUAGE plpgsql\n" +
                "AS\n" +
                "$$\n" +
                "DECLARE\n" +
                "\t_hour INTEGER:=0;\n" +
                "\t_minute INTEGER:=0;\n" +
                "\t_second INTEGER:=0;\n" +
                "\tres VARCHAR;\n" +
                "BEGIN\n" +
                "\t_second := mod(num, 86400);\n" +
                "\t_hour := _second / 3600;\n" +
                "\t_second := abs(mod(_second, 3600));\n" +
                "\t_minute := _second / 60;\n" +
                "\t_second := mod(_second, 60);\n" +
                "\tIF _hour < 10 THEN\n" +
                "\t\tres := CONCAT('0', _hour);\n" +
                "\tELSE\n" +
                "\t\tres := _hour;\n" +
                "\tEND IF;\n" +
                "\t\n" +
                "\tIF _minute < 10 THEN\n" +
                "\t\tres := CONCAT(res, ':', '0', _minute);\n" +
                "\tELSE\n" +
                "\t\tres := CONCAT(res, ':', _minute);\n" +
                "\tEND IF;\n" +
                "\t\n" +
                "\tIF _second < 10 THEN\n" +
                "\t\tres := CONCAT(res, ':', '0', _second);\n" +
                "\tELSE\n" +
                "\t\tres := CONCAT(res, ':', _second);\n" +
                "\tEND IF;\n" +
                "\t\n" +
                "\tRETURN res;\n" +
                "End;\n" +
                "$$;";
        printTips(funcName, sql);
    }

    private static void timestampdiff() {
        String funcName = "TIMESTAMPDIFF";
        String sql = "CREATE OR REPLACE FUNCTION TIMESTAMPDIFF(datepart VARCHAR, time1 TIMESTAMP, time2 TIMESTAMP)\n" +
                "\tRETURNS INTEGER\n" +
                "\tLANGUAGE plpgsql\n" +
                "AS\n" +
                "$$\n" +
                "DECLARE\n" +
                "BEGIN\n" +
                "\n" +
                "\tIF datepart = 'YEAR' THEN\n" +
                "\t\tRETURN EXTRACT(YEAR FROM AGE(time2,time1));\n" +
                "\tEND IF;\n" +
                "\t\n" +
                "\tIF datepart = 'MONTH' THEN\n" +
                "\t\tRETURN EXTRACT(YEAR FROM AGE(time2,time1)) * 12 + EXTRACT(MONTH FROM AGE(time2,time1));\n" +
                "\tEND IF;\n" +
                "\t\n" +
                "\tIF datepart = 'DAY' THEN\n" +
                "\t\tRETURN EXTRACT(EPOCH FROM (time2 - time1)) / 86400;\n" +
                "\tEND IF;\n" +
                "\t\n" +
                "\tIF datepart = 'HOUR' THEN\n" +
                "\t\tRETURN EXTRACT(EPOCH FROM (time2 - time1)) / 3600;\n" +
                "\tEND IF;\n" +
                "\t\n" +
                "\tIF datepart = 'MINUTE' THEN\n" +
                "\t\tRETURN EXTRACT(EPOCH FROM (time2 - time1)) / 60;\n" +
                "\tEND IF;\n" +
                "\t\n" +
                "\tIF datepart = 'SECOND' THEN\n" +
                "\t\tRETURN EXTRACT(EPOCH FROM (time2 - time1));\n" +
                "\tEND IF;\n" +
                "End;\n" +
                "$$;\n" +
                "\n" +
                "CREATE OR REPLACE FUNCTION TIMESTAMPDIFF(datepart VARCHAR, time1 timestamp with time zone, time2 timestamp with time zone)\n" +
                "RETURNS INTEGER LANGUAGE plpgsql AS $$ DECLARE BEGIN\n" +
                "\tRETURN TIMESTAMPDIFF(datepart, time1::TIMESTAMP, time2::TIMESTAMP);\n" +
                "End; $$;\n" +
                "\n" +
                "CREATE OR REPLACE FUNCTION TIMESTAMPDIFF(datepart VARCHAR, time1 VARCHAR, time2 VARCHAR)\n" +
                "RETURNS INTEGER LANGUAGE plpgsql AS $$ DECLARE BEGIN\n" +
                "\tRETURN TIMESTAMPDIFF(datepart, time1::TIMESTAMP, time2::TIMESTAMP);\n" +
                "End; $$;";
        printTips(funcName, sql);
    }

    private static void subTime() {
        String funcName = "SUBTIME";
        String sql = "CREATE OR REPLACE FUNCTION SUBTIME(time1 TIMESTAMP, time2 TIME) RETURNS VARCHAR LANGUAGE plpgsql AS $$ DECLARE BEGIN\n" +
                "\tRETURN time1 - time2;\n" +
                "End; $$;\n" +
                "\n" +
                "CREATE OR REPLACE FUNCTION SUBTIME(time1 TIMESTAMP, time2 INTEGER) RETURNS VARCHAR LANGUAGE plpgsql AS $$ DECLARE BEGIN\n" +
                "\tRETURN time1::TIMESTAMP - time2 * INTERVAL '1 SECOND';\n" +
                "End; $$;\n" +
                "\n" +
                "CREATE OR REPLACE FUNCTION SUBTIME(time1 TIMESTAMP, time2 VARCHAR) RETURNS VARCHAR LANGUAGE plpgsql AS $$ DECLARE BEGIN\n" +
                "\tIF POSITION('-' IN time2) > 0 THEN\n" +
                "\t\tRETURN NULL;\n" +
                "\tEND IF;\n" +
                "\tIF POSITION(':' IN time2) > 0 THEN\n" +
                "\t\tRETURN ADDTIME(time1, time2::TIME);\n" +
                "\tELSE\n" +
                "\t\tRETURN ADDTIME(time1, time2::INTEGER);\n" +
                "\tEND IF;\n" +
                "End; $$;\n" +
                "\n" +
                "CREATE OR REPLACE FUNCTION SUBTIME(time1 VARCHAR, time2 VARCHAR) RETURNS VARCHAR LANGUAGE plpgsql AS $$ DECLARE BEGIN\n" +
                "\tRETURN ADDTIME(time1::TIMESTAMP, time2);\n" +
                "End; $$;\n" +
                "\n" +
                "CREATE OR REPLACE FUNCTION SUBTIME(time1 VARCHAR, time2 INTEGER) RETURNS VARCHAR LANGUAGE plpgsql AS $$ DECLARE BEGIN\n" +
                "\tRETURN ADDTIME(time1::TIMESTAMP, time2);\n" +
                "End; $$;\n" +
                "\n" +
                "CREATE OR REPLACE FUNCTION SUBTIME(time1 TIMESTAMP WITH TIME ZONE, time2 INTEGER) RETURNS VARCHAR LANGUAGE plpgsql AS $$ DECLARE BEGIN\n" +
                "\tRETURN ADDTIME(time1::TIMESTAMP, time2);\n" +
                "End; $$;\n" +
                "\n" +
                "CREATE OR REPLACE FUNCTION SUBTIME(time1 TIMESTAMP WITH TIME ZONE, time2 VARCHAR) RETURNS VARCHAR LANGUAGE plpgsql AS $$ DECLARE BEGIN\n" +
                "\tRETURN ADDTIME(time1::TIMESTAMP, time2);\n" +
                "End; $$;\n";
        printTips(funcName, sql);
    }

    private static void addTime() {
        String funcName = "ADDTIME";
        String sql = "CREATE OR REPLACE FUNCTION ADDTIME(time1 TIMESTAMP, time2 TIME) RETURNS VARCHAR LANGUAGE plpgsql AS $$ DECLARE BEGIN\n" +
                "\tRETURN time1 + time2;\n" +
                "End; $$;\n" +
                "\n" +
                "CREATE OR REPLACE FUNCTION ADDTIME(time1 TIMESTAMP, time2 INTEGER) RETURNS VARCHAR LANGUAGE plpgsql AS $$ DECLARE BEGIN\n" +
                "\tRETURN time1::TIMESTAMP + time2 * INTERVAL '1 SECOND';\n" +
                "End; $$;\n" +
                "\n" +
                "CREATE OR REPLACE FUNCTION ADDTIME(time1 TIMESTAMP, time2 VARCHAR) RETURNS VARCHAR LANGUAGE plpgsql AS $$ DECLARE BEGIN\n" +
                "\tIF POSITION('-' IN time2) > 0 THEN\n" +
                "\t\tRETURN NULL;\n" +
                "\tEND IF;\n" +
                "\tIF POSITION(':' IN time2) > 0 THEN\n" +
                "\t\tRETURN ADDTIME(time1, time2::TIME);\n" +
                "\tELSE\n" +
                "\t\tRETURN ADDTIME(time1, time2::INTEGER);\n" +
                "\tEND IF;\n" +
                "End; $$;\n" +
                "\n" +
                "CREATE OR REPLACE FUNCTION ADDTIME(time1 VARCHAR, time2 VARCHAR) RETURNS VARCHAR LANGUAGE plpgsql AS $$ DECLARE BEGIN\n" +
                "\tRETURN ADDTIME(time1::TIMESTAMP, time2);\n" +
                "End; $$;\n" +
                "\n" +
                "CREATE OR REPLACE FUNCTION ADDTIME(time1 VARCHAR, time2 INTEGER) RETURNS VARCHAR LANGUAGE plpgsql AS $$ DECLARE BEGIN\n" +
                "\tRETURN ADDTIME(time1::TIMESTAMP, time2);\n" +
                "End; $$;\n" +
                "\n" +
                "CREATE OR REPLACE FUNCTION ADDTIME(time1 TIMESTAMP WITH TIME ZONE, time2 INTEGER) RETURNS VARCHAR LANGUAGE plpgsql AS $$ DECLARE BEGIN\n" +
                "\tRETURN ADDTIME(time1::TIMESTAMP, time2);\n" +
                "End; $$;\n" +
                "\n" +
                "CREATE OR REPLACE FUNCTION ADDTIME(time1 TIMESTAMP WITH TIME ZONE, time2 VARCHAR) RETURNS VARCHAR LANGUAGE plpgsql AS $$ DECLARE BEGIN\n" +
                "\tRETURN ADDTIME(time1::TIMESTAMP, time2);\n" +
                "End; $$;";
        printTips(funcName, sql);
    }

    private static void substringIndex() {
        String funcName = "SUBSTRING_INDEX";
        String sql = "CREATE OR REPLACE FUNCTION SUBSTRING_INDEX(str VARCHAR, delim VARCHAR, num INTEGER)\n" +
                "\tRETURNS VARCHAR\n" +
                "\tLANGUAGE plpgsql\n" +
                "AS\n" +
                "$$\n" +
                "DECLARE\n" +
                "\ttokens VARCHAR[];\n" +
                "\tlen INTEGER;\n" +
                "\tindexnum INTEGER;\n" +
                "BEGIN\n" +
                "\t--分割字符串成数组\n" +
                "\ttokens := string_to_array(str, delim);\n" +
                "\tIF num >= 0 THEN\n" +
                "\t\tRETURN array_to_string(tokens[1:num], delim);\n" +
                "\tELSE\n" +
                "\t\t--数组长度\n" +
                "\t\tlen := array_upper(tokens, 1);\n" +
                "\t\t--计算开始索引位置\n" +
                "\t\tindexnum := len - (num * -1) + 1;\n" +
                "\t\tRETURN array_to_string(tokens[indexnum:len], delim);\n" +
                "\tEND IF;\n" +
                "End;\n" +
                "$$";
        printTips(funcName, sql);
    }

    private static void instr() {
        String funcName = "INSTR";
        String sql  = "CREATE OR REPLACE FUNCTION INSTR(str1 VARCHAR, str2 VARCHAR)\n" +
                "\tRETURNS INTEGER\n" +
                "\tLANGUAGE plpgsql\n" +
                "AS\n" +
                "$$\n" +
                "DECLARE\n" +
                "BEGIN\n" +
                "\tRETURN position(str2 in str1); \n" +
                "End;\n" +
                "$$";
        printTips(funcName, sql);
    }

    private static void locate() {
        String funcName = "LOCATE";
        String sql  = "CREATE OR REPLACE FUNCTION LOCATE(str1 VARCHAR, str2 VARCHAR)\n" +
                "\tRETURNS INTEGER\n" +
                "\tLANGUAGE plpgsql\n" +
                "AS\n" +
                "$$\n" +
                "DECLARE\n" +
                "BEGIN\n" +
                "\tRETURN position(str1 in str2); \n" +
                "End;\n" +
                "$$";
        printTips(funcName, sql);
    }

    private static void strcmp() {
        String funcName = "STRCMP";
        String sql = "CREATE OR REPLACE FUNCTION STRCMP(str1 VARCHAR, str2 VARCHAR)\n" +
                "\tRETURNS INTEGER\n" +
                "\tLANGUAGE plpgsql\n" +
                "AS\n" +
                "$$\n" +
                "DECLARE\n" +
                "BEGIN\n" +
                "\tIF str1 < str2 THEN\n" +
                "\t\tRETURN -1;\n" +
                "\tEND IF;\n" +
                "\tIF str1 = str2 THEN\n" +
                "\t\tRETURN 0;\n" +
                "\tELSE \n" +
                "\t\tRETURN 1;\n" +
                "\tEND IF;\n" +
                "End;\n" +
                "$$";
        printTips(funcName, sql);
    }

    private static void yearWeek() {
        String funcName = "YEARWEEK";
        String  sql = "CREATE OR REPLACE FUNCTION YEARWEEK(ts DATE)\n" +
                "\tRETURNS VARCHAR\n" +
                "\tLANGUAGE plpgsql\n" +
                "AS\n" +
                "$$\n" +
                "DECLARE\n" +
                "BEGIN\n" +
                "\tIF ts IS NULL THEN\n" +
                "\t\tRETURN NULL;\n" +
                "\tEND IF;\n" +
                "\tRETURN CONCAT(EXTRACT(YEAR from ts), EXTRACT(WEEK from ts));\n" +
                "End;\n" +
                "$$";
        printTips(funcName, sql);
    }

    private static void makeDay() {
        String funcName = "MAKEDATE";
        String sql = "CREATE OR REPLACE FUNCTION MAKEDATE(y INTEGER, d INTEGER)\n" +
                "\tRETURNS DATE\n" +
                "\tLANGUAGE plpgsql\n" +
                "AS\n" +
                "$$\n" +
                "DECLARE\n" +
                "BEGIN\n" +
                "\tIF d <= 0 OR y <= 0 THEN\n" +
                "\t\tRETURN NULL;\n" +
                "\tELSE\n" +
                "\t\tRETURN CONCAT(y, '-01-01')::DATE + d - 1;\n" +
                "\tEND IF;\n" +
                "End;\n" +
                "$$";
        printTips(funcName, sql);
    }

    private static void printTips(String funcName, String sql) {
        log.warn("postgre默认不支持{}函数，可执行以下sql，自定义{}函数：\n{}", funcName, funcName, sql);
    }

    public static void timeDiff() {
        String funcName = "TIMEDIFF";
        String sql = "CREATE OR REPLACE FUNCTION TIMEDIFF(time1 timestamp, time2 timestamp)\n" +
                "\tRETURNS VARCHAR\n" +
                "\tLANGUAGE plpgsql\n" +
                "AS\n" +
                "$$\n" +
                "DECLARE\n" +
                "\t_hour INTEGER:=0;\n" +
                "\t_minute INTEGER:=0;\n" +
                "\t_second INTEGER:=0;\n" +
                "\tres VARCHAR;\n" +
                "BEGIN\n" +
                "\t_second := EXTRACT(EPOCH FROM AGE(time1, time2));\n" +
                "\t_hour := _second / 3600;\n" +
                "\t_second := abs(mod(_second, 3600));\n" +
                "\t_minute := _second / 60;\n" +
                "\t_second := mod(_second, 60);\n" +
                "\tres := CONCAT(_hour, ':', _minute, ':', _second);\n" +
                "\tRETURN res;\n" +
                "End;\n" +
                "$$;\n" +
                "\n" +
                "CREATE OR REPLACE FUNCTION TIMEDIFF(time1 timestamp with time zone, time2 timestamp with time zone)\n" +
                "RETURNS VARCHAR LANGUAGE plpgsql AS $$ DECLARE BEGIN\n" +
                "\tRETURN TIMEDIFF(time1::TIMESTAMP, time2::TIMESTAMP);\n" +
                "End; $$;\n" +
                "\n" +
                "CREATE OR REPLACE FUNCTION TIMEDIFF(time1 VARCHAR, time2 VARCHAR)\n" +
                "RETURNS VARCHAR LANGUAGE plpgsql AS $$ DECLARE BEGIN\n" +
                "\tRETURN TIMEDIFF(time1::TIMESTAMP, time2::TIMESTAMP);\n" +
                "End; $$;";
        printTips(funcName, sql);
    }

    public static void findInSet() {
        String funcName = "FIND_IN_SET";
        String sql = "CREATE OR REPLACE FUNCTION FIND_IN_SET(piv_str1 VARCHAR, piv_str2 VARCHAR, p_sep VARCHAR DEFAULT ',')\n" +
                "\tRETURNS INTEGER\n" +
                "\tLANGUAGE plpgsql\n" +
                "AS\n" +
                "$$\n" +
                "DECLARE\n" +
                "\tl_idx INTEGER := 0; -- 用于计算 piv_str2 中分隔符的位置\n" +
                "\tstr VARCHAR; -- 根据分隔符截取的子字符串 \n" +
                "\tpiv_str VARCHAR := piv_str2; -- 将 piv_str2 赋值给 piv_str \n" +
                "\tres INTEGER :=  0; -- 返回结果\n" +
                "BEGIN\n" +
                "-- 如果 piv_str 中没有分割符，直接判断 piv_str1 和 piv_str 是否相等，相等 res=1 \n" +
                "IF POSITION(p_sep IN piv_str) = 0 THEN \n" +
                "\tIF piv_str = piv_str1 THEN \n" +
                "\t\tres = 1; \n" +
                "\tEND IF; \n" +
                "ELSE \n" +
                "\t-- 循环按分隔符截取 piv_str\n" +
                "\tLOOP\n" +
                "\t\tl_idx = POSITION(p_sep IN piv_str);\n" +
                "\t\t-- 位置\n" +
                "\t\tres := res + 1; \n" +
                "\t\t-- 当 piv_str 中还有分隔符时 \n" +
                "\t\tIF l_idx > 0 THEN \n" +
                "\t\t\t-- 截取第一个分隔符前的字段str \n" +
                "\t\t\tstr = substr(piv_str, 1, l_idx - 1); \n" +
                "\t\t\t-- 判断 str 和 piv_str1 是否相等，相等 res=1 并结束循环判断 \n" +
                "\t\t\tIF str = piv_str1 THEN  \n" +
                "\t\t\t\tEXIT; \n" +
                "\t\t\tEND IF; \n" +
                "\t\t\tpiv_str := substr(piv_str, l_idx + length(p_sep)); \n" +
                "\t\tELSE\n" +
                "\t\t\t-- 当截取后的 piv_str 中不存在分割符时，判断 piv_str 和 piv_str1 是否相等，相 等 res=1 \n" +
                "\t\t\tIF piv_str != piv_str1 THEN \n" +
                "\t\t\t\tres = 0; \n" +
                "\t\t\tEND IF; \n" +
                "\t\t\t-- 无论最后是否相等，都跳出循环\n" +
                "\t\t\tEXIT; \n" +
                "\t\tEND IF; \n" +
                "\tEND LOOP; \n" +
                "-- 结束循环 \n" +
                "END IF; \n" +
                "-- 返回 res \n" +
                "RETURN res; \n" +
                "End;\n" +
                "$$";
        printTips(funcName, sql);
    }

    public static void periodAdd() {
        String funcName = "PERIOD_ADD";
        String sql = "CREATE OR REPLACE FUNCTION PERIOD_ADD(piv_str1 INTEGER, piv_str2 INTEGER) \n" +
                "\tRETURNS INTEGER\n" +
                "\tLANGUAGE plpgsql\n" +
                "AS\n" +
                "$$\n" +
                "DECLARE\n" +
                "\tyearVar INTEGER;\n" +
                "\tmonthVar INTEGER;\n" +
                "\ttotalMonth INTEGER;\n" +
                "BEGIN\n" +
                "\t-- 分割出年份和月份 \n" +
                "\tyearVar := piv_str1 / 100;\n" +
                "\tmonthVar := mod(piv_str1, 100); \n" +
                "\t-- 总月份\n" +
                "\ttotalMonth := monthVar + piv_str2 + yearVar * 12 ;\n" +
                "\t-- 计算年\n" +
                "\tyearVar := totalMonth / 12;\n" +
                "\t-- 计算月\n" +
                "\tmonthVar :=  mod(totalMonth, 12);\n" +
                "\n" +
                "\tIF monthVar = 0 THEN\n" +
                "\t\t monthVar := 12;\n" +
                "\t\t yearVar := yearVar - 1;\n" +
                "\tEND IF;\n" +
                "\tRETURN yearVar * 100 + monthVar;\n" +
                "End;\n" +
                "$$;";
        printTips(funcName, sql);
    }

    private static void periodDiff() {
        String funcName = "PERIOD_DIFF";
        String sql = "CREATE OR REPLACE FUNCTION PERIOD_DIFF(piv_str1 INTEGER, piv_str2 INTEGER) \n" +
                "\tRETURNS INTEGER\n" +
                "\tLANGUAGE plpgsql\n" +
                "AS\n" +
                "$$\n" +
                "DECLARE\n" +
                "\tyearVar INTEGER;\n" +
                "\tmonthVar INTEGER;\n" +
                "BEGIN\n" +
                "\t-- 相差的年份 \n" +
                "\tyearVar := piv_str1 / 100 - piv_str2 / 100;\n" +
                "\t-- 相差的月份\n" +
                "\tmonthVar := mod(piv_str1, 100) - mod(piv_str2, 100); \n" +
                "\tRETURN yearVar * 12 + monthVar;\n" +
                "End;\n" +
                "$$;";
        printTips(funcName, sql);
    }

}
