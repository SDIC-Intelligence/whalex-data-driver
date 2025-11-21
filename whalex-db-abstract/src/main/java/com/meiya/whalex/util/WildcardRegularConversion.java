package com.meiya.whalex.util;

import com.meiya.whalex.util.collection.CollectionUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * 通配符查询转换为正则表达式
 *
 * @author 黄河森
 * @date 2022/3/4
 * @package com.meiya.whalex.util
 * @project whalex-data-driver
 */

public class WildcardRegularConversion {

    /**
     * 匹配过滤表名
     *
     * @param wildcardStr
     * @param tableNames
     * @return
     */
    public static List<Map<String, Object>> matchFilter(String wildcardStr, List<Map<String, Object>> tableNames) {
        return matchFilter(wildcardStr, tableNames, "tableName");
    }

    /**
     * 匹配过滤数据库名
     *
     * @param wildcardStr
     * @param databaseNames
     * @return
     */
    public static List<Map<String, Object>> matchFilterDatabaseName(String wildcardStr, List<Map<String, Object>> databaseNames) {
        return matchFilter(wildcardStr, databaseNames, "databaseName");
    }

    public static List<Map<String, Object>> matchFilter(String wildcardStr, List<Map<String, Object>> rows, String key) {
        if (StringUtils.isBlank(wildcardStr) || CollectionUtils.isEmpty(rows)) {
            return rows;
        }
        if (!StringUtils.startsWithIgnoreCase(wildcardStr, "*") && !StringUtils.startsWithIgnoreCase(wildcardStr, "?")) {
            wildcardStr = "^" + wildcardStr;
        }
        if (!StringUtils.endsWithIgnoreCase(wildcardStr, "*") && !StringUtils.endsWithIgnoreCase(wildcardStr, "?")) {
            wildcardStr = wildcardStr + "$";
        }
        wildcardStr = StringUtils.replaceEach(wildcardStr, new String[]{"?", "*"}, new String[]{".", ".*"});
        Pattern compile = Pattern.compile(wildcardStr);
        List<Map<String, Object>> collect = rows.stream().filter(tableName -> compile.matcher((String) tableName.get(key)).find())
                .collect(Collectors.toList());
        return collect;
    }

}
