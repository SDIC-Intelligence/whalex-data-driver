package com.meiya.whalex.db.entity.ani;

import com.meiya.whalex.keyword.KeyWordHandler;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * 系统关键字
 *
 * @author 黄河森
 * @date 2020/6/11
 * @project whalex-data-driver
 */
public class KeywordConstant implements KeyWordHandler {

    public static List<String> keywordPool  = new ArrayList<>();

    static {
        keywordPool.add("ctid");
        keywordPool.add("desc");
        keywordPool.add("DESC");
        keywordPool.add("cast");
        keywordPool.add("CAST");
        keywordPool.add("from");
        keywordPool.add("FROM");
        keywordPool.add("to");
        keywordPool.add("TO");
        keywordPool.add("user");
        keywordPool.add("USER");
        keywordPool.add("asc");
        keywordPool.add("ASC");
        keywordPool.add("WHERE");
        keywordPool.add("where");
    }

    public static String findKeyword(String fieldName, String doubleQuotationMarks) {
        if (keywordPool.contains(fieldName)) {
            if (StringUtils.isNotBlank(doubleQuotationMarks)) {
                return fieldName.toUpperCase();
            } else {
                return "\"" + fieldName.toUpperCase() + "\"";
            }
        } else {
            return fieldName;
        }
    }

    @Override
    public boolean isKeyWord(String field) {
        return keywordPool.contains(field);
    }

    @Override
    public String handler(String field) {
        return "\"" + field.toUpperCase() + "\"";
    }
}
