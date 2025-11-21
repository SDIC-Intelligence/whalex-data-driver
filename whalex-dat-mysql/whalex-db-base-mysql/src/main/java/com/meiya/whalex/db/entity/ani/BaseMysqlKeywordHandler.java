package com.meiya.whalex.db.entity.ani;
import com.meiya.whalex.sql.keyword.RdbmsKeyWordHandler;

/**
 * 系统关键字
 *
 * @author 黄河森
 * @date 2020/6/11
 * @project whalex-data-driver
 */
public class BaseMysqlKeywordHandler extends RdbmsKeyWordHandler {


    @Override
    public boolean isKeyWord(String field) {
        if(field.contains("-")) {
            return true;
        }

        String keyword = field.toUpperCase();
        return keywordPool.contains(keyword);
    }

    @Override
    public String handler(String field) {
       return "`" + field + "`";
    }
}
