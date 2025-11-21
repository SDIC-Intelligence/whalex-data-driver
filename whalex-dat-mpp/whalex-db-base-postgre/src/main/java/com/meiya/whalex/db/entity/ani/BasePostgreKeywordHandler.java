package com.meiya.whalex.db.entity.ani;
import com.meiya.whalex.sql.keyword.RdbmsKeyWordHandler;

/**
 * 系统关键字
 *
 * @author 蔡荣桂
 * @date 2022/11/15
 * @project whalex-data-driver
 */
public class BasePostgreKeywordHandler extends RdbmsKeyWordHandler {


    @Override
    public boolean isKeyWord(String field) {
        String keyword = field.toUpperCase();
        return keywordPool.contains(keyword);
    }

    /**
     * postgre关键字必须大定加双引号
     * @param field
     * @return
     */
    @Override
    public String handler(String field) {
        return "\"" + field.toUpperCase() + "\"";
    }
}
