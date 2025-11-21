package com.meiya.whalex.jdbc.parser;

import com.meiya.whalex.db.entity.PageResult;

/**
 * sql解析
 *
 * @author 蔡荣桂
 * @date 2022/06/27
 * @project whalex-dat-sql
 */
public interface ISqlParser<T> {

    T execute() throws Exception;

    PageResult executeAndGetPageResult() throws Exception;
}
