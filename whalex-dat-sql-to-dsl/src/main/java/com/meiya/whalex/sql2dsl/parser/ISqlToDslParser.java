package com.meiya.whalex.sql2dsl.parser;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl;

/**
 * sql解析
 *
 * @author 蔡荣桂
 * @date 2022/06/27
 * @project whalex-dat-sql
 */
public interface ISqlToDslParser<T> {

    SqlParser.Config config = SqlParser.configBuilder()
            .setQuotedCasing(Casing.UNCHANGED)
            .setUnquotedCasing(Casing.UNCHANGED)
            .setParserFactory(SqlDdlParserImpl.FACTORY)
            .setQuoting(Quoting.BACK_TICK)
            .build();

    T handle() throws SqlParseException;

    String getTableName();

//    T execute() throws Exception;

}
