package com.meiya.whalex.sql2dsl.parser;

import com.meiya.whalex.db.entity.DropTableParamCondition;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.ddl.SqlDropTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;

import java.util.List;

/**
 * sql转dsl
 *
 * @author 蔡荣桂
 * @date 2023/02/02
 * @project whalex-dat-sql
 */
public class DropTableSqlToDslParser extends AbstractSqlToDslParser<DropTableParamCondition> {

    private String sql;

    private String tableName;

    private boolean ifExists;

    public DropTableSqlToDslParser(String sql) {
        this.sql = sql;
    }

    @Override
    public DropTableParamCondition handle() throws SqlParseException {

        SqlParser sqlParser = SqlParser.create(sql, config);
        SqlNode sqlNode = sqlParser.parseStmt();

        if(!(sqlNode instanceof SqlDropTable)) {
            throw new RuntimeException("不是drop table类型的sql");
        }

        SqlDropTable sqlDropTable = (SqlDropTable) sqlNode;
        ifExists = sqlDropTable.ifExists;
        //表名
        tableName = getTableName(sqlDropTable.name);

        DropTableParamCondition dropTableParamCondition = new DropTableParamCondition();
        dropTableParamCondition.setIfExists(ifExists);
        return dropTableParamCondition;
    }

    @Override
    public String getTableName() {
        return tableName;
    }


    private String getTableName(SqlNode sqlNode) {
        List<String> names = getNames(sqlNode);
        if(names.size() == 2) {
            return names.get(1);
        }else {
            return names.get(0);
        }
    }

    private List<String> getNames(SqlNode sqlNode) {
        SqlIdentifier sqlIdentifier = (SqlIdentifier) sqlNode;
        return sqlIdentifier.names;
    }

}
