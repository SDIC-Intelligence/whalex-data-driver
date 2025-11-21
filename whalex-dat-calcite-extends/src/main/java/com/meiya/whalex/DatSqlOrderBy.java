package com.meiya.whalex;

import com.sun.org.apache.bcel.internal.generic.I2F;
import lombok.Data;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Calcite 扩展 Order By 解析对象，增加 forUpdate 对象
 *
 * @author 黄河森
 * @date 2023/6/7
 * @package com.meiya.whalex
 * @project whalex-data-driver
 */
public class DatSqlOrderBy extends SqlOrderBy {

    public static final SqlSpecialOperator DAT_ORDER_BY_OPERATOR = new DatSqlOrderBy.Operator() {
        @Override
        public SqlCall createCall(@Nullable SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
            return new SqlOrderBy(pos, operands[0], (SqlNodeList)operands[1], operands[2], operands[3]);
        }
    };

    @Nullable
    public final boolean forUpdate;

    public DatSqlOrderBy(SqlParserPos pos, SqlNode query, SqlNodeList orderList, @Nullable SqlNode offset, @Nullable SqlNode fetch, @Nullable boolean forUpdate) {
        super(pos, query, orderList, offset, fetch);
        this.forUpdate = forUpdate;
    }

    public boolean isForUpdate() {
        return forUpdate;
    }

    @Override
    public SqlOperator getOperator() {
        return DAT_ORDER_BY_OPERATOR;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        DAT_ORDER_BY_OPERATOR.unparse(writer, this, leftPrec, rightPrec);
    }

    private static class Operator extends SqlSpecialOperator {
        private Operator() {
            super("ORDER BY", SqlKind.ORDER_BY, 0);
        }

        @Override
        public SqlSyntax getSyntax() {
            return SqlSyntax.POSTFIX;
        }

        @Override
        public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
            DatSqlOrderBy orderBy = (DatSqlOrderBy)call;
            SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.ORDER_BY);
            orderBy.query.unparse(writer, this.getLeftPrec(), this.getRightPrec());
            if (orderBy.orderList != SqlNodeList.EMPTY) {
                writer.sep(this.getName());
                writer.list(SqlWriter.FrameTypeEnum.ORDER_BY_LIST, SqlWriter.COMMA, orderBy.orderList);
            }

            if (orderBy.offset != null || orderBy.fetch != null) {
                writer.fetchOffset(orderBy.fetch, orderBy.offset);
            }

            if (orderBy.forUpdate) {
                writer.sep("FOR UPDATE");
            }

            writer.endList(frame);
        }
    }
}
