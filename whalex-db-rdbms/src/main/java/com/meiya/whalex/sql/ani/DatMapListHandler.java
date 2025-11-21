package com.meiya.whalex.sql.ani;

import lombok.Data;
import org.apache.commons.dbutils.RowProcessor;
import org.apache.commons.dbutils.handlers.MapListHandler;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Data
public class DatMapListHandler extends MapListHandler {

    List<Cell> headList = new ArrayList<>();

    public DatMapListHandler(RowProcessor convert) {
        super(convert);
    }

    @Override
    public List<Map<String, Object>> handle(ResultSet rs) throws SQLException {

        ResultSetMetaData resultSetMetaData = rs.getMetaData();
        int cols = resultSetMetaData.getColumnCount();
        for(int i = 1; i <= cols; ++i) {
            String columnName = resultSetMetaData.getColumnLabel(i);
            if (null == columnName || 0 == columnName.length()) {
                columnName = resultSetMetaData.getColumnName(i);
            }
            // 获取类型
            String columnTypeName = resultSetMetaData.getColumnTypeName(i);

            Cell cell = new Cell();
            cell.setFieldName(columnName);
            cell.setDataType(columnTypeName);
            headList.add(cell);
        }

        return super.handle(rs);
    }
}
