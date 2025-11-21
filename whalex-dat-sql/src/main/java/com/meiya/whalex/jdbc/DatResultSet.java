package com.meiya.whalex.jdbc;

import cn.hutool.core.date.DateTime;
import cn.hutool.core.date.DateUtil;
import com.meiya.whalex.jdbc.bean.DatBlob;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.Objects;


/**
 * dat结果集对象
 *
 * @author 蔡荣桂
 * @date 2022/06/27
 * @project whalex-dat-sql
 */
@Slf4j
public class DatResultSet implements ResultSet {

    private List<Map> rows;

    private List<String> schema;

    private int cursor = -1;

    private Map<String, Object> currentData;

    private Object value;

    private boolean currentRowFirstFetchValue;

    private int fetchSize = 0;

    public DatResultSet(List<Map> rows, List<String> schema) {
        this.rows = rows;
        this.schema = schema;
    }

    public DatResultSet() {
        this.rows = new ArrayList<>();
        this.schema = new ArrayList<>();
    }

    public void addResultSet(DatResultSet resultSet) {
        if(resultSet != null) {
            List<Map> rows = resultSet.rows;
            if(CollectionUtils.isNotEmpty(rows)) {
                this.rows.addAll(rows);
            }
        }
    }

    @Override
    public boolean next() throws SQLException {
        cursor++;
        if (rows.size() > cursor) {
            currentRowFirstFetchValue = true;
            currentData = rows.get(cursor);
            return true;
        }
        return false;
    }

    @Override
    public void close() throws SQLException {

    }

    @Override
    public boolean wasNull() throws SQLException {
        if(currentRowFirstFetchValue) {
            return false;
        }

        return Objects.isNull(value);
    }

    private Object getValue(int index) {
        value = currentData.get(schema.get(index));
        currentRowFirstFetchValue = false;
        return value;
    }

    private Object getValue(String columnLabel) {
        value = currentData.get(columnLabel);
        currentRowFirstFetchValue = false;
        return value;
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        Object value = getValue(columnIndex - 1);
        if (value == null) {
            return null;
        }
        return value.toString();
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        Object value = getValue(columnIndex - 1);
        if (value == null) {
            return false;
        }

        if(value instanceof Number) {
            Number number = (Number) value;
            return number.floatValue() > 0;
        }

        return Boolean.valueOf(value.toString());

    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        Object value = getValue(columnIndex - 1);
        if (value == null) {
            return 0;
        }
        return Byte.parseByte(value.toString());
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {

        Object value = getValue(columnIndex - 1);
        if (value == null) {
            return 0;
        }
        return Short.parseShort(value.toString());

    }

    @Override
    public int getInt(int columnIndex) throws SQLException {

        Object value = getValue(columnIndex - 1);
        if (value == null) {
            return 0;
        }

        if(StringUtils.isBlank(value.toString())) {
            return 0;
        }

        return Integer.parseInt(value.toString());

    }

    @Override
    public long getLong(int columnIndex) throws SQLException {

        Object value = getValue(columnIndex - 1);
        if (value == null) {
            return 0;
        }

        if(StringUtils.isBlank(value.toString())) {
            return 0;
        }

        return Long.parseLong(value.toString());

    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {

        Object value = getValue(columnIndex - 1);
        if (value == null) {
            return 0;
        }

        if(StringUtils.isBlank(value.toString())) {
            return 0;
        }

        return Float.parseFloat(value.toString());

    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {

        Object value = getValue(columnIndex - 1);
        if (value == null) {
            return 0;
        }

        if(StringUtils.isBlank(value.toString())) {
            return 0;
        }

        return Double.parseDouble(value.toString());

    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        Object value = getValue(columnIndex - 1);
        if (value == null) {
            return null;
        }

        if(StringUtils.isBlank(value.toString())) {
            return null;
        }

        BigDecimal bigDecimal = new BigDecimal(value.toString());
        return bigDecimal.setScale(scale);
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        Object value = getValue(columnIndex - 1);
        if (value == null) {
            return null;
        }

        if (value instanceof byte[]) {
            return (byte[]) value;
        }
        return null;

    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        Object value = getValue(columnIndex - 1);

        if (value == null) {
            return null;
        }

        if(value instanceof Date) {
            return (Date) value;
        }

        if(value instanceof  java.util.Date) {
            java.util.Date date = (java.util.Date) value;
            return new Date(date.getTime());
        }

        if(value instanceof LocalDateTime) {
            LocalDateTime localDateTime = (LocalDateTime) value;
            new Date(Timestamp.valueOf(localDateTime).getTime());
        }

        if(value instanceof LocalDate) {
            LocalDate localDate = (LocalDate) value;
            LocalDateTime localDateTime = localDate.atStartOfDay();
            new Date(Timestamp.valueOf(localDateTime).getTime());
        }

        String str = value.toString();
        DateTime dateTime = DateUtil.parse(str);
        return new Date(dateTime.getTime());

    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        log.error(this.getClass().getName() + "#" + "getTime 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        Object value = getValue(columnIndex - 1);

        if (value == null) {
            return null;
        }

        if(value instanceof  Timestamp) {
            return (Timestamp) value;
        }

        if(value instanceof  java.util.Date) {
            java.util.Date date = (java.util.Date) value;
            return new Timestamp(date.getTime());
        }

        if(value instanceof LocalDateTime) {
            LocalDateTime localDateTime = (LocalDateTime) value;
            return Timestamp.valueOf(localDateTime);
        }

        if(value instanceof LocalDate) {
            LocalDate localDate = (LocalDate) value;
            LocalDateTime localDateTime = localDate.atStartOfDay();
            return Timestamp.valueOf(localDateTime);
        }

        String str = value.toString();
        DateTime dateTime = DateUtil.parse(str);
        return new Timestamp(dateTime.getTime());
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        log.error(this.getClass().getName() + "#" + "getAsciiStream 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        log.error(this.getClass().getName() + "#" + "getUnicodeStream 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        log.error(this.getClass().getName() + "#" + "getBinaryStream 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public String getString(String columnLabel) throws SQLException {

        Object value = getValue(columnLabel);
        if (value == null) {
            return null;
        }
        return value.toString();
    }


    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        Object value = getValue(columnLabel);
        if (value == null) {
            return false;
        }

        if(value instanceof Number) {
            Number number = (Number) value;
            return number.floatValue() > 0;
        }

        return Boolean.valueOf(value.toString());
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        Object value = getValue(columnLabel);
        if (value == null) {
            return 0;
        }
        return Byte.valueOf(value.toString());
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        Object value = getValue(columnLabel);
        if (value == null) {
            return 0;
        }
        return Short.valueOf(value.toString());
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {

        Object value = getValue(columnLabel);
        if (value == null) {
            return 0;
        }

        if(StringUtils.isBlank(value.toString())) {
            return 0;
        }

        return Double.valueOf(value.toString()).intValue();
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        Object value = getValue(columnLabel);
        if (value == null) {
            return 0;
        }

        if(StringUtils.isBlank(value.toString())) {
            return 0;
        }

        return Long.valueOf(value.toString());
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        Object value = getValue(columnLabel);
        if (value == null) {
            return 0;
        }

        if(StringUtils.isBlank(value.toString())) {
            return 0;
        }

        return Float.valueOf(value.toString());
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        Object value = getValue(columnLabel);
        if (value == null) {
            return 0;
        }

        if(StringUtils.isBlank(value.toString())) {
            return 0;
        }

        return Double.valueOf(value.toString());
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        Object value = getValue(columnLabel);
        if (value == null) {
            return null;
        }

        if(StringUtils.isBlank(value.toString())) {
            return null;
        }

        BigDecimal bigDecimal = new BigDecimal(value.toString());
        return  bigDecimal.setScale(scale);
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        Object value = getValue(columnLabel);
        if (value == null) {
            return null;
        }
        if (value instanceof byte[]) {
            return (byte[]) value;
        }
        return null;
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {

        Object value = getValue(columnLabel);

        if (value == null) {
            return null;
        }

        if(value instanceof Date) {
            return (Date) value;
        }

        if(value instanceof  java.util.Date) {
            java.util.Date date = (java.util.Date) value;
            return new Date(date.getTime());
        }

        if(value instanceof LocalDateTime) {
            LocalDateTime localDateTime = (LocalDateTime) value;
            new Date(Timestamp.valueOf(localDateTime).getTime());
        }

        if(value instanceof LocalDate) {
            LocalDate localDate = (LocalDate) value;
            LocalDateTime localDateTime = localDate.atStartOfDay();
            new Date(Timestamp.valueOf(localDateTime).getTime());
        }

        String str = value.toString();
        DateTime dateTime = DateUtil.parse(str);
        return new Date(dateTime.getTime());

    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        Object value = getValue(columnLabel);
        if (value == null) {
            return null;
        }
        return Time.valueOf(value.toString());
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        Object value = getValue(columnLabel);

        if (value == null) {
            return null;
        }

        if(value instanceof  Timestamp) {
            return (Timestamp) value;
        }

        if(value instanceof  java.util.Date) {
            java.util.Date date = (java.util.Date) value;
            return new Timestamp(date.getTime());
        }

        if(value instanceof LocalDateTime) {
            LocalDateTime localDateTime = (LocalDateTime) value;
            return Timestamp.valueOf(localDateTime);
        }

        if(value instanceof LocalDate) {
            LocalDate localDate = (LocalDate) value;
            LocalDateTime localDateTime = localDate.atStartOfDay();
            return Timestamp.valueOf(localDateTime);
        }


        String str = value.toString();
        DateTime dateTime = DateUtil.parse(str);
        return new Timestamp(dateTime.getTime());
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        log.error(this.getClass().getName() + "#" + "getAsciiStream 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        log.error(this.getClass().getName() + "#" + "getUnicodeStream 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        log.error(this.getClass().getName() + "#" + "getBinaryStream 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
    }

    @Override
    public String getCursorName() throws SQLException {
        log.error(this.getClass().getName() + "#" + "getCursorName 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return new DatResultSetMetaData(schema);
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        return getValue(columnIndex - 1);
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return getValue(columnLabel);
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        for (int i = 0; i < schema.size(); i++) {
            if (schema.get(i).equalsIgnoreCase(columnLabel)) {
                return i;
            }
        }
        return -1;
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        log.error(this.getClass().getName() + "#" + "getCharacterStream 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        log.error(this.getClass().getName() + "#" + "getCharacterStream 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        Object value = getValue(columnIndex - 1);
        if (value == null) {
            return null;
        }
        BigDecimal bigDecimal = new BigDecimal(value.toString());
        return bigDecimal;
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        Object value = getValue(columnLabel);
        if (value == null) {
            return null;
        }

        if(StringUtils.isBlank(value.toString())) {
            return null;
        }

        BigDecimal bigDecimal = new BigDecimal(value.toString());
        return bigDecimal;
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        return cursor == -1;
    }

    @Override
    public boolean isAfterLast() throws SQLException {
       return cursor == rows.size();
    }

    @Override
    public boolean isFirst() throws SQLException {
        return cursor == 0;
    }

    @Override
    public boolean isLast() throws SQLException {
        return cursor == rows.size() - 1;
    }

    @Override
    public void beforeFirst() throws SQLException {
        cursor = - 1;
        currentRowFirstFetchValue = true;
        value = null;
        currentData = null;
    }

    @Override
    public void afterLast() throws SQLException {
        cursor = rows.size();
        currentRowFirstFetchValue = false;
        value = null;
        currentData = null;
    }

    @Override
    public boolean first() throws SQLException {
        beforeFirst();
        return next();
    }

    @Override
    public boolean last() throws SQLException {
        cursor = rows.size() - 1;
        return next();
    }

    @Override
    public int getRow() throws SQLException {
        return cursor + 1;
    }

    @Override
    public boolean absolute(int row) throws SQLException {


        // 4 = 5 - 1
        if(row > rows.size()) {
            afterLast();
            return false;
        }else if(row > 0){
            cursor = row - 1  - 1;
            return next();
        }else if (row == 0){
            beforeFirst();
            return false;
            // -5
        }else if (row >= -rows.size()){
            cursor = rows.size() + row + 1;
            return previous();
        }else {
            beforeFirst();
            return false;
        }
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        boolean result = false;

        if(rows > 0) {
            if(isAfterLast()) {
                return false;
            }
            for (int i = 0; i < rows; i++) {
                result = next();
                if(!result) {
                    return false;
                }
            }
        }

        if(rows < 0) {
            if(isBeforeFirst()) {
                return false;
            }
            for (int i = 0; i < -rows; i++) {
                result = previous();
                if(!result) {
                    return false;
                }
            }
        }

        return result;
    }

    @Override
    public boolean previous() throws SQLException {
        cursor--;
        if (cursor >= 0) {
            currentRowFirstFetchValue = true;
            currentData = rows.get(cursor);
            return true;
        }
        return false;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        if(rows < 0) {
            throw new SQLException("fetchSize不能小于0");
        }
        fetchSize = rows;
    }

    @Override
    public int getFetchSize() throws SQLException {
        return fetchSize;
    }

    @Override
    public int getType() throws SQLException {
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public int getConcurrency() throws SQLException {
        log.error(this.getClass().getName() + "#" + "getConcurrency 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public boolean rowUpdated() throws SQLException {
        log.error(this.getClass().getName() + "#" + "rowUpdated 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public boolean rowInserted() throws SQLException {
        log.error(this.getClass().getName() + "#" + "rowInserted 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        log.error(this.getClass().getName() + "#" + "rowDeleted 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void updateNull(int columnIndex) throws SQLException {
        log.error(this.getClass().getName() + "#" + "updateNull 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        log.error(this.getClass().getName() + "#" + "updateBoolean 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
        log.error(this.getClass().getName() + "#" + "updateByte 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
        log.error(this.getClass().getName() + "#" + "updateShort 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
        log.error(this.getClass().getName() + "#" + "updateInt 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {
        log.error(this.getClass().getName() + "#" + "updateLong 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
        log.error(this.getClass().getName() + "#" + "updateFloat 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {
        log.error(this.getClass().getName() + "#" + "updateDouble 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        log.error(this.getClass().getName() + "#" + "updateBigDecimal 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException {
        log.error(this.getClass().getName() + "#" + "updateString 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        log.error(this.getClass().getName() + "#" + "updateBytes 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void updateDate(int columnIndex, Date x) throws SQLException {
        log.error(this.getClass().getName() + "#" + "updateDate 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException {
        log.error(this.getClass().getName() + "#" + "updateTime 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        log.error(this.getClass().getName() + "#" + "updateTimestamp 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        log.error(this.getClass().getName() + "#" + "updateAsciiStream 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        log.error(this.getClass().getName() + "#" + "updateBinaryStream 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        log.error(this.getClass().getName() + "#" + "updateCharacterStream 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        log.error(this.getClass().getName() + "#" + "updateObject 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {
        log.error(this.getClass().getName() + "#" + "updateObject 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void updateNull(String columnLabel) throws SQLException {
        log.error(this.getClass().getName() + "#" + "updateNull 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        log.error(this.getClass().getName() + "#" + "updateBoolean 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {
        log.error(this.getClass().getName() + "#" + "updateByte 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {
        log.error(this.getClass().getName() + "#" + "updateShort 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {
        log.error(this.getClass().getName() + "#" + "updateInt 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {
        log.error(this.getClass().getName() + "#" + "updateLong 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {
        log.error(this.getClass().getName() + "#" + "updateFloat 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {
        log.error(this.getClass().getName() + "#" + "updateDouble 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        log.error(this.getClass().getName() + "#" + "updateBigDecimal 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException {
        log.error(this.getClass().getName() + "#" + "updateString 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        log.error(this.getClass().getName() + "#" + "updateBytes 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException {
        log.error(this.getClass().getName() + "#" + "updateDate 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {
        log.error(this.getClass().getName() + "#" + "updateTime 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        log.error(this.getClass().getName() + "#" + "updateTimestamp 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
        log.error(this.getClass().getName() + "#" + "updateAsciiStream 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
        log.error(this.getClass().getName() + "#" + "updateBinaryStream 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
        log.error(this.getClass().getName() + "#" + "updateCharacterStream 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        log.error(this.getClass().getName() + "#" + "updateObject 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {
        log.error(this.getClass().getName() + "#" + "updateObject 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void insertRow() throws SQLException {
        log.error(this.getClass().getName() + "#" + "insertRow 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void updateRow() throws SQLException {
        log.error(this.getClass().getName() + "#" + "updateRow 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void deleteRow() throws SQLException {
        log.error(this.getClass().getName() + "#" + "deleteRow 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void refreshRow() throws SQLException {
        log.error(this.getClass().getName() + "#" + "refreshRow 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void cancelRowUpdates() throws SQLException {
        log.error(this.getClass().getName() + "#" + "cancelRowUpdates 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        log.error(this.getClass().getName() + "#" + "moveToInsertRow 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
        log.error(this.getClass().getName() + "#" + "moveToCurrentRow 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public Statement getStatement() throws SQLException {
        log.error(this.getClass().getName() + "#" + "getStatement 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        log.error(this.getClass().getName() + "#" + "getObject 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        log.error(this.getClass().getName() + "#" + "getRef 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        Object value = getValue(columnIndex - 1);
        if (value == null) {
            return null;
        }

        if (value instanceof Blob) {
            return (Blob) value;
        }

        if (value instanceof byte[]) {
            return new DatBlob((byte[]) value);
        }

        return null;
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        log.error(this.getClass().getName() + "#" + "getClob 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
        log.error(this.getClass().getName() + "#" + "getArray 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        log.error(this.getClass().getName() + "#" + "getObject 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        log.error(this.getClass().getName() + "#" + "getRef 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        Object value = getValue(columnLabel);
        if (value == null) {
            return null;
        }

        if (value instanceof Blob) {
            return (Blob) value;
        }

        if (value instanceof byte[]) {
            return new DatBlob((byte[]) value);
        }

        return null;

    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        log.error(this.getClass().getName() + "#" + "getClob 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
        log.error(this.getClass().getName() + "#" + "getArray 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        return getDate(columnIndex);
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        return getDate(columnLabel);
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        log.error(this.getClass().getName() + "#" + "getTime 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        log.error(this.getClass().getName() + "#" + "getTime 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        return getTimestamp(columnIndex);
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        return getTimestamp(columnLabel);
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        log.error(this.getClass().getName() + "#" + "getURL 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        log.error(this.getClass().getName() + "#" + "getURL 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {
        log.error(this.getClass().getName() + "#" + "updateRef 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {
        log.error(this.getClass().getName() + "#" + "updateRef 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        log.error(this.getClass().getName() + "#" + "updateBlob 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        log.error(this.getClass().getName() + "#" + "updateBlob 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException {
        log.error(this.getClass().getName() + "#" + "updateClob 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {
        log.error(this.getClass().getName() + "#" + "updateClob 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void updateArray(int columnIndex, Array x) throws SQLException {
        log.error(this.getClass().getName() + "#" + "updateArray 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {
        log.error(this.getClass().getName() + "#" + "updateArray 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        log.error(this.getClass().getName() + "#" + "getRowId 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        log.error(this.getClass().getName() + "#" + "getRowId 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        log.error(this.getClass().getName() + "#" + "updateRowId 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        log.error(this.getClass().getName() + "#" + "updateRowId 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public int getHoldability() throws SQLException {
        log.error(this.getClass().getName() + "#" + "getHoldability 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public boolean isClosed() throws SQLException {
        return false;
    }

    @Override
    public void updateNString(int columnIndex, String nString) throws SQLException {
        log.error(this.getClass().getName() + "#" + "updateNString 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException {
        log.error(this.getClass().getName() + "#" + "updateNString 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        log.error(this.getClass().getName() + "#" + "updateNClob 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        log.error(this.getClass().getName() + "#" + "updateNClob 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        log.error(this.getClass().getName() + "#" + "getNClob 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        log.error(this.getClass().getName() + "#" + "getNClob 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        log.error(this.getClass().getName() + "#" + "getSQLXML 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        log.error(this.getClass().getName() + "#" + "getSQLXML 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        log.error(this.getClass().getName() + "#" + "updateSQLXML 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        log.error(this.getClass().getName() + "#" + "updateSQLXML 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        return getString(columnIndex);
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        return getString(columnLabel);
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        log.error(this.getClass().getName() + "#" + "getNCharacterStream 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        log.error(this.getClass().getName() + "#" + "getNCharacterStream 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        log.error(this.getClass().getName() + "#" + "updateNCharacterStream 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        log.error(this.getClass().getName() + "#" + "updateNCharacterStream 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        log.error(this.getClass().getName() + "#" + "updateAsciiStream 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        log.error(this.getClass().getName() + "#" + "updateBinaryStream 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        log.error(this.getClass().getName() + "#" + "updateCharacterStream 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        log.error(this.getClass().getName() + "#" + "updateAsciiStream 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        log.error(this.getClass().getName() + "#" + "updateBinaryStream 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        log.error(this.getClass().getName() + "#" + "updateCharacterStream 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        log.error(this.getClass().getName() + "#" + "updateBlob 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        log.error(this.getClass().getName() + "#" + "updateBlob 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        log.error(this.getClass().getName() + "#" + "updateClob 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        log.error(this.getClass().getName() + "#" + "updateClob 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        log.error(this.getClass().getName() + "#" + "updateNClob 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        log.error(this.getClass().getName() + "#" + "updateNClob 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        log.error(this.getClass().getName() + "#" + "updateNCharacterStream 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        log.error(this.getClass().getName() + "#" + "updateNCharacterStream 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        log.error(this.getClass().getName() + "#" + "updateAsciiStream 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        log.error(this.getClass().getName() + "#" + "updateBinaryStream 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        log.error(this.getClass().getName() + "#" + "updateCharacterStream 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        log.error(this.getClass().getName() + "#" + "updateAsciiStream 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        log.error(this.getClass().getName() + "#" + "updateBinaryStream 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        log.error(this.getClass().getName() + "#" + "updateCharacterStream 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        log.error(this.getClass().getName() + "#" + "updateBlob 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        log.error(this.getClass().getName() + "#" + "updateBlob 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        log.error(this.getClass().getName() + "#" + "updateClob 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        log.error(this.getClass().getName() + "#" + "updateClob 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        log.error(this.getClass().getName() + "#" + "updateNClob 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        log.error(this.getClass().getName() + "#" + "updateNClob 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {

        if (type == null) {
            throw new SQLException("类型不能为空");
        }

        if (type.equals(String.class)) {
            return (T) this.getString(columnIndex);
        }

        if (type.equals(BigDecimal.class)) {
            return (T) this.getBigDecimal(columnIndex);
        }

        if (type.equals(Boolean.class) || type.equals(Boolean.TYPE)) {
            boolean aBoolean = this.getBoolean(columnIndex);
            return (T) Boolean.valueOf(aBoolean);
        }

        if (type.equals(Byte.class) || type.equals(Byte.TYPE)) {
            byte aByte = getByte(columnIndex);
            return (T) Byte.valueOf(aByte);

        }

        if (type.equals(Short.class) || type.equals(Short.TYPE)) {
            short aShort = getShort(columnIndex);
            return (T) Short.valueOf(aShort);
        }

        if (type.equals(Integer.class) || type.equals(Integer.TYPE)) {
            int anInt = getInt(columnIndex);
            return (T) Integer.valueOf(anInt);
        }


        if (type.equals(Long.class) ||  type.equals(Long.TYPE)) {
            long aLong = getLong(columnIndex);
            return (T) Long.valueOf(aLong);
        }

        if (type.equals(Float.class) || type.equals(Float.TYPE)) {
            float aFloat = getFloat(columnIndex);
            return (T) Float.valueOf(aFloat);
        }

        if (type.equals(Double.class) || type.equals(Double.TYPE)) {
            double aDouble = getDouble(columnIndex);
            return (T) Double.valueOf(aDouble);
        }

        if (type.equals(byte[].class)) {
            return (T) this.getBytes(columnIndex);
        }

        if (type.equals(Date.class)) {
            return (T) this.getDate(columnIndex);
        }

        if (type.equals(Time.class)) {
            return (T) this.getTime(columnIndex);
        }

        if (type.equals(Timestamp.class)) {
            return (T) this.getTimestamp(columnIndex);
        }

        if (type.equals(Array.class)) {
            return (T) this.getArray(columnIndex);
        }

        if (type.equals(Ref.class)) {
            return (T) this.getRef(columnIndex);
        }

        if (type.equals(URL.class)) {
            return (T) this.getURL(columnIndex);
        }

        if (type.equals(RowId.class)) {
            return (T) this.getRowId(columnIndex);
        }

        if (type.equals(NClob.class)) {
            return (T) this.getNClob(columnIndex);
        }

        if (type.equals(SQLXML.class)) {
            return (T) this.getSQLXML(columnIndex);
        }

        if (type.equals(LocalDate.class)) {
            return (T) this.getLocalDate(columnIndex);
        }

        if (type.equals(LocalDateTime.class)) {
            return (T) this.getLocalDateTime(columnIndex);
        }

        if (type.equals(LocalTime.class)) {
            return (T) this.getLocalTime(columnIndex);
        }

        return (T) getValue(columnIndex - 1);
    }

    private LocalDateTime getLocalDateTime(int columnIndex) throws SQLException {

        Timestamp timestamp = getTimestamp(columnIndex);

        if(timestamp == null) {
            return null;
        }

        return timestamp.toLocalDateTime();

    }

    private LocalDateTime getLocalDateTime(String columnLabel) throws SQLException {

        Timestamp timestamp = getTimestamp(columnLabel);

        if(timestamp == null) {
            return null;
        }

        return timestamp.toLocalDateTime();
    }


    private LocalTime getLocalTime(int columnIndex) throws SQLException {

        Timestamp timestamp = getTimestamp(columnIndex);

        if(timestamp == null) {
            return null;
        }

        LocalDateTime localDateTime = timestamp.toLocalDateTime();

        return localDateTime.toLocalTime();
    }

    private LocalTime getLocalTime(String columnLabel) throws SQLException {

        Timestamp timestamp = getTimestamp(columnLabel);

        if(timestamp == null) {
            return null;
        }

        LocalDateTime localDateTime = timestamp.toLocalDateTime();

        return localDateTime.toLocalTime();
    }

    private LocalDate getLocalDate(String columnLabel) throws SQLException {
        Date date = getDate(columnLabel);
        if(date == null) {
            return null;
        }

        return date.toLocalDate();
    }

    private LocalDate getLocalDate(int columnIndex) throws SQLException {
        Date date = getDate(columnIndex);
        if(date == null) {
            return null;
        }

        return date.toLocalDate();
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {

        if (type == null) {
            throw new SQLException("类型不能为空");
        }

        if (type.equals(String.class)) {
            return (T) this.getString(columnLabel);
        }

        if (type.equals(BigDecimal.class)) {
            return (T) this.getBigDecimal(columnLabel);
        }

        if (type.equals(Boolean.class) || type.equals(Boolean.TYPE)) {
            boolean aBoolean = this.getBoolean(columnLabel);
            return (T) Boolean.valueOf(aBoolean);
        }

        if (type.equals(Byte.class) || type.equals(Byte.TYPE)) {
            byte aByte = getByte(columnLabel);
            return (T) Byte.valueOf(aByte);

        }

        if (type.equals(Short.class) || type.equals(Short.TYPE)) {
            short aShort = getShort(columnLabel);
            return (T) Short.valueOf(aShort);
        }

        if (type.equals(Integer.class) || type.equals(Integer.TYPE)) {
            int anInt = getInt(columnLabel);
            return (T) Integer.valueOf(anInt);
        }


        if (type.equals(Long.class) ||  type.equals(Long.TYPE)) {
            long aLong = getLong(columnLabel);
            return (T) Long.valueOf(aLong);
        }

        if (type.equals(Float.class) || type.equals(Float.TYPE)) {
            float aFloat = getFloat(columnLabel);
            return (T) Float.valueOf(aFloat);
        }

        if (type.equals(Double.class) || type.equals(Double.TYPE)) {
            double aDouble = getDouble(columnLabel);
            return (T) Double.valueOf(aDouble);
        }

        if (type.equals(byte[].class)) {
            return (T) this.getBytes(columnLabel);
        }

        if (type.equals(Date.class)) {
            return (T) this.getDate(columnLabel);
        }

        if (type.equals(Time.class)) {
            return (T) this.getTime(columnLabel);
        }

        if (type.equals(Timestamp.class)) {
            return (T) this.getTimestamp(columnLabel);
        }

        if (type.equals(Array.class)) {
            return (T) this.getArray(columnLabel);
        }

        if (type.equals(Ref.class)) {
            return (T) this.getRef(columnLabel);
        }

        if (type.equals(URL.class)) {
            return (T) this.getURL(columnLabel);
        }

        if (type.equals(RowId.class)) {
            return (T) this.getRowId(columnLabel);
        }

        if (type.equals(NClob.class)) {
            return (T) this.getNClob(columnLabel);
        }

        if (type.equals(SQLXML.class)) {
            return (T) this.getSQLXML(columnLabel);
        }

        if (type.equals(LocalDate.class)) {
            return (T) this.getLocalDate(columnLabel);
        }

        if (type.equals(LocalDateTime.class)) {
            return (T) this.getLocalDateTime(columnLabel);
        }

        if (type.equals(LocalTime.class)) {
            return (T) this.getLocalTime(columnLabel);
        }

        return (T) getValue(columnLabel);

    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        log.error(this.getClass().getName() + "#" + "unwrap 方法未实现");
        throw new SQLException("方法未实现");
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        log.error(this.getClass().getName() + "#" + "isWrapperFor 方法未实现");
        throw new SQLException("方法未实现");
    }
}
