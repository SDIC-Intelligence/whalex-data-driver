package com.meiya.whalex.db.entity.ani;

import lombok.extern.slf4j.Slf4j;
import oracle.sql.STRUCT;
import oracle.sql.TIMESTAMP;
import org.apache.commons.dbutils.BasicRowProcessor;
import org.apache.commons.lang3.StringUtils;
import java.sql.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/**
 * 自定义行处理器
 *
 * @author 蔡荣桂
 * @date 2021/4/4
 * @project whalex-data-driver
 */
@Slf4j
public class BaseOracleRowProcessor extends BasicRowProcessor {

    @Override
    public Map<String, Object> toMap(ResultSet rs) throws SQLException {
        Map<String, Object> result = new BaseOracleRowProcessor.OracleCaseInsensitiveHashMap();
        ResultSetMetaData rsmd = rs.getMetaData();
        int cols = rsmd.getColumnCount();
        for(int i = 1; i <= cols; ++i) {
            String columnName = rsmd.getColumnLabel(i);
            //过滤分页字段
            if("ORACLE_PAGE_FIELD".equals(columnName)){
                continue;
            }
            if (null == columnName || 0 == columnName.length()) {
                columnName = rsmd.getColumnName(i);
            }
            Object value = null;
            try {
                value = rs.getObject(i);
                if (value != null) {
                    if (value instanceof oracle.sql.TIMESTAMP) {
                        oracle.sql.TIMESTAMP timestamp = (TIMESTAMP) value;
                        value = timestamp.timestampValue();
                    } /*else if (value instanceof BigInteger) {
                        BigInteger bigInteger = (BigInteger) value;
                        value = bigInteger.intValue();
                    } else if (value instanceof BigDecimal) {
                        BigDecimal bigDecimal = (BigDecimal) value;
                        value = bigDecimal.doubleValue();
                    }*/
                    else if (value instanceof Struct) {
                        Struct struct = (Struct) value;
                        String sqlTypeName = struct.getSQLTypeName();
                        if (StringUtils.startsWithIgnoreCase(sqlTypeName, "MDSYS.SDO_")) {
                            Object[] attributes = struct.getAttributes();
                            STRUCT pointStruct = (STRUCT) attributes[2];
                            Object[] pointStructAttributes = pointStruct.getAttributes();
                            value = pointStructAttributes[0] + "," + pointStructAttributes[1];
                        }

                    }
                }
            } catch (Exception e) {
                log.error("analysis column value error, columnName: [{}] index: [{}]", columnName, rs.getRow(), e);
            }
            result.put(columnName, value);
        }
        return result;
    }

    /**
     * 内部 Map 对象
     */
    private static class OracleCaseInsensitiveHashMap extends LinkedHashMap<String, Object> {
        private final Map<String, String> lowerCaseMap;
        private static final long serialVersionUID = -2848100435296897382L;

        private OracleCaseInsensitiveHashMap() {
            this.lowerCaseMap = new HashMap();
        }
        @Override
        public boolean containsKey(Object key) {
            Object realKey = this.lowerCaseMap.get(key.toString().toLowerCase(Locale.ENGLISH));
            return super.containsKey(realKey);
        }
        @Override
        public Object get(Object key) {
            Object realKey = this.lowerCaseMap.get(key.toString().toLowerCase(Locale.ENGLISH));
            return super.get(realKey);
        }
        @Override
        public Object put(String key, Object value) {
            Object oldKey = this.lowerCaseMap.put(key.toLowerCase(Locale.ENGLISH), key);
            Object oldValue = super.remove(oldKey);
            super.put(key, value);
            return oldValue;
        }
        @Override
        public void putAll(Map<? extends String, ?> m) {
            Iterator var2 = m.entrySet().iterator();

            while(var2.hasNext()) {
                Map.Entry<? extends String, ?> entry = (Map.Entry)var2.next();
                String key = entry.getKey();
                Object value = entry.getValue();
                this.put(key, value);
            }

        }
        @Override
        public Object remove(Object key) {
            Object realKey = this.lowerCaseMap.remove(key.toString().toLowerCase(Locale.ENGLISH));
            return super.remove(realKey);
        }
    }

}
