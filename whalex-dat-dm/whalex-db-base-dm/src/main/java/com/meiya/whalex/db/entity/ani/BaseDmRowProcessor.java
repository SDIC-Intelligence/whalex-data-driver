package com.meiya.whalex.db.entity.ani;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.dbutils.BasicRowProcessor;
import org.apache.commons.lang3.StringUtils;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.io.WKBReader;

import java.sql.*;
import java.util.*;

/**
 * 自定义行处理器
 *
 * @author 蔡荣桂
 * @date 2021/4/4
 * @project whalex-data-driver
 */
@Slf4j
public class BaseDmRowProcessor extends BasicRowProcessor {

    @Override
    public Map<String, Object> toMap(ResultSet rs) throws SQLException {
        Map<String, Object> result = new DmCaseInsensitiveHashMap();
        ResultSetMetaData rsmd = rs.getMetaData();
        int cols = rsmd.getColumnCount();
        for(int i = 1; i <= cols; ++i) {
            String columnName = rsmd.getColumnLabel(i);
            if (null == columnName || 0 == columnName.length()) {
                columnName = rsmd.getColumnName(i);
            }
            Object value = null;
            try {
                value = rs.getObject(i);
                if (value != null) {
                    if (value instanceof Clob) {
                        Clob clob = (Clob) value;
                        value = clob.getSubString(1, (int) clob.length());
                    } else if (value instanceof Struct) {
                        Struct struct = (Struct) value;
                        String sqlTypeName = struct.getSQLTypeName();
                        if (StringUtils.startsWithIgnoreCase(sqlTypeName, "SYSGEO")) {
                            Object[] attributes = struct.getAttributes();
                            int srId = (Integer) attributes[0];
                            Blob blob = (Blob) attributes[1];
                            int type = (Integer) attributes[2];
                            WKBReader wkbReader = new WKBReader();
                            Point geometry = (Point) wkbReader.read(blob.getBytes(1, (int) blob.length()));
                            value = geometry.getX() + "," + geometry.getY();
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
    private static class DmCaseInsensitiveHashMap extends LinkedHashMap<String, Object> {
        private final Map<String, String> lowerCaseMap;
        private static final long serialVersionUID = -2848100435296897382L;

        private DmCaseInsensitiveHashMap() {
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
