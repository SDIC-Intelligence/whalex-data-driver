package com.meiya.whalex.db.entity.ani;

import com.meiya.whalex.util.JsonUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.dbutils.BasicRowProcessor;
import org.apache.commons.lang3.StringUtils;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.geom.impl.CoordinateArraySequenceFactory;
import org.locationtech.jts.io.*;

import java.io.ByteArrayInputStream;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;

/**
 * 自定义行处理器
 *
 * @author 黄河森
 * @date 2021/3/4
 * @project whalex-data-driver
 */
@Slf4j
public class BaseMyRowProcessor extends BasicRowProcessor {

    private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory(new PrecisionModel(), 0, CoordinateArraySequenceFactory.instance());

    /**
     * bit(1) 查询数据时是否转为 boolean 值，默认true
     * false 为 bit(1)输出为int类型
     */
    private Boolean bit1isBoolean = true;

    /**
     * 是否将 json 字段转为 map 或 list，默认为 false：输出字符串
     */
    private Boolean jsonToObject = false;

    public BaseMyRowProcessor() {
        super();
    }

    public BaseMyRowProcessor(boolean bit1isBoolean, boolean jsonToObject) {
        super();
        this.bit1isBoolean = bit1isBoolean;
        this.jsonToObject = jsonToObject;
    }

    @Override
    public Map<String, Object> toMap(ResultSet rs) throws SQLException {
        Map<String, Object> result = new BaseMyRowProcessor.MyCaseInsensitiveHashMap();
        ResultSetMetaData rsmd = rs.getMetaData();
        int cols = rsmd.getColumnCount();
        for(int i = 1; i <= cols; ++i) {
            String columnName = rsmd.getColumnLabel(i);
            if (null == columnName || 0 == columnName.length()) {
                columnName = rsmd.getColumnName(i);
            }
            // 获取类型
            String columnTypeName = rsmd.getColumnTypeName(i);
            Object value = null;
            try {
                if (!bit1isBoolean && rsmd.getPrecision(i) == 1) {
                    value = rs.getInt(i);
                } else if (StringUtils.equalsIgnoreCase(columnTypeName, "JSON") && jsonToObject) {
                    String jsonStr = rs.getString(i);
                    if (StringUtils.isNotBlank(jsonStr)) {
                        if (StringUtils.startsWithIgnoreCase(jsonStr, "[") && StringUtils.endsWithIgnoreCase(jsonStr, "]")) {
                            // 数组 JSON
                            value = JsonUtil.jsonStrToObject(jsonStr, List.class);
                        } else {
                            // MAP
                            value = JsonUtil.jsonStrToMap(jsonStr);
                        }
                    }
                } else {
                    value = rs.getObject(i);
                    // 如果是字节数组，可能是 point 类型，进行进一步判断
                    if (StringUtils.equalsIgnoreCase(columnTypeName, "GEOMETRY") && value instanceof byte[]) {
                        value = pointBytesToPoint((byte[]) value);
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
     * point 坐标
     *
     * @param bytes
     * @return
     */
    private String pointBytesToPoint(byte[] bytes) throws IllegalAccessException {
        if (bytes == null) {
            return null;
        }

        if (bytes.length < 21) {
            throw new IllegalAccessException("Not enough bytes to represent a Point object");
        }

        try (ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes)){
            byte[] srIdBytes = new byte[4];
            inputStream.read(srIdBytes);
            int srId = ByteOrderValues.getInt(srIdBytes, ByteOrderValues.LITTLE_ENDIAN);

            GeometryFactory geometryFactory = GEOMETRY_FACTORY;

            if (srId != 0) {
                geometryFactory = new GeometryFactory(new PrecisionModel(), srId, CoordinateArraySequenceFactory.instance());
            }

            WKBReader wkbReader = new WKBReader(geometryFactory);

            Geometry geometry = wkbReader.read(new InputStreamInStream(inputStream));

            Point point = (Point) geometry;

            return point.getX() + "," + point.getY();

        } catch (Exception e) {
            log.error("failed when mysql reading points from database.", e);
            return null;
        }

    }

    /**
     * 内部 Map 对象
     */
    private static class MyCaseInsensitiveHashMap extends LinkedHashMap<String, Object> {
        private final Map<String, String> lowerCaseMap;
        private static final long serialVersionUID = -2848100435296897382L;

        private MyCaseInsensitiveHashMap() {
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
