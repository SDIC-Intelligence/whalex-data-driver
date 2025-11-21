package com.meiya.whalex.db.util.param.impl.ani;

import cn.hutool.core.collection.CollectionUtil;
import com.kingbase8.jdbc.KbArray;
import com.kingbase8.util.KBobject;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.dbutils.BasicRowProcessor;

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
public class KingBaseEsRowProcessor extends BasicRowProcessor {

    @Override
    public Map<String, Object> toMap(ResultSet rs) throws SQLException {
        Map<String, Object> result = new MyCaseInsensitiveHashMap();
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
                // 处理pg库不存在的字段类型, 比如nvarchar2(libra存在)
                // 只取里面的值
                if (value instanceof KBobject) {
                    if (log.isDebugEnabled()) {
                        log.debug("object is kbObject [type: {}], only read value", ((KBobject) value).getType());
                    }
                    value = ((KBobject) value).getValue();
                } else if (value instanceof KbArray) {
                    // 处理 PG varchar[] 类型
                    Object[] array = (Object[]) ((KbArray) value).getArray();
                    value = CollectionUtil.newArrayList(array);
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
