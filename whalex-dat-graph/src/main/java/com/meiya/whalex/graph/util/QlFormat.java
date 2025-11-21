package com.meiya.whalex.graph.util;

import java.util.Iterator;
import java.util.Map;

/**
 * @author 黄河森
 * @date 2024/3/8
 * @package com.meiya.whalex.graph.util
 * @project whalex-data-driver
 * @description QlFormat
 */
public abstract class QlFormat {

    private QlFormat() {
        throw new UnsupportedOperationException();
    }

    public static <V> String formatPairs(Map<String, V> entries) {
        Iterator<Map.Entry<String, V>> iterator = entries.entrySet().iterator();
        switch (entries.size()) {
            case 0:
                return "{}";
            case 1:
                return String.format("{%s}", keyValueString((Map.Entry)iterator.next()));
            default:
                StringBuilder builder = new StringBuilder();
                builder.append("{");
                builder.append(keyValueString((Map.Entry)iterator.next()));

                while(iterator.hasNext()) {
                    builder.append(',');
                    builder.append(' ');
                    builder.append(keyValueString((Map.Entry)iterator.next()));
                }

                builder.append("}");
                return builder.toString();
        }
    }

    private static <V> String keyValueString(Map.Entry<String, V> entry) {
        return String.format("%s: %s", entry.getKey(), String.valueOf(entry.getValue()));
    }

    public static String valueOrEmpty(String value) {
        return value != null ? value : "";
    }

}
