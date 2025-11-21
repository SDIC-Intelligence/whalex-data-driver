package com.meiya.whalex.util.collection;

import java.util.HashMap;
import java.util.Map;

/**
 * @author 黄河森
 * @date 2019/9/29
 * @project whale-cloud-platformX
 */
public class MapUtil {

    public static final Map EMPTY_MAP = new HashMap(1);

    /**
     * 倒置 Map
     *
     * @param map
     * @return
     */
    public static Map<String, String> convertMap(Map<String, String> map) {
        if (map == null) {
            return new HashMap<>(1);
        }
        Map<String, String> newMap = new HashMap<>(map.size());
        for (String key : map.keySet()) {
            if (map.get(key) != null) {
                newMap.put(map.get(key), key);
            }
        }
        return newMap;
    }

}
