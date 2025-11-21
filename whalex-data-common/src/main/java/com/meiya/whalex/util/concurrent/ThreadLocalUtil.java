package com.meiya.whalex.util.concurrent;

import org.apache.commons.collections.MapUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * 本地线程变量存放
 *
 * 不能存入大对象！！！
 *
 * @author Huanghesen
 * @date 2019/2/22
 */
public class ThreadLocalUtil {

    private static final ThreadLocal<Map<String, Object>> THREAD_LOCAL = new ThreadLocal<>();

    /**
     * 获取本地线程绑定的对象值
     *
     * @param key
     * @return
     */
    public static Object get(String key) {
        Map<String, Object> map = THREAD_LOCAL.get();
        if (MapUtils.isEmpty(map)) {
            return null;
        } else {
            return THREAD_LOCAL.get().get(key);
        }
    }

    /**
     * 绑定参数到本地线程
     *
     * @param key
     * @param val
     */
    public static void set(String key, Object val) {
        Map<String, Object> map = THREAD_LOCAL.get();
        if (MapUtils.isEmpty(map)) {
            map = new HashMap<>(1);
            THREAD_LOCAL.set(map);
        }
        THREAD_LOCAL.get().put(key, val);
    }

    /**
     * 获取结果转换为字符串
     *
     * @param key
     * @return
     */
    public static String getString(String key) {
        Object val = get(key);
        return val == null ? null : String.valueOf(val);
    }

    /**
     * 清空数据
     */
    public static void remove() {
        THREAD_LOCAL.remove();
    }

    public static void remove(String key) {
        Map<String, Object> map = THREAD_LOCAL.get();
        if (!MapUtils.isEmpty(map)) {
            THREAD_LOCAL.get().remove(key);
        }
    }

}
