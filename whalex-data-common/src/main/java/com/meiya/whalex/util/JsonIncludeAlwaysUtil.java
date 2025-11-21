package com.meiya.whalex.util;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.Map;

/**
 * JSON工具类
 * @author myuser
 */
public final class JsonIncludeAlwaysUtil {
    private JsonIncludeAlwaysUtil() {
    }

    private final static ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .setSerializationInclusion(JsonInclude.Include.ALWAYS)
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    /**
     * json字符串转map
     *
     * @param jsonStr
     * @return
     */
    public static Map<String, Object> jsonStrToMap(String jsonStr) {
        try {
            return OBJECT_MAPPER.readValue(jsonStr, new TypeReference<Map<String, Object>>() {
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * java bean转map
     *
     * @param obj
     * @return
     */
    public static Map<String, Object> entityToMap(Object obj) {
        try {
            return OBJECT_MAPPER.readValue(objectToStr(obj), new TypeReference<Map<String, Object>>() {
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * json字符串转java bean
     *
     * @param jsonStr
     * @param valueType
     * @param <T>
     * @return
     */
    public static <T> T jsonStrToObject(String jsonStr, Class<T> valueType) {
        try {
            return OBJECT_MAPPER.readValue(jsonStr, valueType);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 对象转json字符串
     *
     * @param obj
     * @return
     */
    public static String objectToStr(Object obj) {
        try {
            return OBJECT_MAPPER.writeValueAsString(obj);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
