package com.meiya.whalex.cache.entity;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class CacheEntity {

    private String key;

    private String[] keys;

    private String field;

    private String[] fields;

    private Object value;

    private String[] values;

    private Long expire;

    private String[] keysValues;

    private Map<String, String> fieldsValues;

    private int start;

    private int stop;

    private String script;
}
