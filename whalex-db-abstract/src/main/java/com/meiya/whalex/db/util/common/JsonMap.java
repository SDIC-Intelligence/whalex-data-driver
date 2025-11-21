package com.meiya.whalex.db.util.common;

import java.util.HashMap;

/**
 * map树 用来构建多层map 数据,最后只要调整工具就可以直接转成json
 * add by huangzr
 */
public class JsonMap extends HashMap<String,Object> {

    //增加子map
    public JsonMap addSubMap(String key){
        put(key,new JsonMap());
        return (JsonMap) get(key);
    }

    public JsonMap getSubMap(String key){
       return (JsonMap) get(key);
    }
    //增加属性
    public JsonMap addObject(String key, Object object){
        put(key,object);
        return this;
    }

}
