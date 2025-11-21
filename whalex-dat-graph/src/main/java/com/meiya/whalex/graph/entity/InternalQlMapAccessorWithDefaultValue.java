package com.meiya.whalex.graph.entity;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * @author 黄河森
 * @date 2024/3/8
 * @package com.meiya.whalex.graph.entity
 * @project whalex-data-driver
 * @description InternalQlMapAccessorWithDefaultValue
 */
public abstract class InternalQlMapAccessorWithDefaultValue implements QlMapAccessorWithDefaultValue {

    public InternalQlMapAccessorWithDefaultValue() {
    }

    public abstract QlValue get(String var1);

    public QlValue get(String key, QlValue defaultValue) {
        return this.get(this.get(key), defaultValue);
    }

    private QlValue get(QlValue value, QlValue defaultValue) {
        return value.equals(QlValues.NULL) ? defaultValue : ((QlAsValue)value).asValue();
    }

    public Object get(String key, Object defaultValue) {
        return this.get(this.get(key), defaultValue);
    }

    private Object get(QlValue value, Object defaultValue) {
        return value.equals(QlValues.NULL) ? defaultValue : value.asObject();
    }

    public Number get(String key, Number defaultValue) {
        return this.get(this.get(key), defaultValue);
    }

    private Number get(QlValue value, Number defaultValue) {
        return value.equals(QlValues.NULL) ? defaultValue : value.asNumber();
    }

    public QlEntity get(String key, QlEntity defaultValue) {
        return this.get(this.get(key), defaultValue);
    }

    private QlEntity get(QlValue value, QlEntity defaultValue) {
        return value.equals(QlValues.NULL) ? defaultValue : value.asEntity();
    }

    public QlNode get(String key, QlNode defaultValue) {
        return this.get(this.get(key), defaultValue);
    }

    private QlNode get(QlValue value, QlNode defaultValue) {
        return value.equals(QlValues.NULL) ? defaultValue : value.asNode();
    }

    public QlPath get(String key, QlPath defaultValue) {
        return this.get(this.get(key), defaultValue);
    }

    private QlPath get(QlValue value, QlPath defaultValue) {
        return value.equals(QlValues.NULL) ? defaultValue : value.asPath();
    }

    public QlRelationship get(String key, QlRelationship defaultValue) {
        return this.get(this.get(key), defaultValue);
    }

    private QlRelationship get(QlValue value, QlRelationship defaultValue) {
        return value.equals(QlValues.NULL) ? defaultValue : value.asRelationship();
    }

    public List<Object> get(String key, List<Object> defaultValue) {
        return this.get(this.get(key), defaultValue);
    }

    private List<Object> get(QlValue value, List<Object> defaultValue) {
        return value.equals(QlValues.NULL) ? defaultValue : value.asList();
    }

    public <T> List<T> get(String key, List<T> defaultValue, Function<QlValue, T> mapFunc) {
        return this.get(this.get(key), defaultValue, mapFunc);
    }

    private <T> List<T> get(QlValue value, List<T> defaultValue, Function<QlValue, T> mapFunc) {
        return value.equals(QlValues.NULL) ? defaultValue : value.asList(mapFunc);
    }

    public Map<String, Object> get(String key, Map<String, Object> defaultValue) {
        return this.get(this.get(key), defaultValue);
    }

    private Map<String, Object> get(QlValue value, Map<String, Object> defaultValue) {
        return value.equals(QlValues.NULL) ? defaultValue : value.asMap();
    }

    public <T> Map<String, T> get(String key, Map<String, T> defaultValue, Function<QlValue, T> mapFunc) {
        return this.get(this.get(key), defaultValue, mapFunc);
    }

    private <T> Map<String, T> get(QlValue value, Map<String, T> defaultValue, Function<QlValue, T> mapFunc) {
        return value.equals(QlValues.NULL) ? defaultValue : value.asMap(mapFunc);
    }

    public int get(String key, int defaultValue) {
        return this.get(this.get(key), defaultValue);
    }

    private int get(QlValue value, int defaultValue) {
        return value.equals(QlValues.NULL) ? defaultValue : value.asInt();
    }

    public long get(String key, long defaultValue) {
        return this.get(this.get(key), defaultValue);
    }

    private long get(QlValue value, long defaultValue) {
        return value.equals(QlValues.NULL) ? defaultValue : value.asLong();
    }

    public boolean get(String key, boolean defaultValue) {
        return this.get(this.get(key), defaultValue);
    }

    private boolean get(QlValue value, boolean defaultValue) {
        return value.equals(QlValues.NULL) ? defaultValue : value.asBoolean();
    }

    public String get(String key, String defaultValue) {
        return this.get(this.get(key), defaultValue);
    }

    private String get(QlValue value, String defaultValue) {
        return value.equals(QlValues.NULL) ? defaultValue : value.asString();
    }

    public float get(String key, float defaultValue) {
        return this.get(this.get(key), defaultValue);
    }

    private float get(QlValue value, float defaultValue) {
        return value.equals(QlValues.NULL) ? defaultValue : value.asFloat();
    }

    public double get(String key, double defaultValue) {
        return this.get(this.get(key), defaultValue);
    }

    private double get(QlValue value, double defaultValue) {
        return value.equals(QlValues.NULL) ? defaultValue : value.asDouble();
    }
}
