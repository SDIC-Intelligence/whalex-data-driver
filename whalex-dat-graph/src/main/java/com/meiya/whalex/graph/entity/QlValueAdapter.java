package com.meiya.whalex.graph.entity;

import com.meiya.whalex.graph.enums.QlTypeConstructor;
import com.meiya.whalex.graph.exception.QlNotMultiValued;
import com.meiya.whalex.graph.exception.QlUncoercible;
import com.meiya.whalex.graph.exception.QlUnsizable;

import java.time.*;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * @author 黄河森
 * @date 2024/3/8
 * @package com.meiya.whalex.graph.entity
 * @project whalex-data-driver
 * @description QlValueAdapter
 */
public abstract class QlValueAdapter extends InternalQlMapAccessorWithDefaultValue implements InternalQlValue {

    public QlValueAdapter() {
    }

    public QlValue asValue() {
        return this;
    }

    public boolean hasType(QlType type) {
        return type.isTypeOf(this);
    }

    public boolean isTrue() {
        return false;
    }

    public boolean isFalse() {
        return false;
    }

    public boolean isNull() {
        return false;
    }

    public boolean containsKey(String key) {
        throw new QlNotMultiValued(this.type().name() + " is not a keyed collection");
    }

    public String asString() {
        throw new QlUncoercible(this.type().name(), "Java String");
    }

    public boolean asBoolean(boolean defaultValue) {
        return (Boolean)this.computeOrDefault(QlValue::asBoolean, defaultValue);
    }

    public String asString(String defaultValue) {
        return (String)this.computeOrDefault(QlValue::asString, defaultValue);
    }

    public long asLong(long defaultValue) {
        return (Long)this.computeOrDefault(QlValue::asLong, defaultValue);
    }

    public int asInt(int defaultValue) {
        return (Integer)this.computeOrDefault(QlValue::asInt, defaultValue);
    }

    public double asDouble(double defaultValue) {
        return (Double)this.computeOrDefault(QlValue::asDouble, defaultValue);
    }

    public float asFloat(float defaultValue) {
        return (Float)this.computeOrDefault(QlValue::asFloat, defaultValue);
    }

    public long asLong() {
        throw new QlUncoercible(this.type().name(), "Java long");
    }

    public int asInt() {
        throw new QlUncoercible(this.type().name(), "Java int");
    }

    public float asFloat() {
        throw new QlUncoercible(this.type().name(), "Java float");
    }

    public double asDouble() {
        throw new QlUncoercible(this.type().name(), "Java double");
    }

    public boolean asBoolean() {
        throw new QlUncoercible(this.type().name(), "Java boolean");
    }

    public List<Object> asList() {
        return this.asList(QlValues.ofObject());
    }

    public <T> List<T> asList(Function<QlValue, T> mapFunction) {
        throw new QlUncoercible(this.type().name(), "Java List");
    }

    public Map<String, Object> asMap() {
        return this.asMap(QlValues.ofObject());
    }

    public <T> Map<String, T> asMap(Function<QlValue, T> mapFunction) {
        throw new QlUncoercible(this.type().name(), "Java Map");
    }

    public Object asObject() {
        throw new QlUncoercible(this.type().name(), "Java Object");
    }

    public <T> T computeOrDefault(Function<QlValue, T> mapper, T defaultValue) {
        return this.isNull() ? defaultValue : mapper.apply(this);
    }

    public Map<String, Object> asMap(Map<String, Object> defaultValue) {
        return this.computeOrDefault(QlMapAccessor::asMap, defaultValue);
    }

    public <T> Map<String, T> asMap(Function<QlValue, T> mapFunction, Map<String, T> defaultValue) {
        return this.computeOrDefault((value) -> {
            return value.asMap(mapFunction);
        }, defaultValue);
    }

    public byte[] asByteArray(byte[] defaultValue) {
        return (byte[])this.computeOrDefault(QlValue::asByteArray, defaultValue);
    }

    public List<Object> asList(List<Object> defaultValue) {
        return (List)this.computeOrDefault(QlValue::asList, defaultValue);
    }

    public <T> List<T> asList(Function<QlValue, T> mapFunction, List<T> defaultValue) {
        return (List)this.computeOrDefault((value) -> {
            return value.asList(mapFunction);
        }, defaultValue);
    }

    public LocalDate asLocalDate(LocalDate defaultValue) {
        return (LocalDate)this.computeOrDefault(QlValue::asLocalDate, defaultValue);
    }

    public OffsetTime asOffsetTime(OffsetTime defaultValue) {
        return (OffsetTime)this.computeOrDefault(QlValue::asOffsetTime, defaultValue);
    }

    public LocalTime asLocalTime(LocalTime defaultValue) {
        return (LocalTime)this.computeOrDefault(QlValue::asLocalTime, defaultValue);
    }

    public LocalDateTime asLocalDateTime(LocalDateTime defaultValue) {
        return (LocalDateTime)this.computeOrDefault(QlValue::asLocalDateTime, defaultValue);
    }

    public OffsetDateTime asOffsetDateTime(OffsetDateTime defaultValue) {
        return (OffsetDateTime)this.computeOrDefault(QlValue::asOffsetDateTime, defaultValue);
    }

    public ZonedDateTime asZonedDateTime(ZonedDateTime defaultValue) {
        return (ZonedDateTime)this.computeOrDefault(QlValue::asZonedDateTime, defaultValue);
    }

    public QlIsoDuration asIsoDuration(QlIsoDuration defaultValue) {
        return (QlIsoDuration)this.computeOrDefault(QlValue::asIsoDuration, defaultValue);
    }

    public QlPoint asPoint(QlPoint defaultValue) {
        return (QlPoint)this.computeOrDefault(QlValue::asPoint, defaultValue);
    }

    @Override
    public List<QlPoint> asLineString(List<QlPoint> defaultValue) {
        return (List<QlPoint>)this.computeOrDefault(QlValue::asLineString, defaultValue);
    }

    @Override
    public List<List<QlPoint>> asPolygon(List<List<QlPoint>> defaultValue) {
        return (List<List<QlPoint>>)this.computeOrDefault(QlValue::asPolygon, defaultValue);
    }

    public byte[] asByteArray() {
        throw new QlUncoercible(this.type().name(), "Byte array");
    }

    public Number asNumber() {
        throw new QlUncoercible(this.type().name(), "Java Number");
    }

    public QlEntity asEntity() {
        throw new QlUncoercible(this.type().name(), "Entity");
    }

    public QlNode asNode() {
        throw new QlUncoercible(this.type().name(), "Node");
    }

    public QlPath asPath() {
        throw new QlUncoercible(this.type().name(), "Path");
    }

    public QlRelationship asRelationship() {
        throw new QlUncoercible(this.type().name(), "Relationship");
    }

    public LocalDate asLocalDate() {
        throw new QlUncoercible(this.type().name(), "LocalDate");
    }

    public OffsetTime asOffsetTime() {
        throw new QlUncoercible(this.type().name(), "OffsetTime");
    }

    public LocalTime asLocalTime() {
        throw new QlUncoercible(this.type().name(), "LocalTime");
    }

    public LocalDateTime asLocalDateTime() {
        throw new QlUncoercible(this.type().name(), "LocalDateTime");
    }

    public OffsetDateTime asOffsetDateTime() {
        throw new QlUncoercible(this.type().name(), "OffsetDateTime");
    }

    public ZonedDateTime asZonedDateTime() {
        throw new QlUncoercible(this.type().name(), "ZonedDateTime");
    }

    public QlIsoDuration asIsoDuration() {
        throw new QlUncoercible(this.type().name(), "Duration");
    }

    public QlPoint asPoint() {
        throw new QlUncoercible(this.type().name(), "QlPoint");
    }

    @Override
    public List<? extends QlPoint> asLineString() {
        throw new QlUncoercible(this.type().name(), "QlPoint");
    }

    @Override
    public List<List<? extends QlPoint>> asPolygon() {
        throw new QlUncoercible(this.type().name(), "QlPoint");
    }

    public QlValue get(int index) {
        throw new QlNotMultiValued(this.type().name() + " is not an indexed collection");
    }

    public QlValue get(String key) {
        throw new QlNotMultiValued(this.type().name() + " is not a keyed collection");
    }

    public int size() {
        throw new QlUnsizable(this.type().name() + " does not have size");
    }

    public Iterable<String> keys() {
        return Collections.emptyList();
    }

    public boolean isEmpty() {
        return !this.values().iterator().hasNext();
    }

    public Iterable<QlValue> values() {
        return this.values(QlValues.ofValue());
    }

    public <T> Iterable<T> values(Function<QlValue, T> mapFunction) {
        throw new QlNotMultiValued(this.type().name() + " is not iterable");
    }

    public final QlTypeConstructor typeConstructor() {
        return ((QlTypeRepresentation)this.type()).constructor();
    }

    public abstract boolean equals(Object var1);

    public abstract int hashCode();

    public abstract String toString();
    
}
