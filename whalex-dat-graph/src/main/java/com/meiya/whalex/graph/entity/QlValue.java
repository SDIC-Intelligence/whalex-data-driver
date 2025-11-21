package com.meiya.whalex.graph.entity;

import java.time.*;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * @author 黄河森
 * @date 2024/3/7
 * @package com.meiya.whalex.graph.entity
 * @project whalex-data-driver
 * @description QlValue
 */
public interface QlValue extends QlMapAccessor, QlMapAccessorWithDefaultValue {

    int size();

    boolean isEmpty();

    Iterable<String> keys();

    QlValue get(int var1);

    QlType type();

    boolean hasType(QlType var1);

    boolean isTrue();

    boolean isFalse();

    boolean isNull();

    Object asObject();

    <T> T computeOrDefault(Function<QlValue, T> var1, T var2);

    boolean asBoolean();

    boolean asBoolean(boolean var1);

    byte[] asByteArray();

    byte[] asByteArray(byte[] var1);

    String asString();

    String asString(String var1);

    Number asNumber();

    long asLong();

    long asLong(long var1);

    int asInt();

    int asInt(int var1);

    double asDouble();

    double asDouble(double var1);

    float asFloat();

    float asFloat(float var1);

    List<Object> asList();

    List<Object> asList(List<Object> var1);

    <T> List<T> asList(Function<QlValue, T> var1);

    <T> List<T> asList(Function<QlValue, T> var1, List<T> var2);

    QlEntity asEntity();

    QlNode asNode();

    QlRelationship asRelationship();

    QlPath asPath();

    LocalDate asLocalDate();

    OffsetTime asOffsetTime();

    LocalTime asLocalTime();

    LocalDateTime asLocalDateTime();

    OffsetDateTime asOffsetDateTime();

    ZonedDateTime asZonedDateTime();

    QlIsoDuration asIsoDuration();

    QlPoint asPoint();

    List<? extends QlPoint> asLineString();

    List<List<? extends QlPoint>> asPolygon();

    LocalDate asLocalDate(LocalDate var1);

    OffsetTime asOffsetTime(OffsetTime var1);

    LocalTime asLocalTime(LocalTime var1);

    LocalDateTime asLocalDateTime(LocalDateTime var1);

    OffsetDateTime asOffsetDateTime(OffsetDateTime var1);

    ZonedDateTime asZonedDateTime(ZonedDateTime var1);

    QlIsoDuration asIsoDuration(QlIsoDuration var1);

    QlPoint asPoint(QlPoint var1);

    List<QlPoint> asLineString(List<QlPoint> qlPoints);

    List<List<QlPoint>> asPolygon(List<List<QlPoint>> polygons);

    Map<String, Object> asMap(Map<String, Object> var1);

    <T> Map<String, T> asMap(Function<QlValue, T> var1, Map<String, T> var2);

    boolean equals(Object var1);

    int hashCode();

    String toString();

}
