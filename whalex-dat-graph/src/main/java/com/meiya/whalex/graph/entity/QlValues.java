package com.meiya.whalex.graph.entity;

import com.meiya.whalex.graph.exception.QlClientException;
import com.meiya.whalex.graph.util.iterator.QlIterables;

import java.time.*;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * @author 黄河森
 * @date 2024/3/8
 * @package com.meiya.whalex.graph.entity
 * @project whalex-data-driver
 * @description Values
 */
public class QlValues {

    public static final QlValue EmptyMap = value(Collections.emptyMap());
    public static final QlValue NULL;

    private QlValues() {
        throw new UnsupportedOperationException();
    }

    public static QlValue value(Object value) {
        if (value == null) {
            return QlNullValue.NULL;
        } else if (value instanceof QlAsValue) {
            return ((QlAsValue)value).asValue();
        } else if (value instanceof Boolean) {
            return value((Boolean)value);
        } else if (value instanceof String) {
            return value((String)value);
        } else if (value instanceof Character) {
            return value((Character)value);
        } else if (value instanceof Long) {
            return value((Long)value);
        } else if (value instanceof Short) {
            return value((int)(Short)value);
        } else if (value instanceof Byte) {
            return value((int)(Byte)value);
        } else if (value instanceof Integer) {
            return value((Integer)value);
        } else if (value instanceof Double) {
            return value((Double)value);
        } else if (value instanceof Float) {
            return value((double)(Float)value);
        } else if (value instanceof LocalDate) {
            return value((LocalDate)value);
        } else if (value instanceof OffsetTime) {
            return value((OffsetTime)value);
        } else if (value instanceof LocalTime) {
            return value((LocalTime)value);
        } else if (value instanceof LocalDateTime) {
            return value((LocalDateTime)value);
        } else if (value instanceof OffsetDateTime) {
            return value((OffsetDateTime)value);
        } else if (value instanceof ZonedDateTime) {
            return value((ZonedDateTime)value);
        } else if (value instanceof QlIsoDuration) {
            return value((QlIsoDuration)value);
        } else if (value instanceof Period) {
            return value((Period)value);
        } else if (value instanceof Duration) {
            return value((Duration)value);
        } else if (value instanceof QlPoint) {
            return value((QlPoint)value);
        } else if (value instanceof List) {
            return value((List)value);
        } else if (value instanceof Map) {
            return value((Map)value);
        } else if (value instanceof Iterable) {
            return value((Iterable)value);
        } else if (value instanceof Iterator) {
            return value((Iterator)value);
        } else if (value instanceof Stream) {
            return value((Stream)value);
        } else if (value instanceof char[]) {
            return value((char[])((char[])value));
        } else if (value instanceof byte[]) {
            return value((byte[])((byte[])value));
        } else if (value instanceof boolean[]) {
            return value((boolean[])((boolean[])value));
        } else if (value instanceof String[]) {
            return value((String[])((String[])value));
        } else if (value instanceof long[]) {
            return value((long[])((long[])value));
        } else if (value instanceof int[]) {
            return value((int[])((int[])value));
        } else if (value instanceof short[]) {
            return value((short[])((short[])value));
        } else if (value instanceof double[]) {
            return value((double[])((double[])value));
        } else if (value instanceof float[]) {
            return value((float[])((float[])value));
        } else if (value instanceof QlValue[]) {
            return value((QlValue[])((QlValue[])value));
        } else if (value instanceof Object[]) {
            return value(Arrays.asList((Object[])((Object[])value)));
        } else {
            throw new QlClientException("Unable to convert " + value.getClass().getName() + " to Neo4j QlValue.");
        }
    }

    public static QlValue[] values(Object... input) {
        QlValue[] values = new QlValue[input.length];

        for(int i = 0; i < input.length; ++i) {
            values[i] = value(input[i]);
        }

        return values;
    }

    public static QlValue value(QlValue... input) {
        int size = input.length;
        QlValue[] values = new QlValue[size];
        System.arraycopy(input, 0, values, 0, size);
        return new QlListValue(values);
    }

    public static QlBytesValue value(byte... input) {
        return new QlBytesValue(input);
    }

    public static QlValue value(String... input) {
        QlStringValue[] values = new QlStringValue[input.length];

        for(int i = 0; i < input.length; ++i) {
            values[i] = new QlStringValue(input[i]);
        }

        return new QlListValue(values);
    }

    public static QlValue value(boolean... input) {
        QlValue[] values = new QlValue[input.length];

        for(int i = 0; i < input.length; ++i) {
            values[i] = value(input[i]);
        }

        return new QlListValue(values);
    }

    public static QlValue value(char... input) {
        QlValue[] values = new QlValue[input.length];

        for(int i = 0; i < input.length; ++i) {
            values[i] = value(input[i]);
        }

        return new QlListValue(values);
    }

    public static QlValue value(long... input) {
        QlValue[] values = new QlValue[input.length];

        for(int i = 0; i < input.length; ++i) {
            values[i] = value(input[i]);
        }

        return new QlListValue(values);
    }

    public static QlValue value(short... input) {
        QlValue[] values = new QlValue[input.length];

        for(int i = 0; i < input.length; ++i) {
            values[i] = value((int)input[i]);
        }

        return new QlListValue(values);
    }

    public static QlValue value(int... input) {
        QlValue[] values = new QlValue[input.length];

        for(int i = 0; i < input.length; ++i) {
            values[i] = value(input[i]);
        }

        return new QlListValue(values);
    }

    public static QlValue value(double... input) {
        QlValue[] values = new QlValue[input.length];

        for(int i = 0; i < input.length; ++i) {
            values[i] = value(input[i]);
        }

        return new QlListValue(values);
    }

    public static QlValue value(float... input) {
        QlValue[] values = new QlValue[input.length];

        for(int i = 0; i < input.length; ++i) {
            values[i] = value((double)input[i]);
        }

        return new QlListValue(values);
    }

    public static QlValue value(List<Object> vals) {
        QlValue[] values = new QlValue[vals.size()];
        int i = 0;

        Object val;
        for(Iterator var3 = vals.iterator(); var3.hasNext(); values[i++] = value(val)) {
            val = var3.next();
        }

        return new QlListValue(values);
    }

    public static QlValue value(Iterable<Object> val) {
        return value(val.iterator());
    }

    public static QlValue value(Iterator<Object> val) {
        List<QlValue> values = new ArrayList();

        while(val.hasNext()) {
            values.add(value(val.next()));
        }

        return new QlListValue((QlValue[])values.toArray(new QlValue[0]));
    }

    public static QlValue value(Stream<Object> stream) {
        QlValue[] values = (QlValue[])stream.map(QlValues::value).toArray((x$0) -> {
            return new QlValue[x$0];
        });
        return new QlListValue(values);
    }

    public static QlValue value(char val) {
        return new QlStringValue(String.valueOf(val));
    }

    public static QlValue value(String val) {
        return new QlStringValue(val);
    }

    public static QlValue value(long val) {
        return new QlIntegerValue(val);
    }

    public static QlValue value(int val) {
        return new QlIntegerValue((long)val);
    }

    public static QlValue value(double val) {
        return new QlFloatValue(val);
    }

    public static QlValue value(boolean val) {
        return QlBooleanValue.fromBoolean(val);
    }

    public static QlValue value(Map<String, Object> val) {
        Map<String, QlValue> asValues = QlIterables.newHashMapWithSize(val.size());
        Iterator var2 = val.entrySet().iterator();

        while(var2.hasNext()) {
            Map.Entry<String, Object> entry = (Map.Entry)var2.next();
            asValues.put(entry.getKey(), value(entry.getValue()));
        }

        return new QlMapValue(asValues);
    }

    public static QlValue value(LocalDate localDate) {
        return new QlDateValue(localDate);
    }

    public static QlValue value(OffsetTime offsetTime) {
        return new QlTimeValue(offsetTime);
    }

    public static QlValue value(LocalTime localTime) {
        return new QlLocalTimeValue(localTime);
    }

    public static QlValue value(LocalDateTime localDateTime) {
        return new QlLocalDateTimeValue(localDateTime);
    }

    public static QlValue value(OffsetDateTime offsetDateTime) {
        return new QlDateTimeValue(offsetDateTime.toZonedDateTime());
    }

    public static QlValue value(ZonedDateTime zonedDateTime) {
        return new QlDateTimeValue(zonedDateTime);
    }

    public static QlValue value(Period period) {
        return value((QlIsoDuration)(new QlInternalIsoDuration(period)));
    }

    public static QlValue value(Duration duration) {
        return value((QlIsoDuration)(new QlInternalIsoDuration(duration)));
    }

    public static QlValue isoDuration(long months, long days, long seconds, int nanoseconds) {
        return value((QlIsoDuration)(new QlInternalIsoDuration(months, days, seconds, nanoseconds)));
    }

    private static QlValue value(QlIsoDuration duration) {
        return new QlDurationValue(duration);
    }

    public static QlValue point(int srid, double x, double y) {
        return value((QlPoint)(new QlInternalPoint2D(srid, x, y)));
    }

    private static QlValue value(QlPoint point) {
        return new QlPointValue(point);
    }

    public static QlValue point(int srid, double x, double y, double z) {
        return value((QlPoint)(new QlInternalPoint3D(srid, x, y, z)));
    }

    public static QlValue parameters(Object... keysAndValues) {
        if (keysAndValues.length % 2 != 0) {
            throw new QlClientException("Parameters function requires an even number of arguments, alternating key and value. Arguments were: " + Arrays.toString(keysAndValues) + ".");
        } else {
            HashMap<String, QlValue> map = QlIterables.newHashMapWithSize(keysAndValues.length / 2);

            for(int i = 0; i < keysAndValues.length; i += 2) {
                Object value = keysAndValues[i + 1];
                QlExtract.assertParameter(value);
                map.put(keysAndValues[i].toString(), value(value));
            }

            return value((Object)map);
        }
    }

    public static Function<QlValue, QlValue> ofValue() {
        return (val) -> {
            return val;
        };
    }

    public static Function<QlValue, Object> ofObject() {
        return QlValue::asObject;
    }

    public static Function<QlValue, Number> ofNumber() {
        return QlValue::asNumber;
    }

    public static Function<QlValue, String> ofString() {
        return QlValue::asString;
    }

    public static Function<QlValue, String> ofToString() {
        return QlValue::toString;
    }

    public static Function<QlValue, Integer> ofInteger() {
        return QlValue::asInt;
    }

    public static Function<QlValue, Long> ofLong() {
        return QlValue::asLong;
    }

    public static Function<QlValue, Float> ofFloat() {
        return QlValue::asFloat;
    }

    public static Function<QlValue, Double> ofDouble() {
        return QlValue::asDouble;
    }

    public static Function<QlValue, Boolean> ofBoolean() {
        return QlValue::asBoolean;
    }

    public static Function<QlValue, Map<String, Object>> ofMap() {
        return QlMapAccessor::asMap;
    }

    public static <T> Function<QlValue, Map<String, T>> ofMap(Function<QlValue, T> valueConverter) {
        return (val) -> {
            return val.asMap(valueConverter);
        };
    }

    public static Function<QlValue, QlEntity> ofEntity() {
        return QlValue::asEntity;
    }

    public static Function<QlValue, Long> ofEntityId() {
        return (val) -> {
            return val.asEntity().id();
        };
    }

    public static Function<QlValue, QlNode> ofNode() {
        return QlValue::asNode;
    }

    public static Function<QlValue, QlRelationship> ofRelationship() {
        return QlValue::asRelationship;
    }

    public static Function<QlValue, QlPath> ofPath() {
        return QlValue::asPath;
    }

    public static Function<QlValue, LocalDate> ofLocalDate() {
        return QlValue::asLocalDate;
    }

    public static Function<QlValue, OffsetTime> ofOffsetTime() {
        return QlValue::asOffsetTime;
    }

    public static Function<QlValue, LocalTime> ofLocalTime() {
        return QlValue::asLocalTime;
    }

    public static Function<QlValue, LocalDateTime> ofLocalDateTime() {
        return QlValue::asLocalDateTime;
    }

    public static Function<QlValue, OffsetDateTime> ofOffsetDateTime() {
        return QlValue::asOffsetDateTime;
    }

    public static Function<QlValue, ZonedDateTime> ofZonedDateTime() {
        return QlValue::asZonedDateTime;
    }

    public static Function<QlValue, QlIsoDuration> ofIsoDuration() {
        return QlValue::asIsoDuration;
    }

    public static Function<QlValue, QlPoint> ofPoint() {
        return QlValue::asPoint;
    }

    public static Function<QlValue, List<Object>> ofList() {
        return QlValue::asList;
    }

    public static <T> Function<QlValue, List<T>> ofList(Function<QlValue, T> innerMap) {
        return (value) -> {
            return value.asList(innerMap);
        };
    }

    static {
        NULL = QlNullValue.NULL;
    }
    
}
