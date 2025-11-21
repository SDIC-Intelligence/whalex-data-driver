package com.meiya.whalex.db.util;

import cn.hutool.core.map.MapUtil;
import com.meiya.whalex.db.entity.graph.NebulaGraphQlNode;
import com.meiya.whalex.db.entity.graph.NebulaGraphQlPath;
import com.meiya.whalex.db.entity.graph.NebulaGraphQlRelationship;
import com.meiya.whalex.graph.entity.*;
import com.meiya.whalex.graph.exception.QlUnsupportedEncodingException;
import com.meiya.whalex.graph.exception.QlValueException;
import com.vesoft.nebula.Geography;
import com.vesoft.nebula.client.graph.data.*;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.time.*;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author 黄河森
 * @date 2024/3/8
 * @package com.meiya.whalex.db.util
 * @project whalex-data-driver
 * @description NebulaGraphValueWrapperUtil
 */
public class NebulaGraphValueWrapperUtil {

    public static QlValue wrapper(ValueWrapper valueWrapper) throws UnsupportedEncodingException {
        QlValue qlValue = null;
        if (valueWrapper.isEmpty()) {
            qlValue = QlNullValue.NULL;
        } else if (valueWrapper.isNull()) {
            qlValue = QlNullValue.NULL;
        } else if (valueWrapper.isVertex()) {
            Node node = valueWrapper.asNode();
            qlValue = NebulaGraphQlNode.Builder.build(node).asValue();
        } else if (valueWrapper.isEdge()) {
            Relationship relationship = valueWrapper.asRelationship();
            qlValue = NebulaGraphQlRelationship.Builder.build(relationship).asValue();
        } else if (valueWrapper.isPath()) {
            PathWrapper path = valueWrapper.asPath();
            qlValue = NebulaGraphQlPath.Builder.build(path).asValue();
        } else if (valueWrapper.isBoolean()) {
            boolean aBoolean = valueWrapper.asBoolean();
            qlValue = QlBooleanValue.fromBoolean(aBoolean);
        } else if (valueWrapper.isDouble()) {
            double aDouble = valueWrapper.asDouble();
            qlValue = new QlFloatValue(aDouble);
        } else if (valueWrapper.isLong()) {
            long aLong = valueWrapper.asLong();
            qlValue = new QlFloatValue(Double.parseDouble(String.valueOf(aLong)));
        } else if (valueWrapper.isString()) {
            String string = valueWrapper.asString();
            qlValue = new QlStringValue(string);
        } else if (valueWrapper.isList()) {
            ArrayList<ValueWrapper> list = valueWrapper.asList();
            List<QlValue> collect = list.stream().flatMap(_valueWrapper -> {
                try {
                    return Stream.of(NebulaGraphValueWrapperUtil.wrapper(_valueWrapper));
                } catch (UnsupportedEncodingException e) {
                    throw new QlUnsupportedEncodingException(e.getMessage());
                }
            }).collect(Collectors.toList());
            qlValue = new QlListValue(collect.toArray(new QlValue[]{}));
        } else if (valueWrapper.isSet()) {
            HashSet<ValueWrapper> set = valueWrapper.asSet();
            List<QlValue> collect = set.stream().flatMap(_valueWrapper -> {
                try {
                    return Stream.of(NebulaGraphValueWrapperUtil.wrapper(_valueWrapper));
                } catch (UnsupportedEncodingException e) {
                    throw new QlUnsupportedEncodingException(e.getMessage());
                }
            }).collect(Collectors.toList());
            qlValue = new QlListValue(collect.toArray(new QlValue[]{}));
        } else if (valueWrapper.isMap()) {
            HashMap<String, ValueWrapper> map = valueWrapper.asMap();
            Map<String, QlValue> valueMap = map.entrySet().stream().flatMap(entry -> {
                try {
                    Map<String, QlValue> qlValueMap = MapUtil.builder(entry.getKey(), NebulaGraphValueWrapperUtil.wrapper(entry.getValue()))
                            .build();
                    return Stream.of(qlValueMap);
                } catch (UnsupportedEncodingException e) {
                    throw new QlUnsupportedEncodingException(e.getMessage());
                }
            }).reduce((a, b) -> {
                a.putAll(b);
                return a;
            }).orElseGet(HashMap::new);
            qlValue = new QlMapValue(valueMap);
        } else if (valueWrapper.isGeography()) {
            GeographyWrapper geographyWrapper = valueWrapper.asGeography();
            Geography geography = null;
            try {
                Field declaredField = GeographyWrapper.class.getDeclaredField("geography");
                geography = (Geography) declaredField.get(geographyWrapper);
            } catch (Exception e) {
                throw new QlValueException(e.getMessage());
            }
            switch (geography.getSetField()) {
                case 1:
                    PointWrapper pointWrapper = geographyWrapper.getPointWrapper();
                    CoordinateWrapper coordinate = pointWrapper.getCoordinate();
                    qlValue = new QlPointValue(new QlInternalPoint2D(coordinate.hashCode(), coordinate.getX(), coordinate.getY()));
                    break;
                case 2:
                    LineStringWrapper lineStringWrapper = geographyWrapper.getLineStringWrapper();
                    List<CoordinateWrapper> coordinateList = lineStringWrapper.getCoordinateList();
                    List<QlInternalPoint2D> collect = coordinateList.stream().flatMap(coordinateWrapper -> Stream.of(new QlInternalPoint2D(coordinateWrapper.hashCode(), coordinateWrapper.getX(), coordinateWrapper.getY())))
                            .collect(Collectors.toList());
                    qlValue = new QlLineStringValue(collect);
                    break;
                case 3:
                    PolygonWrapper polygonWrapper = geographyWrapper.getPolygonWrapper();
                    List<List<CoordinateWrapper>> coordListList = polygonWrapper.getCoordListList();
                    List<List<? extends QlPoint>> polygon = coordListList.stream().flatMap(coordinateWrappers -> Stream.of(coordinateWrappers.stream().flatMap(coordinateWrapper -> Stream.of(new QlInternalPoint2D(coordinateWrapper.hashCode(), coordinateWrapper.getX(), coordinateWrapper.getY())))
                            .collect(Collectors.toList()))).collect(Collectors.toList());
                    qlValue = new QlPolygonValue(polygon);
                    break;
            }
        } else if (valueWrapper.isDate()) {
            DateWrapper date = valueWrapper.asDate();
            qlValue = new QlDateValue(LocalDate.parse(date.toString()));
        } else if (valueWrapper.isDateTime()) {
            DateTimeWrapper dateTime = valueWrapper.asDateTime();
            qlValue = new QlDateTimeValue(ZonedDateTime.of(LocalDateTime.parse(dateTime.getUTCDateTimeStr()), ZoneOffset.ofHours(dateTime.getTimezoneOffset())));
        } else if (valueWrapper.isDuration()) {
            DurationWrapper duration = valueWrapper.asDuration();
            qlValue = new QlDurationValue(new QlInternalIsoDuration(Duration.parse(duration.toString())));
        } else if (valueWrapper.isTime()) {
            TimeWrapper time = valueWrapper.asTime();
            qlValue = new QlTimeValue(OffsetTime.of(LocalTime.parse(time.getUTCTimeStr()), ZoneOffset.ofHours(time.getTimezoneOffset())));
        }
        return qlValue;
    }

}
