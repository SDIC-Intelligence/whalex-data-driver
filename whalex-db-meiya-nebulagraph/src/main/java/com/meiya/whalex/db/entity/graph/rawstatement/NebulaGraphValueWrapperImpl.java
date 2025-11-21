package com.meiya.whalex.db.entity.graph.rawstatement;

import com.meiya.whalex.db.entity.table.rawstatement.nebulagraph.*;
import com.meiya.whalex.db.entity.table.rawstatement.nebulagraph.NebulaGraphNode;
import com.vesoft.nebula.client.graph.data.*;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author 黄河森
 * @date 2024/3/15
 * @package com.meiya.whalex.db.entity.graph.rawstatement
 * @project whalex-data-driver
 * @description NebulaGraphValueWrapperImpl
 */
public class NebulaGraphValueWrapperImpl implements NebulaGraphValueWrapper {

    private final ValueWrapper valueWrapper;

    public NebulaGraphValueWrapperImpl(ValueWrapper valueWrapper) {
        this.valueWrapper = valueWrapper;
    }

    @Override
    public boolean isEmpty() {
        return valueWrapper.isEmpty();
    }

    @Override
    public boolean isNull() {
        return valueWrapper.isNull();
    }

    @Override
    public boolean isBoolean() {
        return valueWrapper.isBoolean();
    }

    @Override
    public boolean isLong() {
        return valueWrapper.isLong();
    }

    @Override
    public boolean isDouble() {
        return valueWrapper.isDouble();
    }

    @Override
    public boolean isString() {
        return valueWrapper.isString();
    }

    @Override
    public boolean isList() {
        return valueWrapper.isList();
    }

    @Override
    public boolean isSet() {
        return valueWrapper.isSet();
    }

    @Override
    public boolean isMap() {
        return valueWrapper.isMap();
    }

    @Override
    public boolean isTime() {
        return valueWrapper.isTime();
    }

    @Override
    public boolean isDate() {
        return valueWrapper.isDate();
    }

    @Override
    public boolean isDateTime() {
        return valueWrapper.isDateTime();
    }

    @Override
    public boolean isVertex() {
        return valueWrapper.isVertex();
    }

    @Override
    public boolean isEdge() {
        return valueWrapper.isEdge();
    }

    @Override
    public boolean isPath() {
        return valueWrapper.isPath();
    }

    @Override
    public boolean isGeography() {
        return valueWrapper.isGeography();
    }

    @Override
    public boolean isDuration() {
        return valueWrapper.isDuration();
    }

    @Override
    public NebulaGraphNullType asNull() throws Exception {
        ValueWrapper.NullType aNull = valueWrapper.asNull();
        return new NebulaGraphNullTypeImpl(aNull);
    }

    @Override
    public boolean asBoolean() throws Exception {
        return valueWrapper.asBoolean();
    }

    @Override
    public long asLong() throws Exception {
        return valueWrapper.asLong();
    }

    @Override
    public String asString() throws Exception {
        return valueWrapper.asString();
    }

    @Override
    public double asDouble() throws Exception {
        return valueWrapper.asDouble();
    }

    @Override
    public ArrayList<NebulaGraphValueWrapper> asList() throws Exception {
        ArrayList<ValueWrapper> valueWrapperList = valueWrapper.asList();
        return valueWrapperList.stream().flatMap(valueWrapper1 -> Stream.of(new NebulaGraphValueWrapperImpl(valueWrapper1))).collect(Collectors.toCollection(ArrayList::new));
    }

    @Override
    public HashSet<NebulaGraphValueWrapper> asSet() throws Exception {
        HashSet<ValueWrapper> set = valueWrapper.asSet();
        return set.stream().flatMap(valueWrapper1 -> Stream.of(new NebulaGraphValueWrapperImpl(valueWrapper1))).collect(Collectors.toCollection(HashSet::new));
    }

    @Override
    public HashMap<String, NebulaGraphValueWrapper> asMap() throws Exception {
        HashMap<String, ValueWrapper> valueWrapperMap = valueWrapper.asMap();
        HashMap<String, NebulaGraphValueWrapper> nebulaGraphValueWrapperHashMap = new HashMap<>(valueWrapperMap.size());
        for (Map.Entry<String, ValueWrapper> entry : valueWrapperMap.entrySet()) {
            nebulaGraphValueWrapperHashMap.put(entry.getKey(), new NebulaGraphValueWrapperImpl(entry.getValue()));
        }
        return nebulaGraphValueWrapperHashMap;
    }

    @Override
    public NebulaGraphTimeWrapper asTime() throws Exception {
        TimeWrapper time = valueWrapper.asTime();
        return new NebulaGraphTimeWrapperImpl(time);
    }

    @Override
    public NebulaGraphDateWrapper asDate() throws Exception {
        DateWrapper date = valueWrapper.asDate();
        return new NebulaGraphDateWrapperImpl(date);
    }

    @Override
    public NebulaGraphDateTimeWrapper asDateTime() throws Exception {
        return new NebulaGraphDateTimeWrapperImpl(valueWrapper.asDateTime());
    }

    @Override
    public NebulaGraphNode asNode() throws Exception {
        Node node = valueWrapper.asNode();
        return new NebulaGraphNodeImpl(node);
    }

    @Override
    public NebulaGraphRelationship asRelationship() throws Exception {
        Relationship relationship = valueWrapper.asRelationship();
        return new NebulaGraphRelationshipImpl(relationship);
    }

    @Override
    public NebulaGraphPathWrapper asPath() throws Exception {
        return new NebulaGraphPathWrapperImpl(valueWrapper.asPath());
    }

    @Override
    public NebulaGraphGeographyWrapper asGeography() throws Exception {
        GeographyWrapper geography = valueWrapper.asGeography();
        return new NebulaGraphGeographyWrapperImpl(geography);
    }

    @Override
    public NebulaGraphDurationWrapper asDuration() throws Exception {
        return new NebulaGraphDurationWrapperImpl(valueWrapper.asDuration());
    }

    public static class NebulaGraphNullTypeImpl implements NebulaGraphNullType {

        private final ValueWrapper.NullType nullType;
        public NebulaGraphNullTypeImpl(ValueWrapper.NullType nullType) {
            this.nullType = nullType;
        }

        @Override
        public int getNullType() {
            return nullType.getNullType();
        }
    }
}
