package com.meiya.whalex.db.entity.table.rawstatement.nebulagraph;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

/**
 * @author 黄河森
 * @date 2024/3/12
 * @package com.meiya.whalex.db.entity.table.rawstatement.nebulagraph
 * @project whalex-data-driver
 * @description NebulaGraphValueWrapper
 */
public interface NebulaGraphValueWrapper {

    boolean isEmpty();

    boolean isNull();

    boolean isBoolean();

    boolean isLong();

    boolean isDouble();

    boolean isString();

    boolean isList();

    boolean isSet();

    boolean isMap();

    boolean isTime();

    boolean isDate();

    boolean isDateTime();

    boolean isVertex();

    boolean isEdge();

    boolean isPath();

    boolean isGeography();

    boolean isDuration();

    NebulaGraphNullType asNull() throws Exception;

    boolean asBoolean() throws Exception;

    long asLong() throws Exception;

    String asString() throws Exception;

    double asDouble() throws Exception;

    ArrayList<NebulaGraphValueWrapper> asList() throws Exception;

    HashSet<NebulaGraphValueWrapper> asSet() throws Exception;

    HashMap<String, NebulaGraphValueWrapper> asMap() throws Exception;

    NebulaGraphTimeWrapper asTime() throws Exception;

    NebulaGraphDateWrapper asDate() throws Exception;

    NebulaGraphDateTimeWrapper asDateTime() throws Exception;

    NebulaGraphNode asNode() throws Exception;

    NebulaGraphRelationship asRelationship() throws Exception;

    NebulaGraphPathWrapper asPath() throws Exception;

    NebulaGraphGeographyWrapper asGeography() throws Exception;

    NebulaGraphDurationWrapper asDuration() throws Exception;

    interface NebulaGraphNullType {
        int getNullType();

        String toString();
    }

}
