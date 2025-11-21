package com.meiya.whalex.db.entity.graph.rawstatement;

import com.meiya.whalex.db.entity.table.rawstatement.nebulagraph.NebulaGraphRelationship;
import com.meiya.whalex.db.entity.table.rawstatement.nebulagraph.NebulaGraphValueWrapper;
import com.vesoft.nebula.client.graph.data.Relationship;
import com.vesoft.nebula.client.graph.data.ValueWrapper;
import lombok.Getter;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author 黄河森
 * @date 2024/3/15
 * @package com.meiya.whalex.db.entity.graph.rawstatement
 * @project whalex-data-driver
 * @description NebulaGraphRelationshipImpl
 */
@Getter
public class NebulaGraphRelationshipImpl implements NebulaGraphRelationship {

    private final Relationship relationship;

    public NebulaGraphRelationshipImpl(Relationship relationship) {
        this.relationship = relationship;
    }

    @Override
    public String getDecodeType() {
        return relationship.getDecodeType();
    }

    @Override
    public int getTimezoneOffset() {
        return relationship.getTimezoneOffset();
    }

    @Override
    public NebulaGraphValueWrapper srcId() {
        return new NebulaGraphValueWrapperImpl(relationship.srcId());
    }

    @Override
    public NebulaGraphValueWrapper dstId() {
        return new NebulaGraphValueWrapperImpl(relationship.dstId());
    }

    @Override
    public String edgeName() {
        return relationship.edgeName();
    }

    @Override
    public long ranking() {
        return relationship.ranking();
    }

    @Override
    public List<String> keys() throws UnsupportedEncodingException {
        return relationship.keys();
    }

    @Override
    public List<NebulaGraphValueWrapper> values() {
        return relationship.values().stream().flatMap(valueWrapper -> Stream.of(new NebulaGraphValueWrapperImpl(valueWrapper)))
                .collect(Collectors.toCollection(ArrayList::new));
    }

    @Override
    public HashMap<String, NebulaGraphValueWrapper> properties() throws UnsupportedEncodingException {
        HashMap<String, ValueWrapper> properties = relationship.properties();
        HashMap<String, NebulaGraphValueWrapper> nebulaGraphValueWrapperHashMap = new HashMap<>(properties.size());
        for (Map.Entry<String, ValueWrapper> entry : properties.entrySet()) {
            nebulaGraphValueWrapperHashMap.put(entry.getKey(), new NebulaGraphValueWrapperImpl(entry.getValue()));
        }
        return nebulaGraphValueWrapperHashMap;
    }
}
