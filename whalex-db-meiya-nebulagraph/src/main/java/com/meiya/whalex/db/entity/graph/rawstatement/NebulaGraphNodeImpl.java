package com.meiya.whalex.db.entity.graph.rawstatement;

import com.meiya.whalex.db.entity.table.rawstatement.nebulagraph.NebulaGraphNode;
import com.meiya.whalex.db.entity.table.rawstatement.nebulagraph.NebulaGraphValueWrapper;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.vesoft.nebula.client.graph.data.Node;
import com.vesoft.nebula.client.graph.data.ValueWrapper;
import lombok.Getter;

/**
 * @author 黄河森
 * @date 2024/3/12
 * @package com.meiya.whalex.db.entity.graph.rawstatement
 * @project whalex-data-driver
 * @description NebulaGraphNode
 */
@Getter
public class NebulaGraphNodeImpl implements NebulaGraphNode {

    private final Node node;

    public NebulaGraphNodeImpl(Node node) {
        this.node = node;
    }

    @Override
    public NebulaGraphValueWrapper getId() {
        return new NebulaGraphValueWrapperImpl(node.getId());
    }

    @Override
    public List<String> tagNames() {
        return node.tagNames();
    }

    @Override
    public List<String> labels() {
        return node.labels();
    }

    @Override
    public boolean hasTagName(String tagName) {
        return node.hasTagName(tagName);
    }

    @Override
    public boolean hasLabel(String tagName) {
        return node.hasLabel(tagName);
    }

    @Override
    public List<NebulaGraphValueWrapper> values(String tagName) {
        List<ValueWrapper> values = node.values(tagName);
        return values.stream().flatMap(valueWrapper -> Stream.of(new NebulaGraphValueWrapperImpl(valueWrapper))).collect(Collectors.toCollection(ArrayList::new));
    }

    @Override
    public List<String> keys(String tagName) throws UnsupportedEncodingException {
        return node.keys(tagName);
    }

    @Override
    public HashMap<String, NebulaGraphValueWrapper> properties(String tagName) throws UnsupportedEncodingException {
        HashMap<String, ValueWrapper> properties = node.properties(tagName);
        HashMap<String, NebulaGraphValueWrapper> nebulaGraphValueWrapperHashMap = new HashMap<>(properties.size());
        for (Map.Entry<String, ValueWrapper> entry : properties.entrySet()) {
            nebulaGraphValueWrapperHashMap.put(entry.getKey(), new NebulaGraphValueWrapperImpl(entry.getValue()));
        }
        return nebulaGraphValueWrapperHashMap;
    }
}
