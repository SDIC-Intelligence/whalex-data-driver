package com.meiya.whalex.db.entity.graph;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.util.StrUtil;
import com.meiya.whalex.db.util.param.impl.graph.ReturnType;
import com.meiya.whalex.graph.entity.*;
import com.meiya.whalex.graph.exception.GremlinParserException;
import lombok.AllArgsConstructor;
import org.neo4j.driver.Record;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.InternalNode;
import org.neo4j.driver.internal.InternalPath;
import org.neo4j.driver.internal.InternalRelationship;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author 黄河森
 * @date 2023/1/3
 * @package com.meiya.whalex.db.entity.graph
 * @project whalex-data-driver
 */
@AllArgsConstructor
public class Neo4jResult implements GraphResult {

    private Record record;

    /**
     * 当返回 values() 时，neo4j 一个 Record 对应所有属性
     * 而 Gremlin 一个 Result 对应一个属性
     * 所以要把 Record.values 拆分成多个 Result
     */
    private int currentIndex = 0;

    private ReturnType returnType;

    public Neo4jResult(Record record, ReturnType returnType) {
        this.record = record;
        this.returnType = returnType;
    }

    @Override
    public String getString() {
        return record.values().get(currentIndex).asString();
    }

    @Override
    public int getInt() {
        return record.values().get(currentIndex).asInt();
    }

    @Override
    public byte getByte() {
        return Byte.parseByte(record.values().get(currentIndex).asString());
    }

    @Override
    public short getShort() {
        return Short.parseShort(record.values().get(currentIndex).asString());
    }

    @Override
    public long getLong() {
        return record.values().get(currentIndex).asLong();
    }

    @Override
    public float getFloat() {
        return record.values().get(currentIndex).asFloat();
    }

    @Override
    public double getDouble() {
        return record.values().get(currentIndex).asDouble();
    }

    @Override
    public boolean getBoolean() {
        return record.values().get(currentIndex).asBoolean();
    }

    @Override
    public boolean isNull() {
        return CollectionUtil.isEmpty(record.values());
    }

    @Override
    public GraphVertex getVertex() {
        return new Neo4jVertex((InternalNode) record.values().get(currentIndex).asNode());
    }

    @Override
    public GraphEdge getEdge() {
        List<Value> values = record.values();
        InternalRelationship internalRelationship = null;
        Map<String, InternalNode> nodeMap = new HashMap<>(2);
        for (Value value : values) {
            Object object = value.asObject();
            if (object instanceof InternalRelationship) {
                internalRelationship = (InternalRelationship) object;
            } else if (object instanceof InternalNode) {
                InternalNode node = (InternalNode) object;
                nodeMap.put(String.valueOf(node.id()), node);
            } else {
                throw new GremlinParserException("解析结果集关系对象异常, 无法解析 [" + object.getClass().getName() + "]");
            }
        }
        if (internalRelationship == null) {
            throw new GremlinParserException("解析结果集关系对象异常, 未获取到结果集中存在关系对象!");
        }
        String startNodeId = String.valueOf(internalRelationship.startNodeId());
        String endNodeId = String.valueOf(internalRelationship.endNodeId());

        return new Neo4jEdge(internalRelationship,
                nodeMap.get(startNodeId), nodeMap.get(endNodeId));
    }

    @Override
    public GraphElement getElement() {
        return new Neo4jElement<>(record.values().get(currentIndex).asEntity());
    }

    @Override
    public GraphPath getPath() {
        return new Neo4jPath((InternalPath) record.values().get(currentIndex).asPath(), (ReturnType.PATH) returnType);
    }

    @Override
    public <V> GraphProperty<V> getProperty() {
        String key = record.keys().get(currentIndex);
        return new Neo4jProperty(StrUtil.sub(key, StrUtil.indexOf(key, '.') + 1, key.length()), record.values().get(currentIndex).asObject());
    }

    @Override
    public <V> GraphVertexProperty<V> getVertexProperty() {
        InternalNodeProperties object = (InternalNodeProperties) record.values().get(currentIndex).asObject();
        return new Neo4jVertexProperty<>(object);
    }

    @Override
    public Object getObject() {
        return this.record.values().get(currentIndex).asObject();
    }

    @Override
    public <T> T get(Class<? extends T> clazz) {
        return clazz.cast(this.record.values().get(currentIndex).asObject());
    }

    @Override
    public String toString() {
        String c = this.getObject() != null ? this.getObject().getClass().getCanonicalName() : "null";
        return "result{object=" + this.getObject() + " class=" + c + '}';
    }
}
