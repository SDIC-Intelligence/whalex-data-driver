package com.meiya.whalex.db.entity.graph;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.map.MapUtil;
import cn.hutool.core.util.StrUtil;
import com.meiya.whalex.db.util.param.impl.graph.ReturnType;
import com.meiya.whalex.graph.entity.GraphResult;
import com.meiya.whalex.graph.entity.GraphResultSet;
import org.neo4j.driver.Record;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.InternalNode;
import org.neo4j.driver.internal.InternalRecord;
import org.neo4j.driver.internal.value.ListValue;
import org.neo4j.driver.internal.value.MapValue;
import org.neo4j.driver.internal.value.NullValue;
import org.neo4j.driver.types.Node;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Neo4J 基础数据集
 *
 * @author 黄河森
 * @date 2023/3/28
 * @package com.meiya.whalex.db.entity.graph
 * @project whalex-data-driver
 */
public abstract class Neo4jBaseResultSet<R, S> implements GraphResultSet {

    R result;

    S session;

    ReturnType returnType;

    /**
     * 当返回 values() 时，neo4j 一个 Record 对应所有属性
     * 而 Gremlin 一个 Result 对应一个属性
     * 所以要把 Record.values 拆分成多个 Result
     */
    Record record;

    int currentIndex = 0;

    /**
     * .values() 和 .properties() 方法，一行记录根据列拆分为多个返回对象时需要使用的遍历器
     */
    Iterator<Map.Entry<String, Value>> currentEntrySet;

    /**
     * .properties() 方法下，当前属性记录对应的节点信息
     */
    private InternalNode propertiesNode;

    public Neo4jBaseResultSet(R result, S session, ReturnType returnType) {
        this.result = result;
        this.session = session;
        this.returnType = returnType;
    }

    /**
     * 获取下一个结果
     *
     * @return
     */
    protected abstract Record resultNext();

    /**
     * 异步获取所有记录
     *
     * @return
     */
    protected abstract CompletionStage<List<Record>> resultAll();

    /**
     * 关闭Session
     */
    protected abstract void close();

    @Override
    public GraphResult one() {
        if (currentEntrySet != null) {
            if (!currentEntrySet.hasNext()) {
                currentEntrySet = null;
                if (returnType instanceof ReturnType.PROPERTIES || returnType instanceof ReturnType.PROPERTIES_ALL) {
                    this.propertiesNode = null;
                }
            } else {
                if (returnType instanceof ReturnType.VALUES_ALL) {
                    Map.Entry<String, Value> next = currentEntrySet.next();
                    return new Neo4jResult(new InternalRecord(CollectionUtil.newArrayList(next.getKey())
                            , new Value[]{next.getValue()}), returnType);
                } else if (returnType instanceof ReturnType.PROPERTIES || returnType instanceof ReturnType.PROPERTIES_ALL) {
                    Map.Entry<String, Value> next = currentEntrySet.next();
                    NodeProperties nodeProperties = new InternalNodeProperties<>(this.propertiesNode, next.getKey(), next.getValue());
                    return new Neo4jResult(new InternalRecord(CollectionUtil.newArrayList(next.getKey()), new Value[]{new NodePropertiesValue(nodeProperties)}), returnType);
                }
            }
        }
        if (record != null) {
            if (record.values().size() == currentIndex) {
                record = resultNext();
                currentIndex = 0;
            }
        } else {
            record = resultNext();
        }
        if (record == null) {
            return null;
        }
        if (returnType instanceof ReturnType.VALUES_ALL) {
            Value value = record.get(currentIndex++);
            Map<String, Value> asMap = value.asMap(new Function<Value, Value>() {
                @Override
                public Value apply(Value value) {
                    return value;
                }
            });
            currentEntrySet = asMap.entrySet().iterator();
            Map.Entry<String, Value> next = currentEntrySet.next();
            return new Neo4jResult(new InternalRecord(CollectionUtil.newArrayList(next.getKey())
                    , new Value[]{next.getValue()}), returnType);
        } else if (returnType instanceof ReturnType.VALUE_MAP) {
            return getValueMap(record);
        } else if (returnType instanceof ReturnType.VALUE_MAP_ALL) {
            return getValueMapAll(record);
        } else if (returnType instanceof ReturnType.LABEL) {
            return getLabelValue(record);
        } else if (returnType instanceof ReturnType.PROPERTIES_ALL) {
            return getPropertiesAllForOne(record);
        } else if (returnType instanceof ReturnType.PROPERTIES) {
            return getPropertiesForOne(record);
        } else if (returnType instanceof ReturnType.E) {
            currentIndex = record.values().size();
            return new Neo4jResult(record, returnType);
        } else if (returnType instanceof ReturnType.PATH) {
            currentIndex = record.values().size();
            return new Neo4jResult(record, returnType);
        } else {
            return new Neo4jResult(record, currentIndex++, returnType);
        }
    }

    /**
     * 解析节点标签结果，返回单个对象
     *
     * @param record
     * @return
     */
    private GraphResult getLabelValue(Record record) {
        List<Value> values = record.values();
        currentIndex = values.size();
        List<Value> collect = values.stream().flatMap(value -> {
            ListValue listValue = (ListValue) value;
            if (!listValue.isEmpty()) {
                return Stream.of(listValue.get(0));
            } else {
                return Stream.of(NullValue.NULL);
            }
        }).collect(Collectors.toList());

        return new Neo4jResult(new InternalRecord(record.keys(), collect.toArray(new Value[collect.size()])), returnType);
    }

    /**
     * 解析 .properties()
     *
     * @param record
     * @return
     */
    private List<GraphResult> getPropertiesAll(Record record) {
        List<GraphResult> graphResultList = new ArrayList<>();
        Value value = record.values().get(0);
        Node node = value.asNode();
        Iterable<String> labels = node.labels();
        long id = node.id();
        Map<String, Value> properties = node.asMap(new Function<Value, Value>() {
            @Override
            public Value apply(Value value) {
                return value;
            }
        });
        InternalNode internalNode = new InternalNode(id, CollectionUtil.list(false, labels), properties);
        for (Map.Entry<String, Value> entry : properties.entrySet()) {
            NodeProperties nodeProperties = new InternalNodeProperties<>(internalNode, entry.getKey(), entry.getValue());
            graphResultList.add(new Neo4jResult(new InternalRecord(CollectionUtil.newArrayList(entry.getKey()), new Value[]{new NodePropertiesValue(nodeProperties)}), returnType));
        }
        return graphResultList;
    }

    /**
     * 解析 .properties()
     *
     * @param record
     * @return
     */
    private GraphResult getPropertiesAllForOne(Record record) {
        Value value = record.values().get(currentIndex++);
        Node node = value.asNode();
        Iterable<String> labels = node.labels();
        long id = node.id();
        Map<String, Value> properties = node.asMap(new Function<Value, Value>() {
            @Override
            public Value apply(Value value) {
                return value;
            }
        });
        InternalNode internalNode = new InternalNode(id, CollectionUtil.list(false, labels), properties);
        Set<Map.Entry<String, Value>> entries = properties.entrySet();
        Map.Entry<String, Value> next;
        if (entries.size() == 1) {
            next = entries.iterator().next();
        } else {
            this.currentEntrySet = entries.iterator();
            this.propertiesNode = internalNode;
            next = this.currentEntrySet.next();
        }
        NodeProperties nodeProperties = new InternalNodeProperties<>(internalNode, next.getKey(), next.getValue());
        return new Neo4jResult(new InternalRecord(CollectionUtil.newArrayList(next.getKey()), new Value[]{new NodePropertiesValue(nodeProperties)}), returnType);
    }

    /**
     * 解析 .properties(’xx‘)
     *
     * @param record
     * @return
     */
    private List<GraphResult> getProperties(Record record) {
        List<GraphResult> graphResultList = new ArrayList<>();
        Map<String, Value> asMap = record.asMap(new Function<Value, Value>() {
            @Override
            public Value apply(Value value) {
                return value;
            }
        });
        Iterator<Map.Entry<String, Value>> entryIterator = asMap.entrySet().iterator();
        int idValue = entryIterator.next().getValue().asInt();
        List<String> labelValue = entryIterator.next().getValue().asList(new Function<Value, String>() {
            @Override
            public String apply(Value value) {
                return value.asString();
            }
        });
        InternalNode internalNode = new InternalNode(idValue, CollectionUtil.list(false, labelValue), MapUtil.newHashMap());
        while (entryIterator.hasNext()) {
            Map.Entry<String, Value> entry = entryIterator.next();
            NodeProperties nodeProperties = new InternalNodeProperties<>(internalNode, entry.getKey(), entry.getValue());
            graphResultList.add(new Neo4jResult(new InternalRecord(CollectionUtil.newArrayList(entry.getKey()), new Value[]{new NodePropertiesValue(nodeProperties)}), returnType));
        }
        return graphResultList;
    }

    /**
     * 解析 .properties(’xx‘)
     *
     * @param record
     * @return
     */
    private GraphResult getPropertiesForOne(Record record) {
        Map<String, Value> asMap = record.asMap(new Function<Value, Value>() {
            @Override
            public Value apply(Value value) {
                return value;
            }
        });
        currentIndex = asMap.size();
        Value id = asMap.remove("id");
        Value labels = asMap.remove("labels");
        int idValue = id.asInt();
        List<String> labelValue = labels.asList(new Function<Value, String>() {
            @Override
            public String apply(Value value) {
                return value.asString();
            }
        });
        Set<Map.Entry<String, Value>> entries = asMap.entrySet();
        InternalNode internalNode = new InternalNode(idValue, CollectionUtil.list(false, labelValue), MapUtil.newHashMap());
        Map.Entry<String, Value> next;
        if (entries.size() == 1) {
            next = entries.iterator().next();
        } else {
            this.currentEntrySet = entries.iterator();
            this.propertiesNode = internalNode;
            next = this.currentEntrySet.next();
        }
        NodeProperties nodeProperties = new InternalNodeProperties<>(internalNode, next.getKey(), next.getValue());
        return new Neo4jResult(new InternalRecord(CollectionUtil.newArrayList(next.getKey()), new Value[]{new NodePropertiesValue(nodeProperties)}), returnType);
    }

    /**
     * 获取 valueMap() 对象
     *
     * @param record
     * @return
     */
    private GraphResult getValueMapAll(Record record) {
        Map<String, Value> asMap = record.asMap(new Function<Value, Value>() {
            @Override
            public Value apply(Value value) {
                MapValue mapValue = (MapValue) value;
                Map<String, Value> map = mapValue.asMap(new Function<Value, Value>() {
                    @Override
                    public Value apply(Value value) {
                        return new ListValue(value);
                    }
                });
                return new MapValue(map);
            }
        });
        currentIndex = asMap.size();
        return new Neo4jResult(new InternalRecord(record.keys(), new Value[]{asMap.get(record.keys().get(0))}), returnType);
    }

    /**
     * 获取 valueMap(property) 对象
     *
     * @param record
     * @return
     */
    private GraphResult getValueMap(Record record) {
        List<String> keys = record.keys();
        List<Value> values = record.values();
        currentIndex = values.size();
        Map<String, Value> _map = new HashMap<>(keys.size());
        for (int i = 0; i < keys.size(); i++) {
            String key = keys.get(i);
            Value value = values.get(i);
            _map.put(StrUtil.sub(key, StrUtil.indexOf(key, '.') + 1, key.length()), new ListValue(value));
        }
        return new Neo4jResult(new InternalRecord(CollectionUtil.newArrayList("valueMap"), new Value[]{new MapValue(_map)}), returnType);
    }

    @Override
    public Stream<GraphResult> stream() {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(this.iterator(), 1088), false);
    }

    @Override
    public Iterator<GraphResult> iterator() {
        return new Iterator<GraphResult>() {
            private GraphResult nextOne = null;

            @Override
            public boolean hasNext() {
                if (null == this.nextOne) {
                    this.nextOne = one();
                }

                boolean b = this.nextOne != null;

                if (!b) {
                    close();
                }

                return b;
            }

            @Override
            public GraphResult next() {
                if (null == this.nextOne && !this.hasNext()) {
                    throw new NoSuchElementException();
                } else {
                    GraphResult r = this.nextOne;
                    this.nextOne = null;
                    return r;
                }
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    public CompletableFuture<List<GraphResult>> all() {
        return resultAll().thenApply(new Function<List<Record>, List<GraphResult>>() {
            @Override
            public List<GraphResult> apply(List<Record> records) {
                List<GraphResult> graphResults = new ArrayList<>();
                for (Record _record : records) {
                    if (returnType instanceof ReturnType.VALUES || returnType instanceof ReturnType.VALUES_ALL) {
                        List<Value> values = _record.values();
                        for (int i = 0; i < values.size(); i++) {
                            if (returnType instanceof ReturnType.VALUES_ALL) {
                                Value value = values.get(i);
                                Map<String, Value> _valueMap = value.asMap(new Function<Value, Value>() {
                                    @Override
                                    public Value apply(Value value) {
                                        return value;
                                    }
                                });
                                for (Map.Entry<String, Value> entry : _valueMap.entrySet()) {
                                    Record internalRecord = new InternalRecord(CollectionUtil.newArrayList(entry.getKey())
                                            , new Value[]{entry.getValue()});
                                    graphResults.add(new Neo4jResult(internalRecord, returnType));
                                }
                            } else {
                                graphResults.add(new Neo4jResult(_record, i, returnType));
                            }
                        }
                    } else if (returnType instanceof ReturnType.VALUE_MAP) {
                        GraphResult valueMap = getValueMap(_record);
                        graphResults.add(valueMap);
                    } else if (returnType instanceof ReturnType.VALUE_MAP_ALL) {
                        GraphResult valueMapAll = getValueMapAll(_record);
                        graphResults.add(valueMapAll);
                    } else if (returnType instanceof ReturnType.LABEL) {
                        GraphResult labelValue = getLabelValue(_record);
                        graphResults.add(labelValue);
                    } else if (returnType instanceof ReturnType.PROPERTIES_ALL) {
                        List<GraphResult> labelValue = getPropertiesAll(_record);
                        graphResults.addAll(labelValue);
                    } else if (returnType instanceof ReturnType.PROPERTIES) {
                        List<GraphResult> labelValue = getProperties(_record);
                        graphResults.addAll(labelValue);
                    } else {
                        graphResults.add(new Neo4jResult(_record, returnType));
                    }
                }
                return graphResults;
            }
        }).whenCompleteAsync(new BiConsumer<List<GraphResult>, Throwable>() {
            @Override
            public void accept(List<GraphResult> graphResults, Throwable throwable) {
                close();
            }
        }).toCompletableFuture();
    }
}
