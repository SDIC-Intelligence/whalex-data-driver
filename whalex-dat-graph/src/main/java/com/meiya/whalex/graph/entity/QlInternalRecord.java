package com.meiya.whalex.graph.entity;

import com.meiya.whalex.graph.util.QlFormat;
import com.meiya.whalex.graph.util.QlQueryKeys;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author 黄河森
 * @date 2024/3/8
 * @package com.meiya.whalex.graph.entity
 * @project whalex-data-driver
 * @description QlInternalRecord
 */
public class QlInternalRecord extends InternalQlMapAccessorWithDefaultValue implements QlRecord {

    private final QlQueryKeys queryKeys;
    private final QlValue[] values;
    private int hashCode = 0;

    public QlInternalRecord(List<String> keys, QlValue[] values) {
        this.queryKeys = new QlQueryKeys(keys);
        this.values = values;
    }

    public QlInternalRecord(QlQueryKeys queryKeys, QlValue[] values) {
        this.queryKeys = queryKeys;
        this.values = values;
    }

    public List<String> keys() {
        return this.queryKeys.keys();
    }

    public List<QlValue> values() {
        return Arrays.asList(this.values);
    }

    public <T> Iterable<T> values(Function<QlValue, T> mapFunction) {
        return (Iterable)this.values().stream().map(mapFunction).collect(Collectors.toList());
    }

    public List<QlPair<String, QlValue>> fields() {
        return QlExtract.fields(this, QlValues.ofValue());
    }

    public int index(String key) {
        int result = this.queryKeys.indexOf(key);
        if (result == -1) {
            throw new NoSuchElementException("Unknown key: " + key);
        } else {
            return result;
        }
    }

    public boolean containsKey(String key) {
        return this.queryKeys.contains(key);
    }

    public QlValue get(String key) {
        int fieldIndex = this.queryKeys.indexOf(key);
        return fieldIndex == -1 ? QlValues.NULL : this.values[fieldIndex];
    }

    public QlValue get(int index) {
        return index >= 0 && index < this.values.length ? this.values[index] : QlValues.NULL;
    }

    public int size() {
        return this.values.length;
    }

    public Map<String, Object> asMap() {
        return QlExtract.map(this, QlValues.ofObject());
    }

    public <T> Map<String, T> asMap(Function<QlValue, T> mapper) {
        return QlExtract.map(this, mapper);
    }

    public String toString() {
        return String.format("Record<%s>", QlFormat.formatPairs(this.asMap(QlValues.ofValue())));
    }

    public boolean equals(Object other) {
        if (this == other) {
            return true;
        } else if (other instanceof QlRecord) {
            QlRecord otherRecord = (QlRecord)other;
            int size = this.size();
            if (size != otherRecord.size()) {
                return false;
            } else if (!this.queryKeys.keys().equals(otherRecord.keys())) {
                return false;
            } else {
                for(int i = 0; i < size; ++i) {
                    QlValue value = this.get(i);
                    QlValue otherValue = otherRecord.get(i);
                    if (!value.equals(otherValue)) {
                        return false;
                    }
                }

                return true;
            }
        } else {
            return false;
        }
    }

    public int hashCode() {
        if (this.hashCode == 0) {
            this.hashCode = 31 * this.queryKeys.hashCode() + Arrays.hashCode(this.values);
        }

        return this.hashCode;
    }

}
