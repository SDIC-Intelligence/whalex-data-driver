package com.meiya.whalex.db.entity.graph;

import com.meiya.whalex.db.util.NebulaGraphValueWrapperUtil;
import com.meiya.whalex.graph.entity.*;
import com.meiya.whalex.graph.exception.QlNoSuchRecordException;
import com.meiya.whalex.graph.exception.QlUnsupportedEncodingException;
import com.vesoft.nebula.client.graph.data.Node;
import com.vesoft.nebula.client.graph.data.ResultSet;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * @author 黄河森
 * @date 2024/3/7
 * @package com.meiya.whalex.db.entity.graph
 * @project whalex-data-driver
 * @description NebulaGraphQlResult
 */
public class NebulaGraphQlResult implements QlResult {

    private final ResultSet resultSet;

    private final int rowsSize;

    private AtomicInteger currentIndex = new AtomicInteger(0);

    public NebulaGraphQlResult(ResultSet resultSet) {
        this.resultSet = resultSet;
        this.rowsSize = resultSet.rowsSize();
    }

    @Override
    public List<String> keys() {
        return resultSet.keys();
    }

    @Override
    public boolean hasNext() {
        if (currentIndex.get() >= rowsSize) {
            return false;
        }
        return true;
    }

    @Override
    public QlRecord next() {
        ResultSet.Record record = this.resultSet.rowValues(currentIndex.getAndIncrement());
        List<QlValue> collect = record.values().stream().flatMap(valueWrapper -> {
            try {
                return Stream.of(NebulaGraphValueWrapperUtil.wrapper(valueWrapper));
            } catch (UnsupportedEncodingException e) {
                throw new QlUnsupportedEncodingException(e.getMessage());
            }
        }).collect(Collectors.toList());
        return new NebulaGraphQlRecord(keys(), collect.toArray(new QlValue[]{}));
    }

    @Override
    public QlRecord single() throws QlNoSuchRecordException {
        ResultSet.Record record = this.resultSet.rowValues(0);
        List<QlValue> collect = record.values().stream().flatMap(valueWrapper -> {
            try {
                return Stream.of(NebulaGraphValueWrapperUtil.wrapper(valueWrapper));
            } catch (UnsupportedEncodingException e) {
                throw new QlUnsupportedEncodingException(e.getMessage());
            }
        }).collect(Collectors.toList());
        return new NebulaGraphQlRecord(keys(), collect.toArray(new QlValue[]{}));
    }

    @Override
    public QlRecord peek() {
        ResultSet.Record record = this.resultSet.rowValues(currentIndex.get());
        List<QlValue> collect = record.values().stream().flatMap(valueWrapper -> {
            try {
                return Stream.of(NebulaGraphValueWrapperUtil.wrapper(valueWrapper));
            } catch (UnsupportedEncodingException e) {
                throw new QlUnsupportedEncodingException(e.getMessage());
            }
        }).collect(Collectors.toList());
        return new NebulaGraphQlRecord(keys(), collect.toArray(new QlValue[]{}));
    }

    @Override
    public Stream<QlRecord> stream() {
        Spliterator<QlRecord> spliterator = Spliterators.spliteratorUnknownSize(this, 1040);
        return StreamSupport.stream(spliterator, false);
    }

    @Override
    public List<QlRecord> list() {
        List<QlRecord> qlRecords = new ArrayList<>(this.resultSet.rowsSize());
        for (int i = 0; i < this.resultSet.rowsSize(); i++) {
            ResultSet.Record record = this.resultSet.rowValues(i);
            List<QlValue> collect = record.values().stream().flatMap(valueWrapper -> {
                try {
                    return Stream.of(NebulaGraphValueWrapperUtil.wrapper(valueWrapper));
                } catch (UnsupportedEncodingException e) {
                    throw new QlUnsupportedEncodingException(e.getMessage());
                }
            }).collect(Collectors.toList());
            qlRecords.add(new NebulaGraphQlRecord(keys(), collect.toArray(new QlValue[]{})));
        }
        return qlRecords;
    }

    @Override
    public <T> List<T> list(Function<QlRecord, T> function) {
        List<T> qlRecords = new ArrayList<>(this.resultSet.rowsSize());
        for (int i = 0; i < this.resultSet.rowsSize(); i++) {
            ResultSet.Record record = this.resultSet.rowValues(i);
            List<QlValue> collect = record.values().stream().flatMap(valueWrapper -> {
                try {
                    return Stream.of(NebulaGraphValueWrapperUtil.wrapper(valueWrapper));
                } catch (UnsupportedEncodingException e) {
                    throw new QlUnsupportedEncodingException(e.getMessage());
                }
            }).collect(Collectors.toList());
            qlRecords.add(function.apply(new NebulaGraphQlRecord(keys(), collect.toArray(new QlValue[]{}))));
        }
        return qlRecords;
    }
}
