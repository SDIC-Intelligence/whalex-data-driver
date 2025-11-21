package com.meiya.whalex.db.entity.graph.rawstatement;

import com.meiya.whalex.db.entity.table.rawstatement.nebulagraph.NebulaGraphRecord;
import com.meiya.whalex.db.entity.table.rawstatement.nebulagraph.NebulaGraphValueWrapper;
import com.vesoft.nebula.client.graph.data.ResultSet;
import com.vesoft.nebula.client.graph.data.ValueWrapper;

import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.function.Consumer;

/**
 * @author 黄河森
 * @date 2024/3/12
 * @package com.meiya.whalex.db.entity.graph.rawstatement
 * @project whalex-data-driver
 * @description NebulaGraphRecordImpl
 */
public class NebulaGraphRecordImpl implements NebulaGraphRecord {

    private ResultSet.Record record;

    public NebulaGraphRecordImpl(ResultSet.Record record) {
        this.record = record;
    }

    @Override
    public Iterator<NebulaGraphValueWrapper> iterator() {
        Iterator<ValueWrapper> iterator = record.iterator();
        return new Iterator<NebulaGraphValueWrapper>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public NebulaGraphValueWrapper next() {
                ValueWrapper next = iterator.next();
                return new NebulaGraphValueWrapperImpl(next);
            }
        };
    }

    @Override
    public void forEach(Consumer<? super NebulaGraphValueWrapper> action) {

    }

    @Override
    public Spliterator<NebulaGraphValueWrapper> spliterator() {
        return null;
    }

    @Override
    public NebulaGraphValueWrapper get(int index) {
        return null;
    }

    @Override
    public NebulaGraphValueWrapper get(String columnName) {
        return null;
    }

    @Override
    public List<NebulaGraphValueWrapper> values() {
        return null;
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public boolean contains(String columnName) {
        return false;
    }
}
