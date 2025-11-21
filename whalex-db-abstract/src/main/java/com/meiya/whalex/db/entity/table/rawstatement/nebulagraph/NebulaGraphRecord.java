package com.meiya.whalex.db.entity.table.rawstatement.nebulagraph;

import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.function.Consumer;

/**
 * @author 黄河森
 * @date 2024/3/12
 * @package com.meiya.whalex.db.entity.table.rawstatement.nebulagraph
 * @project whalex-data-driver
 * @description NebulaGraphRecord
 */
public interface NebulaGraphRecord extends Iterable<NebulaGraphValueWrapper>{

    Iterator<NebulaGraphValueWrapper> iterator();

    void forEach(Consumer<? super NebulaGraphValueWrapper> action);

    Spliterator<NebulaGraphValueWrapper> spliterator();

    NebulaGraphValueWrapper get(int index);

    NebulaGraphValueWrapper get(String columnName);

    List<NebulaGraphValueWrapper> values();

    int size();

    boolean contains(String columnName);

}
