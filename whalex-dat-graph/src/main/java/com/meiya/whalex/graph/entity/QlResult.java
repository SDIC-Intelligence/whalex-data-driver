package com.meiya.whalex.graph.entity;

import com.meiya.whalex.graph.exception.QlNoSuchRecordException;

import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * CQL、GQL 等 SQL 类型的查询语言统一返参
 *
 * @author 黄河森
 * @date 2024/3/7
 * @package com.meiya.whalex.graph.entity
 * @project whalex-data-driver
 * @description QlResult
 */
public interface QlResult extends Iterator<QlRecord> {

    List<String> keys();

    boolean hasNext();

    QlRecord next();

    QlRecord single() throws QlNoSuchRecordException;

    QlRecord peek();

    Stream<QlRecord> stream();

    List<QlRecord> list();

    <T> List<T> list(Function<QlRecord, T> var1);

//    ResultSummary consume();

}
