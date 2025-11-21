package com.meiya.whalex.db.stream;

import java.util.Iterator;

/**
 * 迭代器
 */
public interface StreamIterator<E> extends Iterator<E> {


    int getDataLength();

}
