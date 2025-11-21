package com.meiya.whalex.util.collection;

import java.util.*;

/**
 * @author 黄河森
 * @date 2019/11/26
 * @project whale-cloud-platformX
 */
public class CollectionUtil {

    public static final Set EMPTY_SET = new HashSet(1);

    public static final List EMPTY_LIST = new ArrayList<>(1);

    public static final List EXCEPTION_EMPTY_LIST = new ExceptionList(1);

    /**
     * 标识异常状态下返回的空集合
     */
    public static class ExceptionList<E> extends ArrayList<E> implements List<E> {
        public ExceptionList(int initialCapacity) {
            super(initialCapacity);
        }

        public ExceptionList() {
        }

        public ExceptionList(Collection<E> c) {
            super(c);
        }
    }

}
