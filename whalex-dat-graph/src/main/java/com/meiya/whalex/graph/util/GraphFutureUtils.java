package com.meiya.whalex.graph.util;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

/**
 * 图数据库 Future 构造工具
 *
 * @author 黄河森
 * @date 2022/12/28
 * @package com.meiya.whalex.graph.util
 * @project whalex-data-driver
 */
public class GraphFutureUtils {

    public static final <S, E> Future<E> map(final Future<S> future, final Function<S, E> function) {
        return new Future<E>() {

            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                return future.cancel(mayInterruptIfRunning);
            }

            @Override
            public boolean isCancelled() {
                return future.isCancelled();
            }

            @Override
            public boolean isDone() {
                return future.isDone();
            }

            @Override
            public E get() throws InterruptedException, ExecutionException {
                return function.apply(future.get());
            }

            @Override
            public E get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
                return function.apply(future.get(timeout, unit));
            }
        };
    }

}
