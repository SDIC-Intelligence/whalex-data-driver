package com.meiya.whalex.db.kafka.admin.client;

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.internals.KafkaFutureImpl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

/**
 * 自定义 kafka 消费者组返回结果
 *
 * @author 黄河森
 * @date 2021/9/19
 * @package com.meiya.whalex.db.kafka.admin.client
 * @project whalex-data-driver
 */
@InterfaceStability.Evolving
public class MyListConsumerGroupsResult {

    private final KafkaFutureImpl<Collection<MyConsumerGroupListing>> all = new KafkaFutureImpl();
    private final KafkaFutureImpl<Collection<MyConsumerGroupListing>> valid = new KafkaFutureImpl();
    private final KafkaFutureImpl<Collection<Throwable>> errors = new KafkaFutureImpl();

    MyListConsumerGroupsResult(KafkaFutureImpl<Collection<Object>> future) {
        future.thenApply((KafkaFuture.BaseFunction<Collection<Object>, Void>) results -> {
            ArrayList<Throwable> curErrors = new ArrayList();
            ArrayList<MyConsumerGroupListing> curValid = new ArrayList();
            Iterator var4 = results.iterator();

            while(var4.hasNext()) {
                Object resultObject = var4.next();
                if (resultObject instanceof Throwable) {
                    curErrors.add((Throwable)resultObject);
                } else {
                    curValid.add((MyConsumerGroupListing)resultObject);
                }
            }

            if (!curErrors.isEmpty()) {
                MyListConsumerGroupsResult.this.all.completeExceptionally((Throwable)curErrors.get(0));
            } else {
                MyListConsumerGroupsResult.this.all.complete(curValid);
            }

            MyListConsumerGroupsResult.this.valid.complete(curValid);
            MyListConsumerGroupsResult.this.errors.complete(curErrors);
            return null;
        });
    }

    public KafkaFuture<Collection<MyConsumerGroupListing>> all() {
        return this.all;
    }

    public KafkaFuture<Collection<MyConsumerGroupListing>> valid() {
        return this.valid;
    }

    public KafkaFuture<Collection<Throwable>> errors() {
        return this.errors;
    }

}
