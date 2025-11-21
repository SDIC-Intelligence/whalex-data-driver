package com.meiya.whalex.db.kafka.admin.client;

import org.apache.kafka.clients.admin.AbstractOptions;
import org.apache.kafka.common.annotation.InterfaceStability;

/**
 * @author 黄河森
 * @date 2021/9/19
 * @package com.meiya.whalex.db.kafka.admin.client
 * @project whalex-data-driver
 */
@InterfaceStability.Evolving
public class MyListConsumerGroupsOptions extends AbstractOptions<MyListConsumerGroupsOptions> {
    public MyListConsumerGroupsOptions() {
    }
}
