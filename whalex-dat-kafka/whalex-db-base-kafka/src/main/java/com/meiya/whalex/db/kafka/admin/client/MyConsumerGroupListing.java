package com.meiya.whalex.db.kafka.admin.client;

/**
 * 自定义分组信息
 *
 * @author 黄河森
 * @date 2021/9/19
 * @package com.meiya.whalex.db.kafka.admin.client
 * @project whalex-data-driver
 */
public class MyConsumerGroupListing {

    private final String groupId;
    private final boolean isSimpleConsumerGroup;

    public MyConsumerGroupListing(String groupId, boolean isSimpleConsumerGroup) {
        this.groupId = groupId;
        this.isSimpleConsumerGroup = isSimpleConsumerGroup;
    }

    public String groupId() {
        return this.groupId;
    }

    public boolean isSimpleConsumerGroup() {
        return this.isSimpleConsumerGroup;
    }

    @Override
    public String toString() {
        return "(groupId='" + this.groupId + '\'' + ", isSimpleConsumerGroup=" + this.isSimpleConsumerGroup + ')';
    }

}
