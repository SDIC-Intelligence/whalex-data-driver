package com.meiya.whalex.db.entity.stream;

import com.meiya.whalex.db.entity.AbstractDbHandler;
import lombok.Data;

import java.util.List;

/**
 * kafka 操作实体
 */
public class KafkaHandler extends AbstractDbHandler {

    private KafkaResourceInsert kafkaResourceInsert;

    private KafkaResourceQuery kafkaResourceQuery;

    private KafkaResourceCreate kafkaResourceCreate;

    private KafkaListTable kafkaListTable;

    public KafkaResourceCreate getKafkaResourceCreate() {
        return kafkaResourceCreate;
    }

    public void setKafkaResourceCreate(KafkaResourceCreate kafkaResourceCreate) {
        this.kafkaResourceCreate = kafkaResourceCreate;
    }

    public KafkaResourceQuery getKafkaResourceQuery() {
        return kafkaResourceQuery;
    }

    public void setKafkaResourceQuery(KafkaResourceQuery kafkaResourceQuery) {
        this.kafkaResourceQuery = kafkaResourceQuery;
    }

    public KafkaResourceInsert getKafkaResourceInsert() {
        return kafkaResourceInsert;
    }

    public void setKafkaResourceInsert(KafkaResourceInsert kafkaResourceInsert) {
        this.kafkaResourceInsert = kafkaResourceInsert;
    }

    public KafkaListTable getKafkaListTable() {
        return kafkaListTable;
    }

    public void setKafkaListTable(KafkaListTable kafkaListTable) {
        this.kafkaListTable = kafkaListTable;
    }

    /**
     * kafka 插入实体
     */
    public static class KafkaResourceInsert {

        private List<KafkaMessage> kafkaInsertList;

        public List<KafkaMessage> getKafkaInsertList() {
            return kafkaInsertList;
        }

        public void setKafkaInsertList(List kafkaInsertList) {
            this.kafkaInsertList = kafkaInsertList;
        }
    }

    /**
     * 消息体
     */
    @Data
    public static class KafkaMessage {

        private Object key;

        private Object message;
    }


    /**
     * kafka 查询实体
     */
    public static class KafkaResourceQuery {

        private Long limit;

        private Long offset;

        private Boolean count;

        private Long beginOffsetTimestamp;

        private Long endOffsetTimestamp;

        public Long getLimit() {
            return limit;
        }

        public void setLimit(Long limit) {
            this.limit = limit;
        }

        public Long getOffset() {
            return offset;
        }

        public void setOffset(Long offset) {
            this.offset = offset;
        }

        public Boolean getCount() {
            return count;
        }

        public void setCount(Boolean count) {
            this.count = count;
        }

        public Long getBeginOffsetTimestamp() {
            return beginOffsetTimestamp;
        }

        public void setBeginOffsetTimestamp(Long beginOffsetTimestamp) {
            this.beginOffsetTimestamp = beginOffsetTimestamp;
        }

        public Long getEndOffsetTimestamp() {
            return endOffsetTimestamp;
        }

        public void setEndOffsetTimestamp(Long endOffsetTimestamp) {
            this.endOffsetTimestamp = endOffsetTimestamp;
        }
    }

    /**
     * kafka topic新增实体
     */
    @Deprecated
    public static class KafkaResourceCreate {
        @Deprecated
        private Integer numPartitions;
        @Deprecated
        private Integer replicationFactor;
        @Deprecated
        private Integer retentionTime;
        @Deprecated
        public Integer getNumPartitions() {
            return numPartitions;
        }
        @Deprecated
        public void setNumPartitions(Integer numPartitions) {
            this.numPartitions = numPartitions;
        }
        @Deprecated
        public Integer getReplicationFactor() {
            return replicationFactor;
        }
        @Deprecated
        public void setReplicationFactor(Integer replicationFactor) {
            this.replicationFactor = replicationFactor;
        }
        @Deprecated
        public Integer getRetentionTime() {
            return retentionTime;
        }
        @Deprecated
        public void setRetentionTime(Integer retentionTime) {
            this.retentionTime = retentionTime;
        }
    }

    @Data
    public static class KafkaListTable {
        private String tableMatch;
    }


}
