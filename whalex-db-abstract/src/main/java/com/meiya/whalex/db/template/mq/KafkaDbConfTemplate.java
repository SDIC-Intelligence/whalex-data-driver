package com.meiya.whalex.db.template.mq;

import com.fasterxml.jackson.annotation.JsonValue;
import com.meiya.whalex.annotation.*;
import com.meiya.whalex.db.template.BaseDbConfTemplate;
import com.meiya.whalex.interior.db.constant.DbResourceEnum;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;

/**
 * Kafka 数据库配置模板
 *
 * @author 黄河森
 * @date 2020/1/7
 * @project whale-cloud-platformX
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@DbType(value = {DbResourceEnum.kafka})
public class KafkaDbConfTemplate extends BaseDbConfTemplate {

    /**
     * 服务地址
     */
    @Url
    private String serviceUrl;

    /**
     * 账号
     */
    @UserName
    private String userName;

    /**
     * 密码
     */
    @Password
    private String password;

    /**
     * JMX Port
     */
    @ExtendField(value = "jmx")
    private String jmx;

    /**
     * 消费组id
     */
    @ExtendField(value = "group")
    private String group;

    /**
     * 认证类型
     */
    @ExtendField(value = "authType")
    private String authType;

    /**
     * 认证证书
     */
    @ExtendField(value = "certificateId")
    private String certificateId;

    /**
     * krb5 地址
     */
    @ExtendField(value = "krb5Path")
    private String krb5Path;
    /**
     * userKeytab 地址
     */
    @ExtendField(value = "userKeytabPath")
    private String userKeytabPath;

    /**
     * kafka消费一次最大超时时间,超出该值,即使达不到设置的批量消费数据量也会返回
     */
    @ExtendField(value = "kafkaBulkConsumeTimeout")
    private long kafkaBulkConsumeTimeout = 1000;

    /**
     * kerberos.principal
     */
    @ExtendField(value = "kerberosPrincipal")
    private String kerberosPrincipal;

    /**
     * zk 命名空间
     */
    @ExtendField(value = "zooKeeperNamespace")
    private String zooKeeperNamespace = "zookeeper/hadoop.hadoop.com";

    /**
     * kerberos 认证服务名
     */
    @ExtendField(value = "kerberosServiceName")
    private String kerberosServiceName = "kafka";

    /**
     * 位移重置设置
     */
    @ExtendField(value = "autoOffsetReset")
    private OffsetResetStrategy autoOffsetReset = OffsetResetStrategy.EARLIEST;

    /**
     * 一次拉取最大数量
     */
    @ExtendField(value = "maxPollRecord")
    private Integer maxPollRecord = 10;

    /**
     * 最大消费时间间隔
     */
    @ExtendField(value = "maxPollIntervalMs")
    private Integer maxPollIntervalMs;

    /**
     * 是否开启自动提交
     */
    @ExtendField(value = "enableAutocommit")
    private Boolean enableAutocommit = Boolean.FALSE;

    /**
     * 消费者从服务获取记录的最小字节，如果不满足，则会等待到满足之后返回
     */
    @ExtendField(value = "fetchMinBytes")
    private Integer fetchMinBytes;

    /**
     * 消费者拉取等待最大时间，与 fetchMinByte 共同作用，两个条件满足其一即可返回
     */
    @ExtendField(value = "fetchMaxWait")
    private Integer fetchMaxWait;


    @ExtendField(value = "requestTimeout")
    private Integer requestTimeout;

    /**
     * 从每个分区消费的最大字节 默认  1MB
     */
    @ExtendField(value = "maxPartitionFetchBytes")
    private Integer maxPartitionFetchBytes;

    /**
     * 心跳超时时间
     */
    @ExtendField(value = "sessionTimeOut")
    private Integer sessionTimeOut;

    /**
     * enableAutocommit=true时，设置定时自动提交的执行间隔
     */
    @ExtendField(value = "autocommitInterval")
    private Integer autocommitInterval;

    /**
     * 分区策略
     * Range：把主题若干个连接的分区分配给消费者
     * Robin：把主题的所有分区逐个分配给消费者
     */
    @ExtendField(value = "partitionAssignmentStrategy")
    private PartitionAssignmentStrategy partitionAssignmentStrategy;

    /**
     * 应答级别
     */
    @ExtendField(value = "ack")
    private AckStrategy ack;

    /**
     * 缓存等待发送的消息大小
     */
    @ExtendField(value = "bufferMemory")
    private Integer bufferMemory;

    /**
     * 批量打包消息发送的大小
     */
    @ExtendField(value = "batchSize")
    private Integer batchSize;

    /**
     * 重试次数
     */
    @ExtendField(value = "retries")
    private Integer retries;

    /**
     * 延迟发送时间 ms
     */
    @ExtendField(value = "lingerMs")
    private Integer lingerMs;

    /**
     * 最大请求大小 字节
     */
    @ExtendField(value = "maxRequestSize")
    private Integer maxRequestSize;

    /**
     * 最大阻塞时间  send/partitionsFor 方法
     */
    @ExtendField(value = "maxBlockSize")
    private Integer maxBlockSize;

    @ExtendField(value = "certificateBase64Str")
    private String certificateBase64Str;

    /**
     * 消费策略
     */
    public enum OffsetResetStrategy {
        /**
         * 消费监听后新生产的数据
         */
        LATEST("latest"),
        EARLIEST("earliest");
        ;
        private String name;

        public static OffsetResetStrategy getEnumByValue(String value) {

            if(StringUtils.isBlank(value)) {
                return null;
            }

            OffsetResetStrategy[] values = OffsetResetStrategy.values();
            for (OffsetResetStrategy ors : values) {
                if(ors.name.equals(value)) {
                    return ors;
                }
            }
            throw new RuntimeException("未知的消费策略" + value);
        }

        OffsetResetStrategy(String name) {
            this.name = name;
        }

        @JsonValue
        public String getName() {
            return name;
        }
    }

    /**
     * 分区分配策略
     */
    public enum PartitionAssignmentStrategy {
        RANGE("Range"),
        ROBIN("Robin"),
        ;
        private String strategy;

        PartitionAssignmentStrategy(String strategy) {
            this.strategy = strategy;
        }

        public static PartitionAssignmentStrategy getEnumByValue(String value) {

            if(StringUtils.isBlank(value)) {
                return null;
            }

            PartitionAssignmentStrategy[] values = PartitionAssignmentStrategy.values();
            for (PartitionAssignmentStrategy ps : values) {
                if(ps.strategy.equals(value)) {
                    return ps;
                }
            }
            throw new RuntimeException("未知的分区分配策略" + value);
        }

        @JsonValue
        public String getStrategy() {
            return strategy;
        }
    }

    /**
     * 确认级别
     */
    public enum AckStrategy {
        AT_MOST_ONCE("0"),
        EXACTLY_ONCE("1"),
        AT_LEAST_ONCE("all"),
        ;
        private String ack;

        public static AckStrategy getEnumByValue(String value) {

            if(StringUtils.isBlank(value)) {
                return null;
            }

            AckStrategy[] values = AckStrategy.values();
            for (AckStrategy as : values) {
                if(as.ack.equals(value)) {
                    return as;
                }
            }
            throw new RuntimeException("未知的确认级别" + value);
        }

        AckStrategy(String ack) {
            this.ack = ack;
        }
        @JsonValue
        public String getAck() {
            return ack;
        }
    }
}
