package com.meiya.whalex.db.entity.stream;

import com.meiya.whalex.db.entity.AbstractDatabaseInfo;
import com.meiya.whalex.db.util.param.impl.stream.KafkaConstantsProperty;
import lombok.Data;

import java.util.Properties;

/**
 * 数据库配置
 */
@Data
public class KafkaDatabaseInfo extends AbstractDatabaseInfo {

    /**
     * 服务地址
     */
    private String serviceUrl;

    /**
     * 账号
     */
    private String userName;

    /**
     * 密码
     */
    private String password;

    /**
     * JMX Port
     */
    private String jmx;

    /**
     * 消费组id
     */
    private String group;

    /**
     * 认证类型
     */
    private String authType;

    /**
     * 认证证书
     */
    private String certificateId;

    private String certificateBase64Str;

    /**
     * krb5 地址
     */
    private String krb5Path;
    /**
     * userKeytab 地址
     */
    private String userKeytabPath;

    /**
     * 证书保存的目录
     */
    private String parentDir;

    /**
     * kafka消费一次最大超时时间,超出该值,即使达不到设置的批量消费数据量也会返回
     */
    private long kafkaBulkConsumeTimeout = 1000;

    /**
     * kerberos.principal
     */
    private String kerberosPrincipal;

    /**
     * zk 命名空间
     */
    private String zooKeeperNamespace = "zookeeper/hadoop.hadoop.com";

    /**
     * kerberos 认证服务名
     */
    private String kerberosServiceName = "kafka";

    //-------------- 消费者参数 --------------//

    /**
     * 位移重置设置
     */
    private String autoOffsetReset = KafkaConstantsProperty.autoOffsetReset;

    /**
     * 一次拉取最大数量
     */
    private Integer maxPollRecord = KafkaConstantsProperty.maxPollRecords;

    /**
     * 最大消费时间间隔
     */
    private Integer maxPollIntervalMs;

    /**
     * 是否开启自动提交
     */
    private Boolean enableAutocommit = KafkaConstantsProperty.enableAutoCommit;

    /**
     * 消费者从服务获取记录的最小字节，如果不满足，则会等待到满足之后返回
     */
    private Integer fetchMinBytes;

    /**
     * 消费者拉取等待最大时间，与 fetchMinByte 共同作用，两个条件满足其一即可返回
     */
    private Integer fetchMaxWait;

    /**
     * 从每个分区消费的最大字节 默认  1MB
     */
    private Integer maxPartitionFetchBytes;

    /**
     * 心跳超时时间
     */
    private Integer sessionTimeOut;

    /**
     * 请求超时时间
     */
    private Integer requestTimeout;
    
    /**
     * enableAutocommit=true时，设置定时自动提交的执行间隔
     */
    private Integer autocommitInterval;

    /**
     * 分区策略
     * Range：把主题若干个连接的分区分配给消费者
     * Robin：把主题的所有分区逐个分配给消费者
     */
    private String partitionAssignmentStrategy;


    //-------------- 生产者参数 --------------//
    /**
     * 应答级别
     */
    private String ack;

    /**
     * 缓存等待发送的消息大小
     */
    private Integer bufferMemory;

    /**
     * 批量打包消息发送的大小
     */
    private Integer batchSize;

    /**
     * 重试次数
     */
    private Integer retries;

    /**
     * 延迟发送时间 ms
     */
    private Integer lingerMs;

    /**
     * 最大请求大小 字节
     */
    private Integer maxRequestSize;

    /**
     * 最大阻塞时间  send/partitionsFor 方法
     */
    private Integer maxBlockSize;

    /**
     * 生产者配置信息
     */
    private Properties producerConfig;

    /**
     * 消费者配置信息
     */
    private Properties consumerConfig;
    @Override
    public String getServerAddr() {
        return null;
    }

    @Override
    public String getDbName() {
        return null;
    }
}
