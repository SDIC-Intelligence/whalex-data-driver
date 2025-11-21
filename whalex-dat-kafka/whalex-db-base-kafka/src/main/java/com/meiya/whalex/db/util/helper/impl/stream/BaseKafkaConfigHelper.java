package com.meiya.whalex.db.util.helper.impl.stream;

import cn.hutool.core.codec.Base64;
import cn.hutool.core.collection.CollectionUtil;
import com.meiya.whalex.annotation.DbHelper;
import com.meiya.whalex.business.entity.CertificateConf;
import com.meiya.whalex.business.entity.DatabaseConf;
import com.meiya.whalex.business.entity.TableConf;
import com.meiya.whalex.business.service.DatabaseConfService;
import com.meiya.whalex.db.entity.AbstractCursorCache;
import com.meiya.whalex.db.entity.stream.KafkaClient;
import com.meiya.whalex.db.entity.stream.KafkaDatabaseInfo;
import com.meiya.whalex.db.entity.stream.KafkaTableInfo;
import com.meiya.whalex.db.kerberos.*;
import com.meiya.whalex.db.kerberos.exception.GetKerberosException;
import com.meiya.whalex.db.util.helper.AbstractDbModuleConfigHelper;
import com.meiya.whalex.db.util.param.impl.stream.KafkaConstantsProperty;
import com.meiya.whalex.exception.BusinessException;
import com.meiya.whalex.exception.ExceptionCode;
import com.meiya.whalex.util.JsonUtil;
import com.meiya.whalex.util.encrypt.AESUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * kafka 组件配置信息服务工具
 */
@Slf4j
public class BaseKafkaConfigHelper<S extends KafkaClient
        , D extends KafkaDatabaseInfo
        , T extends KafkaTableInfo
        , C extends AbstractCursorCache> extends AbstractDbModuleConfigHelper<S, D, T, C> {

    /**
     * 普通认证(账号密码认证或无认证)
     */
    public static final String AUTH_NORMAL = "normal";
    /**
     * kerboes认证证书认证
     */
    public static final String AUTH_CER = "cer";

    @Override
    public boolean checkDataSourceStatus(S connect) {
        return true;
    }

    @Override
    public D initDbModuleConfig(DatabaseConf conf) {
        String connSetting = conf.getConnSetting();
        if (StringUtils.isBlank(connSetting)) {
            log.error("kafka 数据库配置加载失败, 无有效内容! bigResourceId: [{}]", conf.getBigdataResourceId());
            throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "数据库配置必填参数缺失，请校验参数配置是否正确!");
        }
        if (log.isDebugEnabled()) {
            log.debug("kafka 数据库连接配置信息：{}", connSetting);
        }
        KafkaDatabaseInfo kafkaDatabaseInfo = JsonUtil.jsonStrToObject(connSetting, KafkaDatabaseInfo.class);
        if (StringUtils.isBlank(kafkaDatabaseInfo.getServiceUrl())) {
            log.error("kafka 数据库配置加载失败, 服务地址为空! bigResourceId: [{}]", conf.getBigdataResourceId());
            throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "数据库配置必填参数缺失，请校验参数配置是否正确!");
        }

        if (kafkaDatabaseInfo.getKafkaBulkConsumeTimeout() <= 0) {
            kafkaDatabaseInfo.setKafkaBulkConsumeTimeout(this.threadConfig.getTimeOut() * 1000);
        }

        String password = kafkaDatabaseInfo.getPassword();
        // AES 解密
        if (StringUtils.isNotBlank(password)) {
            try {
                kafkaDatabaseInfo.setPassword(AESUtil.decrypt(password));
            } catch (Exception e) {
//                log.error("kafka init db config password [{}] decrypt fail!!!", password, e);
            }
        }

        //设置认证类型
        if (StringUtils.isBlank(kafkaDatabaseInfo.getAuthType())) {
            if (StringUtils.isNotBlank(kafkaDatabaseInfo.getCertificateId()) ||
                    StringUtils.isNoneBlank(kafkaDatabaseInfo.getKrb5Path(), kafkaDatabaseInfo.getUserKeytabPath())
                    || StringUtils.isNotBlank(kafkaDatabaseInfo.getCertificateBase64Str())) {
                kafkaDatabaseInfo.setAuthType(AUTH_CER);
            } else {
                kafkaDatabaseInfo.setAuthType(AUTH_NORMAL);
            }
        }
        return (D) kafkaDatabaseInfo;
    }

    @Override
    public T initTableConfig(TableConf conf) {
        KafkaTableInfo kafkaTableInfo = new KafkaTableInfo();
        if (StringUtils.isNotBlank(conf.getTableName())) {
            kafkaTableInfo.setTableName(conf.getTableName());
        }
        String tableJson = conf.getTableJson();
        //设置分区和副本数量
        if (StringUtils.isNotBlank(tableJson)) {
            Map<String, Object> tableJsonMap = JsonUtil.jsonStrToMap(tableJson);
            String numPartitions = tableJsonMap.get("numPartitions") == null ? null : String.valueOf(tableJsonMap.get("numPartitions"));
            String replicationFactor = tableJsonMap.get("replicationFactor") == null ? null : String.valueOf(tableJsonMap.get("replicationFactor"));
            String retentionTime = tableJsonMap.get("retentionTime") == null ? null : String.valueOf(tableJsonMap.get("retentionTime"));
            List<Integer> currentPartitions = tableJsonMap.get("partitions") == null ? null : (List<Integer>) tableJsonMap.get("partitions");
            String multipleConnect = tableJsonMap.get("multipleConnect") == null ? null : String.valueOf(tableJsonMap.get("multipleConnect"));
            if (StringUtils.isNotBlank(numPartitions)) {
                kafkaTableInfo.setNumPartitions(Integer.valueOf(numPartitions));
            }
            if (StringUtils.isNotBlank(replicationFactor)) {
                kafkaTableInfo.setReplicationFactor(Integer.valueOf(replicationFactor));
            }
            if (StringUtils.isNotBlank(retentionTime)) {
                kafkaTableInfo.setRetentionTime(Integer.valueOf(retentionTime));
            }
            if (CollectionUtil.isNotEmpty(currentPartitions)) {
                kafkaTableInfo.setPartitions(currentPartitions);
            }
            if (multipleConnect != null) {
                kafkaTableInfo.setMultipleConnect(multipleConnect);
            }


            String compressionType = tableJsonMap.get("compressionType") == null ? null : String.valueOf(tableJsonMap.get("compressionType"));
            kafkaTableInfo.setCompressionType(compressionType);
            String deleteRetentionMs = tableJsonMap.get("deleteRetentionMs") == null ? null : String.valueOf(tableJsonMap.get("deleteRetentionMs"));
            kafkaTableInfo.setDeleteRetentionMs(deleteRetentionMs);
            String fileDeleteDelayMs = tableJsonMap.get("fileDeleteDelayMs") == null ? null : String.valueOf(tableJsonMap.get("fileDeleteDelayMs"));
            kafkaTableInfo.setFileDeleteDelayMs(fileDeleteDelayMs);
            String flushMessages = tableJsonMap.get("flushMessages") == null ? null : String.valueOf(tableJsonMap.get("flushMessages"));
            kafkaTableInfo.setFlushMessages(flushMessages);
            String flushMs = tableJsonMap.get("flushMs") == null ? null : String.valueOf(tableJsonMap.get("flushMs"));
            kafkaTableInfo.setFlushMs(flushMs);
            String followerReplicationThrottled = tableJsonMap.get("followerReplicationThrottled") == null ? null : String.valueOf(tableJsonMap.get("followerReplicationThrottled"));
            kafkaTableInfo.setFollowerReplicationThrottled(followerReplicationThrottled);
            String indexIntervalBytes = tableJsonMap.get("indexIntervalBytes") == null ? null : String.valueOf(tableJsonMap.get("indexIntervalBytes"));
            kafkaTableInfo.setIndexIntervalBytes(indexIntervalBytes);
            String leaderReplicationThrottledReplicas = tableJsonMap.get("leaderReplicationThrottledReplicas") == null ? null : String.valueOf(tableJsonMap.get("leaderReplicationThrottledReplicas"));
            kafkaTableInfo.setLeaderReplicationThrottledReplicas(leaderReplicationThrottledReplicas);
            String maxMessageBytes = tableJsonMap.get("maxMessageBytes") == null ? null : String.valueOf(tableJsonMap.get("maxMessageBytes"));
            kafkaTableInfo.setMaxMessageBytes(maxMessageBytes);
            String messageDownconversionEnable = tableJsonMap.get("messageDownconversionEnable") == null ? null : String.valueOf(tableJsonMap.get("messageDownconversionEnable"));
            kafkaTableInfo.setMessageDownconversionEnable(messageDownconversionEnable);
            String messageFormatVersion = tableJsonMap.get("messageFormatVersion") == null ? null : String.valueOf(tableJsonMap.get("messageFormatVersion"));
            kafkaTableInfo.setMessageFormatVersion(messageFormatVersion);
            String messageTimestampDifferenceMaxMs = tableJsonMap.get("messageTimestampDifferenceMaxMs") == null ? null : String.valueOf(tableJsonMap.get("messageTimestampDifferenceMaxMs"));
            kafkaTableInfo.setMessageTimestampDifferenceMaxMs(messageTimestampDifferenceMaxMs);
            String messageTimestampType = tableJsonMap.get("messageTimestampType") == null ? null : String.valueOf(tableJsonMap.get("messageTimestampType"));
            kafkaTableInfo.setMessageTimestampType(messageTimestampType);
            String minCleanableDirtyRatio = tableJsonMap.get("minCleanableDirtyRatio") == null ? null : String.valueOf(tableJsonMap.get("minCleanableDirtyRatio"));
            kafkaTableInfo.setMinCleanableDirtyRatio(minCleanableDirtyRatio);
            String minCompactionLagMs = tableJsonMap.get("minCompactionLagMs") == null ? null : String.valueOf(tableJsonMap.get("minCompactionLagMs"));
            kafkaTableInfo.setMinCompactionLagMs(minCompactionLagMs);
            String minInsyncReplicas = tableJsonMap.get("minInsyncReplicas") == null ? null : String.valueOf(tableJsonMap.get("minInsyncReplicas"));
            kafkaTableInfo.setMinInsyncReplicas(minInsyncReplicas);
            String preallocate = tableJsonMap.get("preallocate") == null ? null : String.valueOf(tableJsonMap.get("preallocate"));
            kafkaTableInfo.setPreallocate(preallocate);
            String retentionBytes = tableJsonMap.get("retentionBytes") == null ? null : String.valueOf(tableJsonMap.get("retentionBytes"));
            kafkaTableInfo.setRetentionBytes(retentionBytes);
            String retentionMs = tableJsonMap.get("retentionMs") == null ? null : String.valueOf(tableJsonMap.get("retentionMs"));
            kafkaTableInfo.setRetentionMs(retentionMs);
            String segmentBytes = tableJsonMap.get("segmentBytes") == null ? null : String.valueOf(tableJsonMap.get("segmentBytes"));
            kafkaTableInfo.setSegmentBytes(segmentBytes);
            String segmentIndexBytes = tableJsonMap.get("segmentIndexBytes") == null ? null : String.valueOf(tableJsonMap.get("segmentIndexBytes"));
            kafkaTableInfo.setSegmentIndexBytes(segmentIndexBytes);
            String segmentJitterMs = tableJsonMap.get("segmentJitterMs") == null ? null : String.valueOf(tableJsonMap.get("segmentJitterMs"));
            kafkaTableInfo.setSegmentJitterMs(segmentJitterMs);
            String segmentMs = tableJsonMap.get("segmentMs") == null ? null : String.valueOf(tableJsonMap.get("segmentMs"));
            kafkaTableInfo.setSegmentMs(segmentMs);
            String uncleanLeaderElectionEnable = tableJsonMap.get("uncleanLeaderElectionEnable") == null ? null : String.valueOf(tableJsonMap.get("uncleanLeaderElectionEnable"));
            kafkaTableInfo.setUncleanLeaderElectionEnable(uncleanLeaderElectionEnable);

        }
        return (T) kafkaTableInfo;
    }

    @Override
    public S initDbConnect(D databaseConf, T tableConf) {
        log.info("kafka认证类型:{}", databaseConf.getAuthType());
        if (StringUtils.equals(databaseConf.getAuthType(), AUTH_CER)) {
            String certificateId = databaseConf.getCertificateId();
            KerberosUniformAuth kerberosUniformLogin;
            try {
                DbHelper annotation = this.getClass().getAnnotation(DbHelper.class);
                DatabaseConfService databaseConfService = getDatabaseConfService();

                //certificateBase64Str存在
                if(StringUtils.isNotBlank(databaseConf.getCertificateBase64Str())) {
                    byte[] content = Base64.decode(databaseConf.getCertificateBase64Str());
                    kerberosUniformLogin = KerberosUniformAuthFactory.getKerberosUniformLoginByZip(annotation.dbType(), annotation.version(), annotation.cloudVendors()
                            , content, ".zip", databaseConf.getUserName(), null, databaseConf.getParentDir(), null);
                } else if (databaseConfService == null) {
                    // 本地获取
                    kerberosUniformLogin = KerberosUniformAuthFactory.getKerberosUniformLoginByLocal(annotation.dbType(), annotation.version(), annotation.cloudVendors()
                            , databaseConf.getUserName(), databaseConf.getKerberosPrincipal(), databaseConf.getKrb5Path(), databaseConf.getUserKeytabPath()
                            , databaseConf.getParentDir()
                            , null);
                } else {
                    // 从数据库获取
                    CertificateConf certificateConf = databaseConfService.queryCertificateConfById(certificateId);
                    if (certificateConf == null) {
                        throw new BusinessException(ExceptionCode.NOT_FOUND_CERTIFICATE, certificateId);
                    }
                    kerberosUniformLogin = KerberosUniformAuthFactory.getKerberosUniformLoginByZip(annotation.dbType(), annotation.version(), annotation.cloudVendors()
                            , certificateConf.getContent(), certificateConf.getFileName(), certificateConf.getUserName(), databaseConf.getKerberosPrincipal(), databaseConf.getParentDir()
                            , null);
                }
            } catch (GetKerberosException e1) {
                throw new BusinessException(e1, ExceptionCode.GET_CERTIFICATE_EXCEPTION, certificateId);
            }

            if (kerberosUniformLogin != null) {
                try {
                    kerberosUniformLogin.login(KerberosJaasConfigurationUtil.Module.KAFKA, new KerberosLoginCallBack() {
                        @Override
                        public void loginExtend(KerberosUniformAuth kerberosUniformLogin) throws Exception {
                            KerberosEnvConfigurationUtil.setZookeeperServerPrincipal(databaseConf.getZooKeeperNamespace());
                        }
                    });
                    databaseConf.setKerberosPrincipal(kerberosUniformLogin.getPrincipalOrKrb5());
                } catch (Exception e) {
                    throw new BusinessException(e, ExceptionCode.CERTIFICATE_LOGIN_EXCEPTION, certificateId);
                }
            }
        }
        // 通过懒加载的方式创建连接
        return (S) new KafkaClient(databaseConf, this);
    }

    /**
     * kafka 认证
     *
     * @param databaseConf
     * @param props
     */
    public void authKafka(KafkaDatabaseInfo databaseConf, Properties props) {
        synchronized (this) {
            log.info("kafka认证类型:{}", databaseConf.getAuthType());
            if (StringUtils.equals(databaseConf.getAuthType(), AUTH_NORMAL) &&
                    StringUtils.isNoneBlank(databaseConf.getUserName(), databaseConf.getPassword())) {
                //账号密码认证
                props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
                props.put("sasl.mechanism", "PLAIN");
                props.put("sasl.jaas.config", String.format(
                        "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
                        databaseConf.getUserName(),
                        databaseConf.getPassword()));
            } else if (StringUtils.equals(databaseConf.getAuthType(), AUTH_CER)) {
                // 协议类型
                props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
                // 服务名
                props.put("sasl.kerberos.service.name", databaseConf.getKerberosServiceName());
                // 域名
                props.put("kerberos.domain.name", databaseConf.getKerberosPrincipal());
            }
        }
    }

    /**
     * 创建消费者
     *
     * @return
     */
    public KafkaProducer createProducer(KafkaDatabaseInfo databaseConf) {
        Properties producerConfig = databaseConf.getProducerConfig();
        // 生产者创建
        Properties props = new Properties();
        // 服务列表
        props.put("bootstrap.servers", databaseConf.getServiceUrl());
        // 应答级别
        if (StringUtils.isNotBlank(databaseConf.getAck())) {
            props.put("acks", databaseConf.getAck());
        }
        if (databaseConf.getBatchSize() != null) {
            props.put("batch.size", databaseConf.getBatchSize());
        }
        if (databaseConf.getRetries() != null) {
            props.put("retries", databaseConf.getRetries());
        }
        if (databaseConf.getBufferMemory() != null) {
            props.put("buffer.memory", databaseConf.getBufferMemory());
        }
        if (databaseConf.getLingerMs() != null) {
            props.put("linger.ms", databaseConf.getLingerMs());
        }
        if (databaseConf.getMaxBlockSize() != null) {
            props.put("max.block.ms", databaseConf.getMaxBlockSize());
        }
        if (databaseConf.getMaxRequestSize() != null) {
            props.put("max.request.size", databaseConf.getMaxRequestSize());
        }
        props.put("key.serializer", KafkaConstantsProperty.SERIALIZER_CLASS);
        props.put("value.serializer", KafkaConstantsProperty.SERIALIZER_CLASS);
        if(producerConfig != null && producerConfig.size() > 0 ){
            producerConfig.forEach((k,v) -> {
                props.put(k,v);
            });
        }
        authKafka(databaseConf, props);
        return new KafkaProducer<>(props);
    }

    /**
     * 创建消费者
     *
     * @param databaseConf
     * @return
     */
    public KafkaConsumer createConsumer(KafkaDatabaseInfo databaseConf) {
        Properties consumerConfig = databaseConf.getConsumerConfig();
        Properties consumerProps = new Properties();
        // zookeeper 配置，通过zk 可以负载均衡的获取broker
        consumerProps.put("bootstrap.servers", databaseConf.getServiceUrl());
        // group 代表一个消费组
        if (StringUtils.isNotBlank(databaseConf.getGroup())) {
            consumerProps.put("group.id", databaseConf.getGroup());
        } else {
            log.warn("当前 kafka countMethod 未设置groupId，采用默认的 res_default 作为分组!");
            consumerProps.put("group.id", "res_default");
        }
        // 如果要读取旧的数据，必须要加earliest,earliest配置会让消费者从头开始消费
        consumerProps.put("auto.offset.reset", databaseConf.getAutoOffsetReset());
        // 默认设置 一次拉取消息最大的数量为10条
        consumerProps.put("max.poll.records", databaseConf.getMaxPollRecord());

        // 最大拉取间隔时间
        if (databaseConf.getMaxPollIntervalMs() != null) {
            consumerProps.put("max.poll.interval.ms", databaseConf.getMaxPollIntervalMs());
        }

        // zk连接超时时间
        consumerProps.put("zookeeper.session.timeout.ms", KafkaConstantsProperty.sessionTimeOut);
        // 关闭位移自动提交
        consumerProps.put("enable.auto.commit", databaseConf.getEnableAutocommit());
        // 自动提交间隔时间
        if (databaseConf.getEnableAutocommit() && databaseConf.getAutocommitInterval() != null) {
            consumerProps.put("auto.commit.interval.ms", databaseConf.getAutocommitInterval());
        }
        // 设置获取记录的最小字节
        if (databaseConf.getFetchMinBytes() != null) {
            consumerProps.put("fetch.min.bytes", databaseConf.getFetchMinBytes());
        }
        // 获取记录最大等待时间
        if (databaseConf.getFetchMaxWait() != null) {
            consumerProps.put("fetch.max.wait.ms", databaseConf.getFetchMaxWait());
        }
        // 从分区获取记录的最大字节
        if (databaseConf.getMaxPartitionFetchBytes() != null) {
            consumerProps.put("max.partition.fetch.bytes", databaseConf.getMaxPartitionFetchBytes());
        }
        // 心跳超时时间
        if (databaseConf.getSessionTimeOut() != null) {
            consumerProps.put("session.timeout.ms", databaseConf.getSessionTimeOut());
        }
        // 请求超时时间
        if (databaseConf.getRequestTimeout() != null) {
            consumerProps.put("request.timeout.ms", databaseConf.getRequestTimeout());
        }else {
            consumerProps.put("request.timeout.ms", this.threadConfig.getTimeOut() * 1000 + 5);
        }
        // 分区分配策略
        if (StringUtils.isNotBlank(databaseConf.getPartitionAssignmentStrategy())) {
            consumerProps.put("partition.assignment.strategy", databaseConf.getPartitionAssignmentStrategy());
        }
        consumerProps.put("key.deserializer", KafkaConstantsProperty.SERIALIZER_DESER_CLASS);
        consumerProps.put("value.deserializer", KafkaConstantsProperty.SERIALIZER_DESER_CLASS);
        if(consumerConfig != null && consumerConfig.size() > 0){
            consumerConfig.forEach((k,v) -> {
                consumerProps.put(k,v);
            });
        }
        authKafka(databaseConf, consumerProps);
        return new KafkaConsumer<>(consumerProps);
    }

    /**
     * 创建管理对象
     *
     * @param databaseConf
     * @return
     */
    public AdminClient createAdmin(KafkaDatabaseInfo databaseConf) {
        Properties adminProps = new Properties();
        adminProps.put("bootstrap.servers", databaseConf.getServiceUrl());
        authKafka(databaseConf, adminProps);
        return KafkaAdminClient.create(adminProps);
    }

    @Override
    public void destroyDbConnect(String cacheKey, S connect) {
        connect.close();
    }

    /**
     * 拼上表名，避免一个客户端消费多个不同的topic
     *
     * @param dbDatabaseConf
     * @param tableInfo
     * @return
     */
    @Override
    public String getCacheKey(D dbDatabaseConf, T tableInfo) {
        String cacheKey = super.getCacheKey(dbDatabaseConf, tableInfo);
        StringBuilder sb = new StringBuilder(cacheKey);
        if (tableInfo != null && StringUtils.isNotBlank(tableInfo.getTableName())) {
            sb.append("#").append(tableInfo.getTableName());
            if (CollectionUtil.isNotEmpty(tableInfo.getPartitions())) {
                List<Integer> partitions = tableInfo.getPartitions();
                for (Integer partition : partitions) {
                    sb.append("#").append(partition);
                }
            }
            String multipleConnect = tableInfo.getMultipleConnect();
            if (multipleConnect != null) {
                sb.append("#").append(multipleConnect);
            }
        }
        return sb.toString();
    }
}
