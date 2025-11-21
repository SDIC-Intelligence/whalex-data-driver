package com.meiya.whalex.db.module.stream;

import cn.hutool.core.map.MapUtil;
import cn.hutool.json.JSONUtil;
import com.google.common.collect.Lists;
import com.meiya.whalex.annotation.Support;
import com.meiya.whalex.db.constant.SupportPower;
import com.meiya.whalex.db.constant.TransactionConstant;
import com.meiya.whalex.db.entity.AbstractCursorCache;
import com.meiya.whalex.db.entity.QueryCursorMethodResult;
import com.meiya.whalex.db.entity.QueryMethodResult;
import com.meiya.whalex.db.entity.table.infomaration.KafkaTableInformation;
import com.meiya.whalex.db.entity.stream.KafkaClient;
import com.meiya.whalex.db.entity.stream.KafkaDatabaseInfo;
import com.meiya.whalex.db.entity.stream.KafkaHandler;
import com.meiya.whalex.db.entity.stream.KafkaTableInfo;
import com.meiya.whalex.db.kafka.admin.client.*;
import com.meiya.whalex.db.module.AbstractDbModuleBaseService;
import com.meiya.whalex.db.util.helper.impl.stream.BaseKafkaConfigHelper;
import com.meiya.whalex.exception.BusinessException;
import com.meiya.whalex.exception.ExceptionCode;
import com.meiya.whalex.util.JsonUtil;
import com.meiya.whalex.util.WildcardRegularConversion;
import com.meiya.whalex.util.collection.CollectionUtil;
import com.meiya.whalex.util.concurrent.ThreadLocalUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.consumer.internals.NoOpConsumerRebalanceListener;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.*;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * kafka 服务实现类
 * @author huanghs
 */
@Slf4j
@Support(value = {
        SupportPower.TRANSACTION,
        SupportPower.TEST_CONNECTION,
        SupportPower.CREATE,
        SupportPower.SEARCH,
        SupportPower.SHOW_SCHEMA,
        SupportPower.CREATE_TABLE,
        SupportPower.SHOW_TABLE_LIST
})
public class BaseKafkaServiceImpl<S extends KafkaClient,
        Q extends KafkaHandler,
        D extends KafkaDatabaseInfo,
        T extends KafkaTableInfo,
        C extends AbstractCursorCache> extends AbstractDbModuleBaseService<S, Q, D, T, C> {

    /**
     * 消费kafka消息
     * <p>
     * 查询条件中offset目前没有意义
     * 消费会根据limit与<code>KafkaDatabaseInfo中的kafkaBulkConsumeTimeout</code>(消费最大时长)结合消费
     * limit限制批量消费的记录数,只要大于或等于即返回
     * kafkaBulkConsumeTimeout限制消费的最大时长,只要大于该时长,不管是否达到limit数据量都返回
     * </p>
     *
     * @param kafkaClient
     * @param queryEntity
     * @param databaseConf
     * @param tableConf
     * @return
     * @throws Exception
     */
    @Override
    protected QueryMethodResult queryMethod(S kafkaClient, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        //事务处理，将客户端放入事务管理器中
        KafkaTransactionManager kafkaTransactionManager = transactionHandler(databaseConf, kafkaClient);
        KafkaConsumer<String, Object> consumer = kafkaClient.getConsumer();
        List<Integer> assignPartitions = tableConf.getPartitions();
        // kafka生产者再多线程下为线程安全，消费者为非线程安全
        synchronized (consumer) {
            //订阅查询
            initTopic(tableConf, assignPartitions, consumer);
            List<Map<String, Object>> resultList = new LinkedList<>();
            ConsumerRecords<String, Object> records = consumer.poll(databaseConf.getKafkaBulkConsumeTimeout());
            //自动提交
            if(kafkaTransactionManager == null) {
                for (ConsumerRecord<String, Object> record : records) {
                    long offset = record.offset();
                    int partition = record.partition();
                    long timestamp = record.timestamp();
                    String key = record.key();
                    Object value = record.value();
                    Map<String, Object> rowMap = new HashMap<>();
                    rowMap.put("record", value);
                    rowMap.put("offset", offset);
                    rowMap.put("partition", partition);
                    rowMap.put("timestamp", timestamp);
                    rowMap.put("key", key);
                    rowMap.put("value", value);
                    resultList.add(rowMap);
                }
                Boolean enableAutocommit = databaseConf.getEnableAutocommit();
                if (enableAutocommit == null || !enableAutocommit) {
                    // 没有设置自动提交时，在手动提交
                    consumer.commitAsync();
                }
                return new QueryMethodResult(resultList.size(), resultList);
            }

            //开启事务需要记录回滚的位置
            //获取分区
            Set<TopicPartition> partitions = records.partitions();
            Map<TopicPartition, Long> topicPartitionToOffset = new HashMap<>();
            Map<TopicPartition, Long> topicPartitionToOffsetCommit = new HashMap<>();
            for (TopicPartition topicPartition : partitions) {
                List<ConsumerRecord<String, Object>> consumerRecords = records.records(topicPartition);
                long offset = consumerRecords.get(0).offset();
                long offsetCommit = consumerRecords.get(0).offset();
                for (ConsumerRecord<String, Object> record : consumerRecords) {
                    int partition = record.partition();
                    long timestamp = record.timestamp();
                    String key = record.key();
                    Object value = record.value();
                    Map<String, Object> rowMap = new HashMap<>();
                    rowMap.put("record", value);
                    rowMap.put("offset", record.offset());
                    rowMap.put("partition", partition);
                    rowMap.put("timestamp", timestamp);
                    rowMap.put("key", key);
                    rowMap.put("value", value);
                    resultList.add(rowMap);
                    //回滚的位置
                    offset = offset > record.offset() ? record.offset() : offset;
                    //提交的位置
                    offsetCommit = offsetCommit < record.offset() ? record.offset() : offsetCommit;
                }

                topicPartitionToOffset.put(topicPartition, offset);
                topicPartitionToOffsetCommit.put(topicPartition, offsetCommit);
            }
            //保存回滚点
            kafkaTransactionManager.saveEndPoint(topicPartitionToOffset);
            //保存提交点
            kafkaTransactionManager.saveEndPointCommit(topicPartitionToOffsetCommit);

            return new QueryMethodResult(resultList.size(), resultList);
        }
    }

    /**
     * 获取连接
     *
     * @param connect
     * @return Connection 连接对象
     * @throws Exception
     */
    protected KafkaTransactionManager transactionHandler(D databaseConf, S connect) throws Exception {
        // 1.判断 ThreadLocal 中是否存在 TransactionId
        String databaseKey = this.helper.getCacheKey(databaseConf, null);
        String transactionId = (String) ThreadLocalUtil.get(databaseKey.hashCode() + "_" + TransactionConstant.ID);
        KafkaTransactionManager transactionManager =null;
        if (StringUtils.isNotBlank(transactionId)) {
            //2.判断时候第一次开启事务
            String first = (String) ThreadLocalUtil.get(databaseKey.hashCode() + "_" + TransactionConstant.FIRST);
            if (StringUtils.isBlank(first)) {
                // 3.不是第一次则判断 this.getHelper().getTransactionManager(TransactionId) 是否获取得到TransactionManager
                transactionManager = (KafkaTransactionManager) this.getHelper().getTransactionManager(transactionId);
                if (Objects.isNull(transactionManager)) {
                    throw new BusinessException(ExceptionCode.TRANSACTION_NULL_EXCEPTION);
                }
                // 4.若 从第三步中获取到 TransactionManager，则调用 TransactionManager.getTransactionClient 获取 connection
                return transactionManager;
            } else {
                // 5.是第一次则第一次创建事务对象 将 connection.setAutoCommit(false) 并将 connection 作为事务管理对象放到 TransactionManager 中
                transactionManager = (KafkaTransactionManager) this.getHelper().getTransactionManager(transactionId);
                if (!Objects.isNull(transactionManager)) {
                    throw new BusinessException(ExceptionCode.TRANSACTION_ID_EXIST_EXCEPTION);
                }
                transactionManager = new KafkaTransactionManager(connect);
                this.getHelper().createTransactionManager(transactionId, transactionManager);
                //6.删除第一次开始事务标志位
                ThreadLocalUtil.set(databaseKey.hashCode() + "_" + TransactionConstant.FIRST, null);
            }

        }

        if(transactionManager == null) {
            return transactionHandler(connect);
        }

        return transactionManager;
    }

    protected KafkaTransactionManager transactionHandler(S connect) throws Exception {
        // 1.判断 ThreadLocal 中是否存在 TransactionId
        String transactionId = (String) ThreadLocalUtil.get(TransactionConstant.ID);
        KafkaTransactionManager transactionManager =null;
        if (StringUtils.isNotBlank(transactionId)) {
            //2.判断时候第一次开启事务
            String first = (String) ThreadLocalUtil.get(TransactionConstant.FIRST);
            if (StringUtils.isBlank(first)) {
                // 3.不是第一次则判断 this.getHelper().getTransactionManager(TransactionId) 是否获取得到TransactionManager
                transactionManager = (KafkaTransactionManager) this.getHelper().getTransactionManager(transactionId);
                if (Objects.isNull(transactionManager)) {
                    throw new BusinessException(ExceptionCode.TRANSACTION_NULL_EXCEPTION);
                }
                // 4.若 从第三步中获取到 TransactionManager，则调用 TransactionManager.getTransactionClient 获取 connection
                return transactionManager;
            } else {
                // 5.是第一次则第一次创建事务对象 将 connection.setAutoCommit(false) 并将 connection 作为事务管理对象放到 TransactionManager 中
                transactionManager = (KafkaTransactionManager) this.getHelper().getTransactionManager(transactionId);
                if (!Objects.isNull(transactionManager)) {
                    throw new BusinessException(ExceptionCode.TRANSACTION_ID_EXIST_EXCEPTION);
                }
                transactionManager = new KafkaTransactionManager(connect);
                this.getHelper().createTransactionManager(transactionId, transactionManager);
                //6.删除第一次开始事务标志位
                ThreadLocalUtil.set(TransactionConstant.FIRST, null);
            }

        }
        return transactionManager;
    }

    @Override
    protected QueryMethodResult countMethod(S kafkaClient, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        List<Integer> assignPartitions = tableConf.getPartitions();
        KafkaConsumer consumer;
        // 通过懒加载方式获取消费者
        boolean closeFlag = true;
        // 位移时间戳
        Long beginOffsetTimestamp = null;
        Long endOffsetTimestamp = null;
        if(queryEntity != null && queryEntity.getKafkaResourceQuery() != null) {
            KafkaHandler.KafkaResourceQuery kafkaResourceQuery = queryEntity.getKafkaResourceQuery();
            beginOffsetTimestamp = kafkaResourceQuery.getBeginOffsetTimestamp();
            endOffsetTimestamp = kafkaResourceQuery.getEndOffsetTimestamp();
        }
        Map<String, Object> result = new HashMap<>(3);
        if (kafkaClient.isExistConsumer()) {
            consumer = kafkaClient.getConsumer();
            closeFlag = false;
        } else {
            consumer = kafkaClient.createConsumer();
        }
        try {
            synchronized (consumer) {
                int total = 0;
                // 获取分区
                List<PartitionInfo> partitionInfos = consumer.partitionsFor(tableConf.getTableName());
                if (cn.hutool.core.collection.CollectionUtil.isNotEmpty(partitionInfos)) {
                    List<TopicPartition> tps = partitionInfos.stream().filter(
                            partitionInfo -> {
                                if (cn.hutool.core.collection.CollectionUtil.isNotEmpty(assignPartitions)) {
                                    int partition = partitionInfo.partition();
                                    return assignPartitions.contains(partition);
                                } else {
                                    return true;
                                }
                            }
                    ).map(partitionInfo ->
                            new TopicPartition(partitionInfo.topic(), partitionInfo.partition())
                    ).collect(Collectors.toList());
                    // 开始位移
                    Map<TopicPartition, Long> beginningOffsets;
                    if (beginOffsetTimestamp == null) {
                        beginningOffsets = consumer.beginningOffsets(tps);
                    } else {
                        Map<TopicPartition, Long> offsetsForTimesMap = new HashMap<>(tps.size());
                        for (TopicPartition topicPartition : tps) {
                            offsetsForTimesMap.put(topicPartition, beginOffsetTimestamp);
                        }
                        Map<TopicPartition, OffsetAndTimestamp> offsetAndTimestampMap = consumer.offsetsForTimes(offsetsForTimesMap);
                        beginningOffsets = new HashMap<>(offsetAndTimestampMap.size());
                        for (Map.Entry<TopicPartition, OffsetAndTimestamp> topicPartitionOffsetAndTimestampEntry : offsetAndTimestampMap.entrySet()) {
                            TopicPartition topicPartition = topicPartitionOffsetAndTimestampEntry.getKey();
                            OffsetAndTimestamp offsetAndTimestamp = topicPartitionOffsetAndTimestampEntry.getValue();
                            long offset = 0;
                            if (offsetAndTimestamp != null) {
                                offset = offsetAndTimestamp.offset();
                            } else {
                                // 如果当前起始位移的时间戳没有消息了，那么就获取这个分区最后的位移
                                Map<TopicPartition, Long> endedOffsets = consumer.endOffsets(cn.hutool.core.collection.CollectionUtil.newArrayList(topicPartition));
                                Long endOffset = endedOffsets.get(topicPartition);
                                if (endOffset != null) {
                                    offset = endOffset;
                                }
                            }
                            beginningOffsets.put(topicPartition, offset);
                        }
                    }
                    Map<Integer, Long> beginningOffsetMap = new HashMap<>(beginningOffsets.size());
                    beginningOffsets.forEach((topicPartition, beginOffset) -> {
                        beginningOffsetMap.put(topicPartition.partition(), beginOffset);
                    });

                    // 结束位移
                    Map<TopicPartition, Long> endOffsets;
                    if (endOffsetTimestamp == null) {
                        endOffsets = consumer.endOffsets(tps);
                    } else {
                        Map<TopicPartition, Long> offsetsForTimesMap = new HashMap<>(tps.size());
                        for (TopicPartition topicPartition : tps) {
                            offsetsForTimesMap.put(topicPartition, endOffsetTimestamp);
                        }
                        Map<TopicPartition, OffsetAndTimestamp> offsetAndTimestampMap = consumer.offsetsForTimes(offsetsForTimesMap);
                        endOffsets = new HashMap<>(offsetAndTimestampMap.size());
                        for (Map.Entry<TopicPartition, OffsetAndTimestamp> topicPartitionOffsetAndTimestampEntry : offsetAndTimestampMap.entrySet()) {
                            TopicPartition topicPartition = topicPartitionOffsetAndTimestampEntry.getKey();
                            OffsetAndTimestamp offsetAndTimestamp = topicPartitionOffsetAndTimestampEntry.getValue();
                            long offset = 0;
                            if (offsetAndTimestamp != null) {
                                offset = offsetAndTimestamp.offset();
                            } else {
                                Map<TopicPartition, Long> endOffsetMap = consumer.endOffsets(cn.hutool.core.collection.CollectionUtil.newArrayList(topicPartition));
                                Long endOffset = endOffsetMap.get(topicPartition);
                                if (endOffset != null) {
                                    offset = endOffset;
                                }
                            }
                            endOffsets.put(topicPartition, offset);
                        }
                    }
                    Map<Integer, Long> endOffsetMap = new HashMap<>(endOffsets.size());
                    endOffsets.forEach((topicPartition, endOffset) -> {
                        endOffsetMap.put(topicPartition.partition(), endOffset);
                    });

                    // 挤压量
                    Map<Integer, Long> committedOffsetMap = new HashMap<>(beginningOffsets.size());

                    // 获取当前分组下记录的topic的分区的位移
                    MyKafkaAdminClient myKafkaAdminClient = kafkaClient.getMyKafkaAdminClient_V1();
                    MyListConsumerGroupOffsetsOptions offsetsOptions = new MyListConsumerGroupOffsetsOptions();
                    offsetsOptions.topicPartitions(tps);
                    String group = databaseConf.getGroup();
                    if (StringUtils.isBlank(group)) {
                        log.warn("当前 kafka countMethod 未设置groupId，采用默认的 res_default 作为分组!");
                        group = "res_default";
                    }
                    MyListConsumerGroupOffsetsResult offsetsResult = myKafkaAdminClient.listConsumerGroupOffsets(group, offsetsOptions);
                    Map<TopicPartition, OffsetAndMetadata> topicPartitionOffsetAndMetadataMap = offsetsResult.partitionsToOffsetAndMetadata().get();
                    for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : topicPartitionOffsetAndMetadataMap.entrySet()) {
                        committedOffsetMap.put(entry.getKey().partition(), entry.getValue() == null ? 0 : entry.getValue().offset());
                    }

                    // 统计数据总量
                    for (Map.Entry<TopicPartition, Long> beginningOffset : beginningOffsets.entrySet()) {
                        TopicPartition key = beginningOffset.getKey();
                        Long beginning = beginningOffset.getValue();
                        Long end = endOffsets.get(key);
                        total += end - beginning;
                    }

                    result.put("beggingOffset", beginningOffsetMap);
                    result.put("endOffset", endOffsetMap);
                    result.put("currentOffset", committedOffsetMap);
                    return new QueryMethodResult(total, cn.hutool.core.collection.CollectionUtil.newArrayList(result));
                } else {
                    return new QueryMethodResult(0, cn.hutool.core.collection.CollectionUtil.newArrayList(result));
                }
            }
        } finally {
            if (closeFlag && consumer != null) {
                consumer.close();
            }
        }
    }

    /**
     * 设置 topic
     * @param tableConf
     * @param assignPartitions
     * @param consumer
     */
    void initTopic(T tableConf, List<Integer> assignPartitions, KafkaConsumer consumer) {
        if (cn.hutool.core.collection.CollectionUtil.isEmpty(assignPartitions)) {
            consumer.subscribe(Arrays.asList(tableConf.getTableName()));
        } else {
            List<TopicPartition> topicPartitionList = new ArrayList<>(assignPartitions.size());
            for (Integer assignPartition : assignPartitions) {
                TopicPartition partition = new TopicPartition(tableConf.getTableName(), assignPartition);
                topicPartitionList.add(partition);
            }
            consumer.assign(topicPartitionList);
        }
    }

    /**
     * 发送请求主方法
     *
     * @return
     *//*
    private ByteBuffer send(String host, int port, AbstractRequest request, ApiKeys apiKeys) throws Exception {
        Socket socket = null;
        try {
            socket = new Socket(host, port);
            return send(request, apiKeys, socket);
        } finally {
            if (socket != null) {
                socket.close();
            }
        }
    }

    *//**
     * 向给定的socket发送请求
     *
     * @param request
     * @param apiKeys
     * @param socket
     * @return
     *//*
    private ByteBuffer send(AbstractRequest request, ApiKeys apiKeys, Socket socket) throws IOException {
        RequestHeader header = new RequestHeader(apiKeys, (short) 2, "client-id", 0);
        ByteBuffer buffer = request.serialize(header);
        byte[] array = buffer.array();

        sendRequest(socket, array);
        byte[] response = getResponse(socket);
        ByteBuffer responseBuffer = ByteBuffer.wrap(response);
        ResponseHeader.parse(responseBuffer);
        return responseBuffer;
    }

    *//**
     * 发送序列化请求给socket
     *//*
    private void sendRequest(Socket socket, byte[] array) throws IOException {
        DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
        dataOutputStream.writeInt(array.length);
        dataOutputStream.write(array);
        dataOutputStream.flush();
    }*/

    /**
     * 从socket处获取response
     *
     * @return
     */
    private byte[] getResponse(Socket socket) throws IOException {
        DataInputStream dataInputStream = null;
        try {
            dataInputStream = new DataInputStream(socket.getInputStream());
            byte[] response = new byte[dataInputStream.readInt()];
            dataInputStream.readFully(response);
            return response;
        } finally {
            if (dataInputStream != null) {
                dataInputStream.close();
            }
        }
    }

    @Override
    protected QueryMethodResult testConnectMethod(S kafkaClient, D databaseConf) throws Exception {
        try {
            DescribeClusterResult describeClusterResult = kafkaClient.getAdminClient().describeCluster();
            Collection<Node> nodes = StringUtils.equals(databaseConf.getAuthType(), "cer")
                    ? describeClusterResult.nodes().get(5, TimeUnit.MINUTES) //本地虚拟机认证测试连接会比较久
                    : describeClusterResult.nodes().get(50, TimeUnit.SECONDS);
            boolean isSuccess = nodes != null && nodes.size() > 0;
            if (isSuccess && log.isDebugEnabled()) {
                log.debug(nodes.stream().map(n -> String.format("%s:%s", n.host(), n.port())).collect(Collectors.joining(",")));
            }
            if (!isSuccess) {
                throw new BusinessException("Kafka测试连接失败!");
            }
            return new QueryMethodResult();
        } catch (TimeoutException e) {
            throw new BusinessException("Connection time out: " + e.getMessage(), e);
        }
    }

    /**
     * 获取kafka集群所有topics列表
     *
     * @param kafkaClient
     * @param databaseConf
     * @return
     */
    @Override
    protected QueryMethodResult showTablesMethod(S kafkaClient, Q queryEntity, D databaseConf) throws Exception {
        ListTopicsResult listTopicsResult = kafkaClient.getAdminClient().listTopics();
        KafkaFuture<Set<String>> set = listTopicsResult.names();
        List<String> topics = Lists.newArrayList(set.get());

        List<Map<String, Object>> result = new ArrayList<>(topics.size());
        for (int i = 0; i < topics.size(); i++) {
            Map<String, Object> valueMap = new HashMap<>(1);
            valueMap.put("tableName", topics.get(i));
            result.add(valueMap);
        }
        result = WildcardRegularConversion.matchFilter(queryEntity.getKafkaListTable().getTableMatch(), result);
        return new QueryMethodResult(result.size(), result);
    }

    @Override
    protected QueryMethodResult getIndexesMethod(S connect, D databaseConf, T tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "kafkaServiceImpl.getIndexesMethod");
    }

    /**
     * 创建topic
     *
     * @param kafkaClient
     * @param queryEntity
     * @param databaseConf
     * @param tableConf
     * @return
     * @throws Exception
     */
    @Override
    protected QueryMethodResult createTableMethod(S kafkaClient, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        KafkaHandler.KafkaResourceCreate kafkaResourceCreate = queryEntity.getKafkaResourceCreate();
        NewTopic _newTopic = null;
        Integer retentionTime;
        if (tableConf.getNumPartitions() != null && tableConf.getReplicationFactor() != null) {
            _newTopic = new NewTopic(tableConf.getTableName(), tableConf.getNumPartitions(), Short.valueOf(String.valueOf(tableConf.getReplicationFactor())));
            retentionTime = tableConf.getRetentionTime();
        } else if (kafkaResourceCreate.getNumPartitions() != null && kafkaResourceCreate.getReplicationFactor() != null) {
            _newTopic = new NewTopic(tableConf.getTableName(), kafkaResourceCreate.getNumPartitions(), Short.valueOf(String.valueOf(kafkaResourceCreate.getReplicationFactor())));
            retentionTime = kafkaResourceCreate.getRetentionTime();
        } else {
            throw new BusinessException("kafka创建topic参数需要传入 numPartitions 与 replicationFactor！");
        }

        //topic配置
        Map<String, String> config = new HashMap<>();
        if (retentionTime != null) {
            config.put("delete.retention.ms", String.valueOf(retentionTime * 24 * 60 * 60 * 1000));
        }
        NewTopic newTopic = _newTopic.configs(config);
        CreateTopicsResult topics = kafkaClient.getAdminClient().createTopics(Arrays.asList(newTopic));
        KafkaFuture<Void> all = topics.all();
        all.get(60, TimeUnit.SECONDS);
        if (all.isDone()) {
            return new QueryMethodResult();
        } else {
            throw new BusinessException(ExceptionCode.CREATE_TABLE_EXCEPTION, "新建topic失败! msg: 未成功.");
        }
    }

    @Override
    protected QueryMethodResult dropTableMethod(S kafkaClient, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        try {
            String tableName = tableConf.getTableName();
            DeleteTopicsResult deleteTopicsResult = kafkaClient.getAdminClient().deleteTopics(Lists.newArrayList(tableName));
            deleteTopicsResult.all().get();
            boolean isDone = deleteTopicsResult.all().isDone();
            if (isDone) {
                return new QueryMethodResult(1, null);
            } else {
                throw new BusinessException(ExceptionCode.DROP_TABLE_EXCEPTION, tableName);
            }
        } catch (Exception e) {
            log.error("kafka drop topic fail,detail:{}", e.getMessage());
            throw new BusinessException(ExceptionCode.DROP_TABLE_EXCEPTION, tableConf.getTableName());
        }
    }

    @Override
    protected QueryMethodResult deleteIndexMethod(S kafkaClient, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "kafkaServiceImpl.deleteIndexMethod");
    }

    @Override
    protected QueryMethodResult createIndexMethod(S kafkaClient, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "kafkaServiceImpl.createIndexMethod");
    }

    @Override
    protected QueryMethodResult insertMethod(S kafkaClient, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        try {
            List failMessages = new ArrayList();
            KafkaHandler.KafkaResourceInsert kafkaResourceInsert = queryEntity.getKafkaResourceInsert();
            List<Integer> partitions = tableConf.getPartitions();
            if (cn.hutool.core.collection.CollectionUtil.isNotEmpty(partitions) && partitions.size() > 1) {
                throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "Kafka 生产者指定分区时，有且只能指定一个分区!");
            }
            List<KafkaHandler.KafkaMessage> kafkaInsertList = kafkaResourceInsert.getKafkaInsertList();
            for (KafkaHandler.KafkaMessage kafkaMessage : kafkaInsertList) {
                KafkaProducer<String, Object> producer = kafkaClient.getProducer();
                ProducerRecord<String, Object> record;
                if (cn.hutool.core.collection.CollectionUtil.isNotEmpty(partitions)) {
                    record = new ProducerRecord(tableConf.getTableName(), partitions.get(0), kafkaMessage.getKey(), kafkaMessage.getMessage());
                } else {
                    //没有指定分区
                    record = new ProducerRecord(tableConf.getTableName(), null, kafkaMessage.getKey(), kafkaMessage.getMessage());
                }
                Future<RecordMetadata> send = producer.send(record);
                if (send.isCancelled()) {
                    failMessages.add(kafkaMessage.getMessage());
                }
            }

            if (failMessages.size() > 0) {
                log.error("kafka bulk add fail time:{}", failMessages.size());
                throw new BusinessException(ExceptionCode.ADD_DOC_EXCEPTION, failMessages.toString());
            }
            return new QueryMethodResult(kafkaInsertList.size(), null);
        } catch (Exception e) {
            log.error("kafka bulk add fail,detail:{}", e.getMessage());
            throw new BusinessException(ExceptionCode.ADD_DOC_EXCEPTION);
        }
    }

    @Override
    protected QueryMethodResult updateMethod(S kafkaClient, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "kafkaServiceImpl.updateMethod");
    }

    @Override
    protected QueryMethodResult delMethod(S kafkaClient, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "kafkaServiceImpl.delMethod");
    }

    @Override
    protected QueryMethodResult monitorStatusMethod(S kafkaClient, D databaseConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "kafkaServiceImpl.monitorStatusMethod");
    }

    @Override
    protected QueryMethodResult querySchemaMethod(S kafkaClient, D databaseConf, T tableConf) throws Exception {
        DescribeTopicsResult describeTopicsResult = kafkaClient.getAdminClient().describeTopics(Arrays.asList(tableConf.getTableName()));
        KafkaFuture<Map<String, TopicDescription>> all = describeTopicsResult.all();
        Map<String, TopicDescription> topicDescriptionMap = all.get();

        List<Map<String, Object>> result = new ArrayList<>();
        Map<String, Object> resultMap = new HashMap<>();
        // 1、解析主题信息
        List<Map<String, Object>> tableInfo = topicDescriptionMap.values()
                .stream()
                .map(topicDescription -> {
                    Map<String, Object> map = new HashMap();
                    map.put("name", topicDescription.name());
                    map.put("internal", topicDescription.isInternal());
                    map.put("partitions", topicDescription.partitions()
                            .stream()
                            .map(info -> {
                                Map<String, Object> infoMap = new HashMap();
                                infoMap.put("partition", info.partition());
                                infoMap.put("leader", info.leader().toString());
                                infoMap.put("replicas", info.replicas().stream().map(a -> a.toString()).collect(Collectors.toList()));
                                infoMap.put("isr", info.isr().stream().map(a -> a.toString()).collect(Collectors.toList()));
                                return infoMap;
                            }).collect(Collectors.toList())
                    );
                    return map;
                }).collect(Collectors.toList());
        resultMap.put("tableInfo", tableInfo);

        // 2、解析消息对象的mapping映射
        KafkaDatabaseInfo schemaDbInfo = ObjectUtils.clone(kafkaClient.getDatabaseConf());
        // 设置 schema 专用分组
        schemaDbInfo.setGroup("dat_schema_query");
        schemaDbInfo.setAutoOffsetReset("earliest");
        schemaDbInfo.setMaxPollRecord(10);
        schemaDbInfo.setEnableAutocommit(false);
        KafkaConsumer<String, Object> consumer = null;
        try {
            consumer = ((BaseKafkaConfigHelper) this.helper).createConsumer(schemaDbInfo);
            //订阅查询
            consumer.subscribe(Arrays.asList(tableConf.getTableName()), new NoOpConsumerRebalanceListener());
            ConsumerRecords<String, Object> records = consumer.poll(databaseConf.getKafkaBulkConsumeTimeout());
            String message = null;
            for (ConsumerRecord<String, Object> record : records) {
                message = (String) record.value();
                if (JSONUtil.isJson(message)) {
                    Map messageMap = JsonUtil.jsonStrToObject(message, Map.class);
                    resultMap.put("messageMapping", traverseResultSchema(messageMap));
                    break;
                }
            }
            //此处不手动提交位移，只是为了得到消息体的映射关系，不是为了消费消息
            if (!resultMap.containsKey("messageMapping")) {
                resultMap.put("messageMapping", CollectionUtil.EMPTY_LIST);
            }
            result.add(resultMap);
            return new QueryMethodResult(1, result);
        } finally {
            if (consumer != null) {
                consumer.close();
            }
        }
    }

    /**
     * 解析消息体字段名称与字段类型或字段的值
     *
     * @param rowMap
     * @return
     */
    private List<Map<String, Object>> traverseResultSchema(Map<String, Object> rowMap) {
        List<Map<String, Object>> result = new ArrayList<>();
        rowMap.forEach((key, value) -> {
            Map<String, Object> fieldMap = new HashMap<>(2);
            fieldMap.put("col_name", key);
            fieldMap.put("data_type", value == null ? "Null" : value.getClass().getSimpleName());
            result.add(fieldMap);
        });
        return result;
    }

    @Override
    protected QueryMethodResult queryDatabaseSchemaMethod(S connect, D databaseConf, T tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "kafkaServiceImpl.queryDatabaseSchemaMethod");
    }

    @Override
    protected QueryCursorMethodResult queryCursorMethod(S kafkaClient, Q queryEntity, D databaseConf, T tableConf, C cursorCache, Consumer<Map<String, Object>> consumer, Boolean rollAllData) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "kafkaServiceImpl.queryCursorMethod");
    }

    @Override
    protected QueryMethodResult saveOrUpdateMethod(S kafkaClient, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "kafkaServiceImpl.saveOrUpdateMethod");
    }

    @Override
    protected QueryMethodResult saveOrUpdateBatchMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "kafkaServiceImpl.saveOrUpdateBatchMethod");
    }

    @Override
    protected QueryMethodResult alterTableMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {

        AdminClient adminClient = connect.getAdminClient();

        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, tableConf.getTableName());

        List<ConfigEntry> entries = new ArrayList<>();

        String compressionType = tableConf.getCompressionType();
        if(compressionType != null) {
            entries.add(new ConfigEntry(TopicConfig.COMPRESSION_TYPE_CONFIG, compressionType));
        }
        String deleteRetentionMs = tableConf.getDeleteRetentionMs();
        if(deleteRetentionMs != null) {
            entries.add(new ConfigEntry(TopicConfig.DELETE_RETENTION_MS_CONFIG, deleteRetentionMs));
        }
        String fileDeleteDelayMs = tableConf.getFileDeleteDelayMs();
        if(fileDeleteDelayMs != null) {
            entries.add(new ConfigEntry(TopicConfig.FILE_DELETE_DELAY_MS_CONFIG, fileDeleteDelayMs));
        }
        String flushMessages = tableConf.getFlushMessages();
        if(flushMessages != null) {
            entries.add(new ConfigEntry(TopicConfig.FLUSH_MESSAGES_INTERVAL_CONFIG, flushMessages));
        }
        String flushMs = tableConf.getFlushMs();
        if(flushMs != null) {
            entries.add(new ConfigEntry(TopicConfig.FLUSH_MS_CONFIG, flushMs));
        }
        String followerReplicationThrottled = tableConf.getFollowerReplicationThrottled();
        if(followerReplicationThrottled != null) {
            entries.add(new ConfigEntry("follower.replication.throttled", followerReplicationThrottled));
        }
        String indexIntervalBytes = tableConf.getIndexIntervalBytes();
        if(indexIntervalBytes != null) {
            entries.add(new ConfigEntry(TopicConfig.INDEX_INTERVAL_BYTES_CONFIG, indexIntervalBytes));
        }
        String leaderReplicationThrottledReplicas = tableConf.getLeaderReplicationThrottledReplicas();
        if(leaderReplicationThrottledReplicas != null) {
            entries.add(new ConfigEntry("leader.replication.throttled.replicas", leaderReplicationThrottledReplicas));
        }
        String maxMessageBytes = tableConf.getMaxMessageBytes();
        if(maxMessageBytes != null) {
            entries.add(new ConfigEntry(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, maxMessageBytes));
        }
        String messageDownconversionEnable = tableConf.getMessageDownconversionEnable();
        if(messageDownconversionEnable != null) {
            entries.add(new ConfigEntry("message.downconversion.enable", messageDownconversionEnable));
        }
        String messageFormatVersion = tableConf.getMessageFormatVersion();
        if(messageFormatVersion != null) {
            entries.add(new ConfigEntry(TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG, messageFormatVersion));
        }
        String messageTimestampDifferenceMaxMs = tableConf.getMessageTimestampDifferenceMaxMs();
        if(messageTimestampDifferenceMaxMs != null) {
            entries.add(new ConfigEntry(TopicConfig.MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_CONFIG, messageTimestampDifferenceMaxMs));
        }
        String messageTimestampType = tableConf.getMessageTimestampType();
        if(messageTimestampType != null) {
            entries.add(new ConfigEntry(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG, messageTimestampType));
        }
        String minCleanableDirtyRatio = tableConf.getMinCleanableDirtyRatio();
        if(minCleanableDirtyRatio != null) {
            entries.add(new ConfigEntry(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, minCleanableDirtyRatio));
        }
        String minCompactionLagMs = tableConf.getMinCompactionLagMs();
        if(minCompactionLagMs != null) {
            entries.add(new ConfigEntry(TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG, minCompactionLagMs));
        }
        String minInsyncReplicas = tableConf.getMinInsyncReplicas();
        if(minInsyncReplicas != null) {
            entries.add(new ConfigEntry(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, minInsyncReplicas));
        }
        String preallocate = tableConf.getPreallocate();
        if(preallocate != null) {
            entries.add(new ConfigEntry(TopicConfig.PREALLOCATE_CONFIG, preallocate));
        }
        String retentionBytes = tableConf.getRetentionBytes();
        if(retentionBytes != null) {
            entries.add(new ConfigEntry(TopicConfig.RETENTION_BYTES_CONFIG, retentionBytes));
        }
        String retentionMs = tableConf.getRetentionMs();
        if(retentionMs != null) {
            entries.add(new ConfigEntry(TopicConfig.RETENTION_MS_CONFIG, retentionMs));
        }
        String segmentBytes = tableConf.getSegmentBytes();
        if(segmentBytes != null) {
            entries.add(new ConfigEntry(TopicConfig.SEGMENT_BYTES_CONFIG, segmentBytes));
        }
        String segmentIndexBytes = tableConf.getSegmentIndexBytes();
        if(segmentIndexBytes != null) {
            entries.add(new ConfigEntry(TopicConfig.SEGMENT_INDEX_BYTES_CONFIG, segmentIndexBytes));
        }
        String segmentJitterMs = tableConf.getSegmentJitterMs();
        if(segmentJitterMs != null) {
            entries.add(new ConfigEntry(TopicConfig.SEGMENT_JITTER_MS_CONFIG, segmentJitterMs));
        }
        String segmentMs = tableConf.getSegmentMs();
        if(segmentMs != null) {
            entries.add(new ConfigEntry(TopicConfig.SEGMENT_MS_CONFIG, segmentMs));
        }
        String uncleanLeaderElectionEnable = tableConf.getUncleanLeaderElectionEnable();
        if(uncleanLeaderElectionEnable != null) {
            entries.add(new ConfigEntry(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, uncleanLeaderElectionEnable));
        }

        if(entries.isEmpty()) {
            throw new BusinessException(ExceptionCode.CREATE_TABLE_EXCEPTION, "topic属性未发生变化，不需要进行修改");
        }

        Config config = new Config(entries);

        KafkaFuture<Void> all = adminClient.alterConfigs(Collections.singletonMap(configResource, config)).all();
        all.get(60, TimeUnit.SECONDS);
        if (all.isDone()) {
            return new QueryMethodResult();
        } else {
            throw new BusinessException(ExceptionCode.CREATE_TABLE_EXCEPTION, "修改topic失败! msg: 未成功.");
        }
    }

    @Override
    protected QueryMethodResult tableExistsMethod(S connect, D databaseConf, T tableConf) throws Exception {
        ListTopicsResult listTopicsResult = connect.getAdminClient().listTopics();
        KafkaFuture<Set<String>> set = listTopicsResult.names();
        List<String> topics = Lists.newArrayList(set.get());
        Map<String, Object> existsMap = new HashMap<>(1);
        if (topics.contains(tableConf.getTableName())) {
            existsMap.put(tableConf.getTableName(), true);
        } else {
            existsMap.put(tableConf.getTableName(), false);
        }
        return new QueryMethodResult(1, cn.hutool.core.collection.CollectionUtil.newArrayList(existsMap));
    }

    @Override
    protected QueryMethodResult queryTableInformationMethod(S connect, D databaseConf, T tableConf) throws Exception {
        DescribeTopicsResult topicsResult = connect.getAdminClient().describeTopics(cn.hutool.core.collection.CollectionUtil.newArrayList(tableConf.getTableName()));
        KafkaFuture<TopicDescription> kafkaFuture = topicsResult.values().get(tableConf.getTableName());
        TopicDescription topicDescription = kafkaFuture.get();
        // 分区数
        List<TopicPartitionInfo> partitions = topicDescription.partitions();
        // 副本数
        Integer replicaNum = 0;
        if (partitions.size() > 1) {
            List<Node> replicas = partitions.get(0).replicas();
            if (cn.hutool.core.collection.CollectionUtil.isNotEmpty(replicas)) {
                replicaNum = replicas.size();
            }
        }
        // 分区信息
        Map<String, Map<String, Object>> partitionInfos = new HashMap<>();
        for (TopicPartitionInfo partition : partitions) {
            int id = partition.partition();
            Node leader = partition.leader();
            Map<String, Object> leaderInfo = MapUtil.builder(new HashMap<String, Object>()).put("id", leader.id())
                    .put("host", leader.host())
                    .put("port", leader.port())
                    .build();
            List<Node> replicas = partition.replicas();
            List<Map<String, Object>> replicaNodes = new ArrayList<>(replicas.size());
            for (Node replica : replicas) {
                Map<String, Object> replicaInfo = MapUtil.builder(new HashMap<String, Object>()).put("id", replica.id())
                        .put("host", replica.host())
                        .put("port", replica.port())
                        .build();
                replicaNodes.add(replicaInfo);
            }
            partitionInfos.put(String.valueOf(id), MapUtil.builder(new HashMap<String, Object>(2)).put("leaderNode", leaderInfo).put("replicaNodes", replicaNodes).build());
        }
        // 分区数据量
        QueryMethodResult queryMethodResult = countMethod(connect, null, databaseConf, tableConf);
        List<Map<String, Object>> rows = queryMethodResult.getRows();
        if (cn.hutool.core.collection.CollectionUtil.isNotEmpty(rows)) {
            Map<String, Object> result = rows.get(0);
            Map<Integer, Long> beggingOffset = (Map<Integer, Long>) result.get("beggingOffset");
            Map<Integer, Long> endOffset = (Map<Integer, Long>) result.get("endOffset");
            for (Map.Entry<String, Map<String, Object>> entry : partitionInfos.entrySet()) {
                String key = entry.getKey();
                Map<String, Object> value = entry.getValue();
                Long begin = beggingOffset.get(Integer.valueOf(key));
                Long end = endOffset.get(Integer.valueOf(key));
                value.put("count", end - begin);
            }
        } else {
            for (Map.Entry<String, Map<String, Object>> entry : partitionInfos.entrySet()) {
                Map<String, Object> value = entry.getValue();
                value.put("count", 0L);
            }
        }
        // 订阅分组
        MyKafkaAdminClient myKafkaAdminClient = connect.getMyKafkaAdminClient_V1();
        MyListConsumerGroupsResult listConsumerGroupsResult = myKafkaAdminClient.listConsumerGroups();
        Collection<MyConsumerGroupListing> groupListings = listConsumerGroupsResult.all().get();
        if (cn.hutool.core.collection.CollectionUtil.isEmpty(groupListings)) {
            KafkaTableInformation kafkaTableInformation = new KafkaTableInformation(tableConf.getTableName(), partitions.size(), replicaNum, null, null, partitionInfos, com.meiya.whalex.util.collection.MapUtil.EMPTY_MAP);
            return new QueryMethodResult(1, cn.hutool.core.collection.CollectionUtil.newArrayList(JsonUtil.entityToMap(kafkaTableInformation)));
        }
        List<String> groupIds = groupListings.stream().filter(group -> !StringUtils.startsWith(group.groupId(), "kafka-consumer-client") && !StringUtils.startsWith(group.groupId(), "console-consumer")).flatMap(group -> Stream.of(group.groupId())).collect(Collectors.toList());

        if (cn.hutool.core.collection.CollectionUtil.isEmpty(groupIds)) {
            KafkaTableInformation kafkaTableInformation = new KafkaTableInformation(tableConf.getTableName(), partitions.size(), replicaNum, null, null, partitionInfos, com.meiya.whalex.util.collection.MapUtil.EMPTY_MAP);
            return new QueryMethodResult(1, cn.hutool.core.collection.CollectionUtil.newArrayList(JsonUtil.entityToMap(kafkaTableInformation)));
        }

        Map<String, Map<Integer, Long>> groupInfoMap = new HashMap<>(8);
        // 构造 TopicPartition
        List<TopicPartition> topicPartitions = new ArrayList<>();
        for (TopicPartitionInfo partition : partitions) {
            topicPartitions.add(new TopicPartition(tableConf.getTableName(), partition.partition()));
        }
        groupIds.parallelStream().forEach(groupId -> {
            try {
                // 获取分组消费位移
                MyListConsumerGroupOffsetsOptions listConsumerGroupOffsetsOptions = new MyListConsumerGroupOffsetsOptions().topicPartitions(topicPartitions);
                MyListConsumerGroupOffsetsResult listConsumerGroupOffsetsResult = myKafkaAdminClient.listConsumerGroupOffsets(groupId, listConsumerGroupOffsetsOptions);
                Map<TopicPartition, OffsetAndMetadata> topicPartitionOffsetAndMetadataMap = listConsumerGroupOffsetsResult.partitionsToOffsetAndMetadata().get();
                if (cn.hutool.core.map.MapUtil.isEmpty(topicPartitionOffsetAndMetadataMap)) {
                    return;
                }
                Map<Integer, Long> committedOffsetMap = topicPartitionOffsetAndMetadataMap.entrySet().stream()
                        // 过滤掉当前topic无消费的记录
                        .filter((metadataEntry) -> metadataEntry != null && metadataEntry.getValue() != null)
                        .flatMap(metadataEntry -> {
                            OffsetAndMetadata value = metadataEntry.getValue();
                            Map<Integer, Long> offset = new HashMap<>(8);
                            offset.put(metadataEntry.getKey().partition(), value.offset());
                            return Stream.of(offset);
                        }).reduce((a, b) -> {
                            a.putAll(b);
                            return a;
                        }).orElse(null);
                if (cn.hutool.core.map.MapUtil.isNotEmpty(committedOffsetMap)) {
                    groupInfoMap.put(groupId, committedOffsetMap);
                }
            } catch (Exception e) {
                log.error("kafka topic group info fail! topic: [{}] group: [{}]", tableConf.getTableName(), groupId, e);
            }
        });
        KafkaTableInformation kafkaTableInformation = new KafkaTableInformation(tableConf.getTableName(), partitions.size(), replicaNum, null, null, partitionInfos, groupInfoMap);
        return new QueryMethodResult(1, cn.hutool.core.collection.CollectionUtil.newArrayList(JsonUtil.entityToMap(kafkaTableInformation)));
    }

    @Override
    protected QueryMethodResult queryListDatabaseMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + ".queryListDatabaseMethod");
    }

    @Override
    protected QueryMethodResult queryDatabaseInformationMethod(S connect, Q queryEntity, D databaseConf, T tableConf) {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + ".queryDatabaseInformationMethod");
    }

}
