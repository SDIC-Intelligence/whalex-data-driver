package com.meiya.whalex.db.util.param.impl.stream;

import java.util.Arrays;
import java.util.List;

/**
 * kafka的各个参数
 */
public class KafkaConstantsProperty {
	public static String brokerAddress = "15.40.3.45:9092,15.40.3.39:9092,15.40.3.7:9092";
	public static int batchSize = 16384;
	public static int bufferMemory = 33554432;
	public static int sessionTimeOut = 60 * 1000;
	public static int lingerMs = 5;
	public static int retries = 0;
	public static String groupId = "whale";
	public static String autoOffsetReset = "earliest";
	public static Integer maxPollRecords = 10;
	public static Boolean enableAutoCommit = false;
	public static int autoCommitInterval = 1000;
	public static List<String> topicList = Arrays.asList("TEST_TEST,TEST_TEST_2".split(","));
	public static final String SERIALIZER_CLASS = "org.apache.kafka.common.serialization.StringSerializer";
	public static final String SERIALIZER_DESER_CLASS = "org.apache.kafka.common.serialization.StringDeserializer";
	/**
	 * provider默认传送的topic
	 */
	public static String defaultTopic = "whale-cloud-platform-log";
	public static String DALtopic = "whale-cloud-platform-dal-log";
	public static String DSL_Ctopic = "whale-cloud-platform-dsl-log";
	private static boolean launchConsumer;
	private static int consumerWorkerNum;
}
