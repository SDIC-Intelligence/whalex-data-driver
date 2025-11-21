package com.meiya.whalex.db.kafka.admin.client;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.*;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.*;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.errors.*;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.metrics.*;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.*;
import org.apache.kafka.common.utils.*;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * 自定义 kafka admin 方法，完善 1.0 缺失的方法
 * 若使用 2.0 以上版本，则无需获取该方法
 *
 * @author 黄河森
 * @date 2021/9/19
 * @package com.meiya.whalex.db.kafka.admin.client
 * @project whalex-data-driver
 */
@InterfaceStability.Evolving
@Slf4j
public class MyKafkaAdminClient implements AutoCloseable {

    private static final AtomicInteger ADMIN_CLIENT_ID_SEQUENCE = new AtomicInteger(1);
    private static final String JMX_PREFIX = "kafka.admin.client";
    private static final long INVALID_SHUTDOWN_TIME = -1L;
    private final int defaultTimeoutMs;
    private final String clientId;
    private final Time time;
    private final Metadata metadata;
    private final Metrics metrics;
    private final KafkaClient client;
    private final MyKafkaAdminClient.AdminClientRunnable runnable;
    private final Thread thread;
    private final AtomicLong hardShutdownTimeMs = new AtomicLong(-1L);
    private final MyKafkaAdminClient.TimeoutProcessorFactory timeoutProcessorFactory;
    private final int maxRetries;

    public static MyKafkaAdminClient createDatKafkaAdmin(Properties props) {
        return MyKafkaAdminClient.createInternal(new AdminClientConfig(props), null);
    }

    static <K, V> List<V> getOrCreateListValue(Map<K, List<V>> map, K key) {
        List<V> list = map.get(key);
        if (list != null) {
            return list;
        } else {
            list = new LinkedList();
            map.put(key, list);
            return list;
        }
    }

    private static <T> void completeAllExceptionally(Collection<KafkaFutureImpl<T>> futures, Throwable exc) {
        Iterator var2 = futures.iterator();

        while(var2.hasNext()) {
            KafkaFutureImpl<?> future = (KafkaFutureImpl)var2.next();
            future.completeExceptionally(exc);
        }

    }

    static int calcTimeoutMsRemainingAsInt(long now, long deadlineMs) {
        long deltaMs = deadlineMs - now;
        if (deltaMs > 2147483647L) {
            deltaMs = 2147483647L;
        } else if (deltaMs < -2147483648L) {
            deltaMs = -2147483648L;
        }

        return (int)deltaMs;
    }

    static String generateClientId(AdminClientConfig config) {
        String clientId = config.getString("client.id");
        return !clientId.isEmpty() ? clientId : "datAdminClient-" + ADMIN_CLIENT_ID_SEQUENCE.getAndIncrement();
    }

    private long calcDeadlineMs(long now, Integer optionTimeoutMs) {
        return optionTimeoutMs != null ? now + (long)Math.max(0, optionTimeoutMs) : now + (long)this.defaultTimeoutMs;
    }

    static String prettyPrintException(Throwable throwable) {
        if (throwable == null) {
            return "Null exception.";
        } else {
            return throwable.getMessage() != null ? throwable.getClass().getSimpleName() + ": " + throwable.getMessage() : throwable.getClass().getSimpleName();
        }
    }

    static MyKafkaAdminClient createInternal(AdminClientConfig config, MyKafkaAdminClient.TimeoutProcessorFactory timeoutProcessorFactory) {
        Metrics metrics = null;
        NetworkClient networkClient = null;
        Time time = Time.SYSTEM;
        String clientId = generateClientId(config);
        ChannelBuilder channelBuilder = null;
        Selector selector = null;
        ApiVersions apiVersions = new ApiVersions();
        LogContext logContext = createLogContext(clientId);

        try {
            Metadata metadata = new Metadata(config.getLong("retry.backoff.ms"), config.getLong("metadata.max.age.ms"), true);
            List<MetricsReporter> reporters = config.getConfiguredInstances("metric.reporters", MetricsReporter.class);
            Map<String, String> metricTags = Collections.singletonMap("client-id", clientId);
            MetricConfig metricConfig = (new MetricConfig()).samples(config.getInt("metrics.num.samples")).timeWindow(config.getLong("metrics.sample.window.ms"), TimeUnit.MILLISECONDS).recordLevel(Sensor.RecordingLevel.forName(config.getString("metrics.recording.level"))).tags(metricTags);
            reporters.add(new JmxReporter(JMX_PREFIX));
            metrics = new Metrics(metricConfig, reporters, time);
            String metricGrpPrefix = "admin-client";
            channelBuilder = ClientUtils.createChannelBuilder(config);
            selector = new Selector(config.getLong("connections.max.idle.ms"), metrics, time, metricGrpPrefix, channelBuilder, logContext);
            networkClient = new NetworkClient(selector, metadata, clientId, 1, config.getLong("reconnect.backoff.ms"), config.getLong("reconnect.backoff.max.ms"), config.getInt("send.buffer.bytes"), config.getInt("receive.buffer.bytes"), (int)TimeUnit.HOURS.toMillis(1L), time, true, apiVersions, logContext);
            return new MyKafkaAdminClient(config, clientId, time, metadata, metrics, networkClient, timeoutProcessorFactory);
        } catch (Throwable var15) {
            Utils.closeQuietly(metrics, "Metrics");
            Utils.closeQuietly(networkClient, "NetworkClient");
            Utils.closeQuietly(selector, "Selector");
            Utils.closeQuietly(channelBuilder, "ChannelBuilder");
            throw new KafkaException("Failed create new MyKafkaAdminClient", var15);
        }
    }

    static MyKafkaAdminClient createInternal(AdminClientConfig config, KafkaClient client, Metadata metadata, Time time) {
        Metrics metrics = null;
        String clientId = generateClientId(config);

        try {
            metrics = new Metrics(new MetricConfig(), new LinkedList(), time);
            return new MyKafkaAdminClient(config, clientId, time, metadata, metrics, client, (MyKafkaAdminClient.TimeoutProcessorFactory)null);
        } catch (Throwable var7) {
            Utils.closeQuietly(metrics, "Metrics");
            throw new KafkaException("Failed create new MyKafkaAdminClient", var7);
        }
    }

    private static LogContext createLogContext(String clientId) {
        return new LogContext("[DatAdminClient clientId=" + clientId + "] ");
    }

    private MyKafkaAdminClient(AdminClientConfig config, String clientId, Time time, Metadata metadata, Metrics metrics, KafkaClient client, MyKafkaAdminClient.TimeoutProcessorFactory timeoutProcessorFactory) {
        this.defaultTimeoutMs = config.getInt("request.timeout.ms");
        this.clientId = clientId;
        this.time = time;
        this.metadata = metadata;
        List<InetSocketAddress> addresses = ClientUtils.parseAndValidateAddresses(config.getList("bootstrap.servers"));
        this.metadata.update(Cluster.bootstrap(addresses), Collections.emptySet(), time.milliseconds());
        this.metrics = metrics;
        this.client = client;
        this.runnable = new MyKafkaAdminClient.AdminClientRunnable();
        String threadName = "dat-kafka-admin-client-thread | " + clientId;
        this.thread = new KafkaThread(threadName, this.runnable, true);
        this.timeoutProcessorFactory = timeoutProcessorFactory == null ? new MyKafkaAdminClient.TimeoutProcessorFactory() : timeoutProcessorFactory;
        this.maxRetries = config.getInt("retries");
        config.logUnused();
        AppInfoParser.registerAppInfo(JMX_PREFIX, clientId, metrics);
        log.debug("dat Kafka admin client initialized");
        // 启动一个线程轮询 runnable
        this.thread.start();
    }

    Time time() {
        return this.time;
    }

    public void close(long duration, TimeUnit unit) {
        long waitTimeMs = unit.toMillis(duration);
        waitTimeMs = Math.min(TimeUnit.DAYS.toMillis(365L), waitTimeMs);
        long now = this.time.milliseconds();
        long newHardShutdownTimeMs = now + waitTimeMs;
        long prev = -1L;

        while(true) {
            if (this.hardShutdownTimeMs.compareAndSet(prev, newHardShutdownTimeMs)) {
                if (prev == -1L) {
                    log.debug("Initiating close operation.");
                } else {
                    log.debug("Moving hard shutdown time forward.");
                }

                this.client.wakeup();
                break;
            }

            prev = this.hardShutdownTimeMs.get();
            if (prev < newHardShutdownTimeMs) {
                log.debug("Hard shutdown time is already earlier than requested.");
                newHardShutdownTimeMs = prev;
                break;
            }
        }

        if (log.isDebugEnabled()) {
            long deltaMs = Math.max(0L, newHardShutdownTimeMs - this.time.milliseconds());
            log.debug("Waiting for the I/O thread to exit. Hard shutdown in {} ms.", deltaMs);
        }

        try {
            this.thread.join();
            AppInfoParser.unregisterAppInfo(JMX_PREFIX, this.clientId, this.metrics);
            log.debug("Kafka admin client closed.");
        } catch (InterruptedException var14) {
            log.debug("Interrupted while joining I/O thread", var14);
            Thread.currentThread().interrupt();
        }

    }

    @Override
    public void close() {
        this.close(9223372036854775807L, TimeUnit.MILLISECONDS);
    }

    /**
     * Admin 执行器
     */
    private final class AdminClientRunnable implements Runnable {
        /**
         * 任务队列
         */
        private List<MyKafkaAdminClient.Call> newCalls;

        private AdminClientRunnable() {
            this.newCalls = new LinkedList();
        }

        private Integer checkMetadataReady(Integer prevMetadataVersion) {
            if (prevMetadataVersion != null && prevMetadataVersion == MyKafkaAdminClient.this.metadata.version()) {
                return prevMetadataVersion;
            } else {
                Cluster cluster = MyKafkaAdminClient.this.metadata.fetch();
                if (cluster.nodes().isEmpty()) {
                    MyKafkaAdminClient.log.trace("Metadata is not ready yet. No cluster nodes found.");
                    return MyKafkaAdminClient.this.metadata.requestUpdate();
                } else if (cluster.controller() == null) {
                    MyKafkaAdminClient.log.trace("Metadata is not ready yet. No controller found.");
                    return MyKafkaAdminClient.this.metadata.requestUpdate();
                } else {
                    if (prevMetadataVersion != null) {
                        MyKafkaAdminClient.log.trace("Metadata is now ready.");
                    }

                    return null;
                }
            }
        }

        private synchronized void timeoutNewCalls(MyKafkaAdminClient.TimeoutProcessor processor) {
            int numTimedOut = processor.handleTimeouts(this.newCalls, "Timed out waiting for a node assignment.");
            if (numTimedOut > 0) {
                MyKafkaAdminClient.log.debug("Timed out {} new calls.", numTimedOut);
            }

        }

        private void timeoutCallsToSend(MyKafkaAdminClient.TimeoutProcessor processor, Map<Node, List<MyKafkaAdminClient.Call>> callsToSend) {
            int numTimedOut = 0;

            List callList;
            for(Iterator var4 = callsToSend.values().iterator(); var4.hasNext(); numTimedOut += processor.handleTimeouts(callList, "Timed out waiting to send the call.")) {
                callList = (List)var4.next();
            }

            if (numTimedOut > 0) {
                MyKafkaAdminClient.log.debug("Timed out {} call(s) with assigned nodes.", numTimedOut);
            }

        }

        private void chooseNodesForNewCalls(long now, Map<Node, List<MyKafkaAdminClient.Call>> callsToSend) {
            List<MyKafkaAdminClient.Call> newCallsToAdd = null;
            synchronized(this) {
                if (this.newCalls.isEmpty()) {
                    return;
                }

                newCallsToAdd = this.newCalls;
                this.newCalls = new LinkedList();
            }

            Iterator var5 = newCallsToAdd.iterator();

            while(var5.hasNext()) {
                MyKafkaAdminClient.Call call = (MyKafkaAdminClient.Call)var5.next();
                this.chooseNodeForNewCall(now, callsToSend, call);
            }

        }

        private void chooseNodeForNewCall(long now, Map<Node, List<MyKafkaAdminClient.Call>> callsToSend, MyKafkaAdminClient.Call call) {
            Node node = call.nodeProvider.provide();
            if (node == null) {
                call.fail(now, new BrokerNotAvailableException(String.format("Error choosing node for %s: no node found.", call.callName)));
            } else {
                MyKafkaAdminClient.log.trace("Assigned {} to {}", call, node);
                MyKafkaAdminClient.getOrCreateListValue(callsToSend, node).add(call);
            }
        }

        /**
         * 选举合格的任务
         *
         * @param now
         * @param callsToSend
         * @param correlationIdToCalls
         * @param callsInFlight
         * @return
         */
        private long sendEligibleCalls(long now, Map<Node, List<MyKafkaAdminClient.Call>> callsToSend, Map<Integer, MyKafkaAdminClient.Call> correlationIdToCalls, Map<String, List<MyKafkaAdminClient.Call>> callsInFlight) {
            long pollTimeout = 9223372036854775807L;
            Iterator iter = callsToSend.entrySet().iterator();

            while(true) {
                while(iter.hasNext()) {
                    Map.Entry<Node, List<MyKafkaAdminClient.Call>> entry = (Map.Entry)iter.next();
                    List<MyKafkaAdminClient.Call> calls = entry.getValue();
                    if (calls.isEmpty()) {
                        iter.remove();
                    } else {
                        Node node = entry.getKey();
                        // 如果当前选举的node节点不为ready状态
                        if (!MyKafkaAdminClient.this.client.ready(node, now)) {
                            long nodeTimeout = MyKafkaAdminClient.this.client.connectionDelay(node, now);
                            pollTimeout = Math.min(pollTimeout, nodeTimeout);
                            MyKafkaAdminClient.log.trace("Client is not ready to send to {}. Must delay {} ms", node, nodeTimeout);
                        } else {
                            MyKafkaAdminClient.Call call = calls.remove(0);
                            int timeoutMs = MyKafkaAdminClient.calcTimeoutMsRemainingAsInt(now, call.deadlineMs);
                            AbstractRequest.Builder requestBuilder = null;

                            try {
                                requestBuilder = call.createRequest(timeoutMs);
                            } catch (Throwable var16) {
                                call.fail(now, new KafkaException(String.format("Internal error sending %s to %s.", call.callName, node)));
                                continue;
                            }

                            ClientRequest clientRequest = MyKafkaAdminClient.this.client.newClientRequest(node.idString(), requestBuilder, now, true);
                            MyKafkaAdminClient.log.trace("Sending {} to {}. correlationId={}", new Object[]{requestBuilder, node, clientRequest.correlationId()});
                            MyKafkaAdminClient.this.client.send(clientRequest, now);
                            MyKafkaAdminClient.getOrCreateListValue(callsInFlight, node.idString()).add(call);
                            correlationIdToCalls.put(clientRequest.correlationId(), call);
                        }
                    }
                }

                return pollTimeout;
            }
        }

        private void timeoutCallsInFlight(MyKafkaAdminClient.TimeoutProcessor processor, Map<String, List<MyKafkaAdminClient.Call>> callsInFlight) {
            int numTimedOut = 0;
            Iterator var4 = callsInFlight.entrySet().iterator();

            while(var4.hasNext()) {
                Map.Entry<String, List<MyKafkaAdminClient.Call>> entry = (Map.Entry)var4.next();
                List<MyKafkaAdminClient.Call> contexts = entry.getValue();
                if (!contexts.isEmpty()) {
                    String nodeId = entry.getKey();
                    MyKafkaAdminClient.Call call = contexts.get(0);
                    if (processor.callHasExpired(call)) {
                        if (call.aborted) {
                            MyKafkaAdminClient.log.warn("Aborted call {} is still in callsInFlight.", call);
                        } else {
                            MyKafkaAdminClient.log.debug("Closing connection to {} to time out {}", nodeId, call);
                            call.aborted = true;
                            MyKafkaAdminClient.this.client.disconnect(nodeId);
                            ++numTimedOut;
                        }
                    }
                }
            }

            if (numTimedOut > 0) {
                MyKafkaAdminClient.log.debug("Timed out {} call(s) in flight.", numTimedOut);
            }

        }

        /**
         * 认证异常处理
         *
         * @param now
         * @param callsToSend
         */
        private void handleAuthenticationException(long now, Map<Node, List<MyKafkaAdminClient.Call>> callsToSend) {
            AuthenticationException authenticationException = MyKafkaAdminClient.this.metadata.getAndClearAuthenticationException();
            Iterator var5;
            if (authenticationException == null) {
                var5 = callsToSend.keySet().iterator();

                while(var5.hasNext()) {
                    Node node = (Node)var5.next();
                    authenticationException = MyKafkaAdminClient.this.client.authenticationException(node);
                    if (authenticationException != null) {
                        break;
                    }
                }
            }

            if (authenticationException != null) {
                synchronized(this) {
                    this.failCalls(now, this.newCalls, authenticationException);
                }

                var5 = callsToSend.values().iterator();

                while(var5.hasNext()) {
                    List<MyKafkaAdminClient.Call> calls = (List)var5.next();
                    this.failCalls(now, calls, authenticationException);
                }

                callsToSend.clear();
            }

        }

        /**
         * 任务执行失败处理
         *
         * @param now
         * @param calls
         * @param authenticationException
         */
        private void failCalls(long now, List<MyKafkaAdminClient.Call> calls, AuthenticationException authenticationException) {
            Iterator var5 = calls.iterator();

            while(var5.hasNext()) {
                MyKafkaAdminClient.Call call = (MyKafkaAdminClient.Call)var5.next();
                call.fail(now, authenticationException);
            }

            calls.clear();
        }

        /**
         * 任务结果处理
         *
         * @param now
         * @param responses
         * @param callsInFlight
         * @param correlationIdToCall
         */
        private void handleResponses(long now, List<ClientResponse> responses, Map<String, List<MyKafkaAdminClient.Call>> callsInFlight, Map<Integer, MyKafkaAdminClient.Call> correlationIdToCall) {
            Iterator var6 = responses.iterator();

            while(true) {
                while(var6.hasNext()) {
                    ClientResponse response = (ClientResponse)var6.next();
                    int correlationId = response.requestHeader().correlationId();
                    MyKafkaAdminClient.Call call = (MyKafkaAdminClient.Call)correlationIdToCall.get(correlationId);
                    if (call == null) {
                        MyKafkaAdminClient.log.error("Internal server error on {}: server returned information about unknown correlation ID {}, requestHeader = {}", new Object[]{response.destination(), correlationId, response.requestHeader()});
                        MyKafkaAdminClient.this.client.disconnect(response.destination());
                    } else {
                        correlationIdToCall.remove(correlationId);
                        List<MyKafkaAdminClient.Call> calls = (List)callsInFlight.get(response.destination());
                        if (calls != null && calls.remove(call)) {
                            if (response.versionMismatch() != null) {
                                call.fail(now, response.versionMismatch());
                            } else if (response.wasDisconnected()) {
                                call.fail(now, new DisconnectException(String.format("Cancelled %s request with correlation id %s due to node %s being disconnected", call.callName, correlationId, response.destination())));
                            } else {
                                try {
                                    call.handleResponse(response.responseBody());
                                    if (MyKafkaAdminClient.log.isTraceEnabled()) {
                                        MyKafkaAdminClient.log.trace("{} got response {}", call, response.responseBody().toString(response.requestHeader().apiVersion()));
                                    }
                                } catch (Throwable var12) {
                                    if (MyKafkaAdminClient.log.isTraceEnabled()) {
                                        MyKafkaAdminClient.log.trace("{} handleResponse failed with {}", call, MyKafkaAdminClient.prettyPrintException(var12));
                                    }

                                    call.fail(now, var12);
                                }
                            }
                        } else {
                            MyKafkaAdminClient.log.error("Internal server error on {}: ignoring call {} in correlationIdToCall that did not exist in callsInFlight", response.destination(), call);
                        }
                    }
                }

                return;
            }
        }

        /**
         * 判断当前任务执行线程是否可以退出
         *
         * @param now
         * @param curHardShutdownTimeMs
         * @param callsToSend
         * @param correlationIdToCalls
         * @return
         */
        private synchronized boolean threadShouldExit(long now, long curHardShutdownTimeMs, Map<Node, List<MyKafkaAdminClient.Call>> callsToSend, Map<Integer, MyKafkaAdminClient.Call> correlationIdToCalls) {
            if (this.newCalls.isEmpty() && callsToSend.isEmpty() && correlationIdToCalls.isEmpty()) {
                MyKafkaAdminClient.log.trace("All work has been completed, and the I/O thread is now exiting.");
                return true;
            } else if (now > curHardShutdownTimeMs) {
                MyKafkaAdminClient.log.info("Forcing a hard I/O thread shutdown. Requests in progress will be aborted.");
                return true;
            } else {
                MyKafkaAdminClient.log.debug("Hard shutdown in {} ms.", curHardShutdownTimeMs - now);
                return false;
            }
        }

        /**
         * 任务执行逻辑
         * 第一步：判断 prevMetadataVersion，为 null 时，判断 Metadata 对象内部集群信息是否完整，若未完整获取，则先更新 Metadata 中的集群信息
         * 第二部：执行目标方法
         */
        @Override
        public void run() {
            // 节点对象 -> 任务集合
            Map<Node, List<MyKafkaAdminClient.Call>> callsToSend = new HashMap();
            // 节点ID -> 任务集合
            Map<String, List<MyKafkaAdminClient.Call>> callsInFlight = new HashMap();
            // 请求对象 ClientRequest.correlationId() -> 任务 call
            Map<Integer, MyKafkaAdminClient.Call> correlationIdToCalls = new HashMap();
            Integer prevMetadataVersion = null;
            long now = MyKafkaAdminClient.this.time.milliseconds();
            log.trace("Thread starting");

            while(true) {
                // 若当前 admin 是 close 状态，则 hardShutdownTimeMs 不为 -1，则当前任务无法完成，直接返回
                long curHardShutdownTimeMs = MyKafkaAdminClient.this.hardShutdownTimeMs.get();
                if (curHardShutdownTimeMs != -1L && this.threadShouldExit(now, curHardShutdownTimeMs, callsToSend, correlationIdToCalls)) {
                    int numTimedOut = 0;
                    MyKafkaAdminClient.TimeoutProcessor timeoutProcessor = new MyKafkaAdminClient.TimeoutProcessor(9223372036854775807L);
                    int numTimedOutx;
                    synchronized(this) {
                        numTimedOutx = numTimedOut + timeoutProcessor.handleTimeouts(this.newCalls, "The AdminClient thread has exited.");
                        this.newCalls = null;
                    }

                    numTimedOutx += timeoutProcessor.handleTimeouts(correlationIdToCalls.values(), "The AdminClient thread has exited.");
                    if (numTimedOutx > 0) {
                        log.debug("Timed out {} remaining operations.", numTimedOutx);
                    }

                    Utils.closeQuietly(MyKafkaAdminClient.this.client, "KafkaClient");
                    Utils.closeQuietly(MyKafkaAdminClient.this.metrics, "Metrics");
                    log.debug("Exiting AdminClientRunnable thread.");
                    return;
                }

                MyKafkaAdminClient.TimeoutProcessor timeoutProcessor = MyKafkaAdminClient.this.timeoutProcessorFactory.create(now);
                // 判断在 newCall 队列中的任务是否超时
                this.timeoutNewCalls(timeoutProcessor);
                // 判断当前执行的 call 是否超时
                this.timeoutCallsToSend(timeoutProcessor, callsToSend);
                this.timeoutCallsInFlight(timeoutProcessor, callsInFlight);
                long pollTimeout = Math.min(1200000, timeoutProcessor.nextTimeoutMs());
                if (curHardShutdownTimeMs != -1L) {
                    pollTimeout = Math.min(pollTimeout, curHardShutdownTimeMs - now);
                }

                // 校验元数据信息版本状态
                prevMetadataVersion = this.checkMetadataReady(prevMetadataVersion);
                if (prevMetadataVersion == null) {
                    // 选举一个节点执行任务
                    this.chooseNodesForNewCalls(now, callsToSend);
                    pollTimeout = Math.min(pollTimeout, this.sendEligibleCalls(now, callsToSend, correlationIdToCalls, callsInFlight));
                }

                log.trace("Entering KafkaClient#poll(timeout={})", pollTimeout);
                List<ClientResponse> responses = MyKafkaAdminClient.this.client.poll(pollTimeout, now);
                log.trace("KafkaClient#poll retrieved {} response(s)", responses.size());
                now = MyKafkaAdminClient.this.time.milliseconds();
                this.handleAuthenticationException(now, callsToSend);
                this.handleResponses(now, responses, callsInFlight, correlationIdToCalls);
            }
        }

        void enqueue(MyKafkaAdminClient.Call call, long now) {
            if (log.isDebugEnabled()) {
                log.debug("Queueing {} with a timeout {} ms from now.", call, call.deadlineMs - now);
            }

            boolean accepted = false;
            synchronized(this) {
                if (this.newCalls != null) {
                    this.newCalls.add(call);
                    accepted = true;
                }
            }

            if (accepted) {
                MyKafkaAdminClient.this.client.wakeup();
            } else {
                log.debug("The AdminClient thread has exited. Timing out {}.", call);
                call.fail(9223372036854775807L, new TimeoutException("The AdminClient thread has exited."));
            }

        }

        void call(MyKafkaAdminClient.Call call, long now) {
            if (MyKafkaAdminClient.this.hardShutdownTimeMs.get() != -1L) {
                MyKafkaAdminClient.log.debug("The AdminClient is not accepting new calls. Timing out {}.", call);
                call.fail(9223372036854775807L, new TimeoutException("The AdminClient thread is not accepting new calls."));
            } else {
                this.enqueue(call, now);
            }

        }
    }

    static class TimeoutProcessor {
        private final long now;
        private int nextTimeoutMs;

        TimeoutProcessor(long now) {
            this.now = now;
            this.nextTimeoutMs = 2147483647;
        }

        int handleTimeouts(Collection<MyKafkaAdminClient.Call> calls, String msg) {
            int numTimedOut = 0;
            Iterator iter = calls.iterator();

            while(iter.hasNext()) {
                MyKafkaAdminClient.Call call = (MyKafkaAdminClient.Call)iter.next();
                int remainingMs = MyKafkaAdminClient.calcTimeoutMsRemainingAsInt(this.now, call.deadlineMs);
                if (remainingMs < 0) {
                    call.fail(this.now, new TimeoutException(msg));
                    iter.remove();
                    ++numTimedOut;
                } else {
                    this.nextTimeoutMs = Math.min(this.nextTimeoutMs, remainingMs);
                }
            }

            return numTimedOut;
        }

        boolean callHasExpired(MyKafkaAdminClient.Call call) {
            int remainingMs = MyKafkaAdminClient.calcTimeoutMsRemainingAsInt(this.now, call.deadlineMs);
            if (remainingMs < 0) {
                return true;
            } else {
                this.nextTimeoutMs = Math.min(this.nextTimeoutMs, remainingMs);
                return false;
            }
        }

        int nextTimeoutMs() {
            return this.nextTimeoutMs;
        }
    }

    static class TimeoutProcessorFactory {
        TimeoutProcessorFactory() {
        }

        MyKafkaAdminClient.TimeoutProcessor create(long now) {
            return new MyKafkaAdminClient.TimeoutProcessor(now);
        }
    }

    abstract class Call {
        private final String callName;
        private final long deadlineMs;
        private final MyKafkaAdminClient.NodeProvider nodeProvider;
        private int tries = 0;
        private boolean aborted = false;

        Call(String callName, long deadlineMs, MyKafkaAdminClient.NodeProvider nodeProvider) {
            this.callName = callName;
            this.deadlineMs = deadlineMs;
            this.nodeProvider = nodeProvider;
        }

        final void fail(long now, Throwable throwable) {
            if (this.aborted) {
                ++this.tries;
                if (MyKafkaAdminClient.log.isDebugEnabled()) {
                    MyKafkaAdminClient.log.debug("{} aborted at {} after {} attempt(s)", new Object[]{this, now, this.tries, new Exception(MyKafkaAdminClient.prettyPrintException(throwable))});
                }

                this.handleFailure(new TimeoutException("Aborted due to timeout."));
            } else if (throwable instanceof UnsupportedVersionException && this.handleUnsupportedVersionException((UnsupportedVersionException)throwable)) {
                MyKafkaAdminClient.log.trace("{} attempting protocol downgrade.", this);
                MyKafkaAdminClient.this.runnable.enqueue(this, now);
            } else {
                ++this.tries;
                if (MyKafkaAdminClient.calcTimeoutMsRemainingAsInt(now, this.deadlineMs) < 0) {
                    if (MyKafkaAdminClient.log.isDebugEnabled()) {
                        MyKafkaAdminClient.log.debug("{} timed out at {} after {} attempt(s)", new Object[]{this, now, this.tries, new Exception(MyKafkaAdminClient.prettyPrintException(throwable))});
                    }

                    this.handleFailure(throwable);
                } else if (!(throwable instanceof RetriableException)) {
                    if (MyKafkaAdminClient.log.isDebugEnabled()) {
                        MyKafkaAdminClient.log.debug("{} failed with non-retriable exception after {} attempt(s)", new Object[]{this, this.tries, new Exception(MyKafkaAdminClient.prettyPrintException(throwable))});
                    }

                    this.handleFailure(throwable);
                } else if (this.tries > MyKafkaAdminClient.this.maxRetries) {
                    if (MyKafkaAdminClient.log.isDebugEnabled()) {
                        MyKafkaAdminClient.log.debug("{} failed after {} attempt(s)", new Object[]{this, this.tries, new Exception(MyKafkaAdminClient.prettyPrintException(throwable))});
                    }

                    this.handleFailure(throwable);
                } else {
                    if (MyKafkaAdminClient.log.isDebugEnabled()) {
                        MyKafkaAdminClient.log.debug("{} failed: {}. Beginning retry #{}", new Object[]{this, MyKafkaAdminClient.prettyPrintException(throwable), this.tries});
                    }

                    MyKafkaAdminClient.this.runnable.enqueue(this, now);
                }
            }
        }

        abstract AbstractRequest.Builder createRequest(int var1);

        abstract void handleResponse(AbstractResponse var1);

        abstract void handleFailure(Throwable var1);

        boolean handleUnsupportedVersionException(UnsupportedVersionException exception) {
            return false;
        }

        @Override
        public String toString() {
            return "Call(callName=" + this.callName + ", deadlineMs=" + this.deadlineMs + ")";
        }
    }

    private class LeastLoadedNodeProvider implements MyKafkaAdminClient.NodeProvider {
        private LeastLoadedNodeProvider() {
        }

        @Override
        public Node provide() {
            return MyKafkaAdminClient.this.client.leastLoadedNode(MyKafkaAdminClient.this.time.milliseconds());
        }
    }

    private class ControllerNodeProvider implements MyKafkaAdminClient.NodeProvider {
        private ControllerNodeProvider() {
        }

        @Override
        public Node provide() {
            return MyKafkaAdminClient.this.metadata.fetch().controller();
        }
    }

    private class ConstantNodeIdProvider implements MyKafkaAdminClient.NodeProvider {
        private final int nodeId;

        ConstantNodeIdProvider(int nodeId) {
            this.nodeId = nodeId;
        }

        @Override
        public Node provide() {
            return MyKafkaAdminClient.this.metadata.fetch().nodeById(this.nodeId);
        }
    }

    private interface NodeProvider {
        Node provide();
    }

    // ----------------------------------------------------------------------------------
    //                                          自定义方法
    // ----------------------------------------------------------------------------------

    /**
     * 获取所有分组
     *
     * @param options
     * @return
     */
    MyListConsumerGroupsResult listConsumerGroups(MyListConsumerGroupsOptions options) {
        final KafkaFutureImpl<Collection<Object>> all = new KafkaFutureImpl();
        long nowMetadata = this.time.milliseconds();
        final long deadline = this.calcDeadlineMs(nowMetadata, options.timeoutMs());
        this.runnable.call(new MyKafkaAdminClient.Call("findAllBrokers", deadline, new MyKafkaAdminClient.LeastLoadedNodeProvider()) {
            @Override
            AbstractRequest.Builder createRequest(int timeoutMs) {
                return new org.apache.kafka.common.requests.MetadataRequest.Builder(Collections.emptyList(), true);
            }

            @Override
            void handleResponse(AbstractResponse abstractResponse) {
                MetadataResponse metadataResponse = (MetadataResponse)abstractResponse;
                Collection<Node> nodes = metadataResponse.brokers();
                if (nodes.isEmpty()) {
                    throw new KafkaException("Metadata fetch failed due to missing broker list");
                } else {
                    HashSet<Node> allNodes = new HashSet(nodes);
                    final MyKafkaAdminClient.ListConsumerGroupsResults results = new MyKafkaAdminClient.ListConsumerGroupsResults(allNodes, all);
                    Iterator var6 = allNodes.iterator();

                    while(var6.hasNext()) {
                        final Node node = (Node)var6.next();
                        long nowList = MyKafkaAdminClient.this.time.milliseconds();
                        MyKafkaAdminClient.this.runnable.call(new MyKafkaAdminClient.Call("listConsumerGroups", deadline, MyKafkaAdminClient.this.new ConstantNodeIdProvider(node.id())) {
                            @Override
                            AbstractRequest.Builder createRequest(int timeoutMs) {
                                return new org.apache.kafka.common.requests.ListGroupsRequest.Builder();
                            }

                            private void maybeAddConsumerGroup(ListGroupsResponse.Group group) {
                                String protocolType = group.protocolType();
                                if (protocolType.equals("consumer") || protocolType.isEmpty()) {
                                    String groupId = group.groupId();
                                    MyConsumerGroupListing groupListing = new MyConsumerGroupListing(groupId, protocolType.isEmpty());
                                    results.addListing(groupListing);
                                }
                            }

                            @Override
                            void handleResponse(AbstractResponse abstractResponse) {
                                ListGroupsResponse response = (ListGroupsResponse)abstractResponse;
                                synchronized(results) {
                                    Errors error = Errors.forCode(response.error().code());
                                    if (error != Errors.COORDINATOR_LOAD_IN_PROGRESS && error != Errors.COORDINATOR_NOT_AVAILABLE) {
                                        if (error != Errors.NONE) {
                                            results.addError(error.exception(), node);
                                        } else {
                                            Iterator groupIterator = response.groups().iterator();

                                            while(groupIterator.hasNext()) {
                                                ListGroupsResponse.Group group = (ListGroupsResponse.Group) groupIterator.next();
                                                this.maybeAddConsumerGroup(group);
                                            }
                                        }

                                        results.tryComplete(node);
                                    } else {
                                        throw error.exception();
                                    }
                                }
                            }

                            @Override
                            void handleFailure(Throwable throwable) {
                                synchronized(results) {
                                    results.addError(throwable, node);
                                    results.tryComplete(node);
                                }
                            }
                        }, nowList);
                    }

                }
            }

            @Override
            void handleFailure(Throwable throwable) {
                KafkaException exception = new KafkaException("Failed to find brokers to send ListGroups", throwable);
                all.complete(Collections.singletonList(exception));
            }
        }, nowMetadata);
        return new MyListConsumerGroupsResult(all);
    }

    public MyListConsumerGroupsResult listConsumerGroups() {
        return this.listConsumerGroups(new MyListConsumerGroupsOptions());
    }

    private static final class ListConsumerGroupsResults {
        private final List<Throwable> errors = new ArrayList();
        private final HashMap<String, MyConsumerGroupListing> listings = new HashMap();
        private final HashSet<Node> remaining;
        private final KafkaFutureImpl<Collection<Object>> future;

        ListConsumerGroupsResults(Collection<Node> leaders, KafkaFutureImpl<Collection<Object>> future) {
            this.remaining = new HashSet(leaders);
            this.future = future;
            this.tryComplete();
        }

        synchronized void addError(Throwable throwable, Node node) {
            ApiError error = ApiError.fromThrowable(throwable);
            if (error.message() != null && !error.message().isEmpty()) {
                this.errors.add(error.error().exception("Error listing groups on " + node + ": " + error.message()));
            } else {
                this.errors.add(error.error().exception("Error listing groups on " + node));
            }

        }

        synchronized void addListing(MyConsumerGroupListing listing) {
            this.listings.put(listing.groupId(), listing);
        }

        synchronized void tryComplete(Node leader) {
            this.remaining.remove(leader);
            this.tryComplete();
        }

        private synchronized void tryComplete() {
            if (this.remaining.isEmpty()) {
                ArrayList<Object> results = new ArrayList(this.listings.values());
                results.addAll(this.errors);
                this.future.complete(results);
            }

        }
    }

    private static final class ConsumerGroupOperationContext<T, O extends AbstractOptions<O>> {
        private final String groupId;
        private final O options;
        private final long deadline;
        private final KafkaFutureImpl<T> future;
        private Optional<Node> node;

        public ConsumerGroupOperationContext(String groupId, O options, long deadline, KafkaFutureImpl<T> future) {
            this.groupId = groupId;
            this.options = options;
            this.deadline = deadline;
            this.future = future;
            this.node = Optional.empty();
        }

        public String getGroupId() {
            return this.groupId;
        }

        public O getOptions() {
            return this.options;
        }

        public long getDeadline() {
            return this.deadline;
        }

        public KafkaFutureImpl<T> getFuture() {
            return this.future;
        }

        public Optional<Node> getNode() {
            return this.node;
        }

        public void setNode(Node node) {
            this.node = Optional.ofNullable(node);
        }

        public boolean hasCoordinatorMoved(AbstractResponse response) {
            return response.errorCounts().keySet().stream().anyMatch((error) -> {
                return error == Errors.NOT_COORDINATOR;
            });
        }
    }

    /**
     * 获取分组消费位移
     * @param groupId
     * @return
     */
    public MyListConsumerGroupOffsetsResult listConsumerGroupOffsets(String groupId) {
        return this.listConsumerGroupOffsets(groupId, new MyListConsumerGroupOffsetsOptions());
    }

    public MyListConsumerGroupOffsetsResult listConsumerGroupOffsets(String groupId, MyListConsumerGroupOffsetsOptions options) {
        KafkaFutureImpl<Map<TopicPartition, OffsetAndMetadata>> groupOffsetListingFuture = new KafkaFutureImpl();
        long startFindCoordinatorMs = this.time.milliseconds();
        long deadline = this.calcDeadlineMs(startFindCoordinatorMs, options.timeoutMs());
        MyKafkaAdminClient.ConsumerGroupOperationContext<Map<TopicPartition, OffsetAndMetadata>, MyListConsumerGroupOffsetsOptions> context = new MyKafkaAdminClient.ConsumerGroupOperationContext(groupId, options, deadline, groupOffsetListingFuture);
        MyKafkaAdminClient.Call findCoordinatorCall = this.getFindCoordinatorCall(context, () -> {
            return this.getListConsumerGroupOffsetsCall(context);
        });
        this.runnable.call(findCoordinatorCall, startFindCoordinatorMs);
        return new MyListConsumerGroupOffsetsResult(groupOffsetListingFuture);
    }

    private MyKafkaAdminClient.Call getListConsumerGroupOffsetsCall(final MyKafkaAdminClient.ConsumerGroupOperationContext<Map<TopicPartition, OffsetAndMetadata>, MyListConsumerGroupOffsetsOptions> context) {
        return new MyKafkaAdminClient.Call("listConsumerGroupOffsets", context.getDeadline(), new MyKafkaAdminClient.ConstantNodeIdProvider(((Node)context.getNode().get()).id())) {
            @Override
            AbstractRequest.Builder createRequest(int timeoutMs) {
                return new org.apache.kafka.common.requests.OffsetFetchRequest.Builder(context.getGroupId(), (context.getOptions()).topicPartitions());
            }

            @Override
            void handleResponse(AbstractResponse abstractResponse) {
                OffsetFetchResponse response = (OffsetFetchResponse)abstractResponse;
                Map<TopicPartition, OffsetAndMetadata> groupOffsetsListing = new HashMap();
                if (context.hasCoordinatorMoved(response)) {
                    MyKafkaAdminClient.this.rescheduleTask(context, () -> {
                        return MyKafkaAdminClient.this.getListConsumerGroupOffsetsCall(context);
                    });
                } else if (!MyKafkaAdminClient.this.handleGroupRequestError(response.error(), context.getFuture())) {
                    Iterator var4 = response.responseData().entrySet().iterator();

                    while(var4.hasNext()) {
                        Map.Entry<TopicPartition, OffsetFetchResponse.PartitionData> entry = (Map.Entry)var4.next();
                        TopicPartition topicPartition = entry.getKey();
                        OffsetFetchResponse.PartitionData partitionData = entry.getValue();
                        Errors error = partitionData.error;
                        if (error == Errors.NONE) {
                            Long offset = partitionData.offset;
                            String metadata = partitionData.metadata;
                            if (offset < 0L) {
                                groupOffsetsListing.put(topicPartition, null);
                            } else {
                                groupOffsetsListing.put(topicPartition, new OffsetAndMetadata(offset, metadata));
                            }
                        } else {
                            log.warn("Skipping return offset for {} due to error {}.", topicPartition, error);
                        }
                    }

                    context.getFuture().complete(groupOffsetsListing);
                }
            }

            @Override
            void handleFailure(Throwable throwable) {
                context.getFuture().completeExceptionally(throwable);
            }
        };
    }

    private <T, O extends AbstractOptions<O>> MyKafkaAdminClient.Call getFindCoordinatorCall(final MyKafkaAdminClient.ConsumerGroupOperationContext<T, O> context, final Supplier<MyKafkaAdminClient.Call> nextCall) {
        return new MyKafkaAdminClient.Call("findCoordinator", context.getDeadline(), new MyKafkaAdminClient.LeastLoadedNodeProvider()) {
            @Override
            org.apache.kafka.common.requests.FindCoordinatorRequest.Builder createRequest(int timeoutMs) {
                return new org.apache.kafka.common.requests.FindCoordinatorRequest.Builder(FindCoordinatorRequest.CoordinatorType.GROUP, context.getGroupId());
            }

            @Override
            void handleResponse(AbstractResponse abstractResponse) {
                FindCoordinatorResponse response = (FindCoordinatorResponse)abstractResponse;
                if (!MyKafkaAdminClient.this.handleGroupRequestError(response.error(), context.getFuture())) {
                    context.setNode(response.node());
                    MyKafkaAdminClient.this.runnable.call((MyKafkaAdminClient.Call)nextCall.get(), MyKafkaAdminClient.this.time.milliseconds());
                }
            }

            @Override
            void handleFailure(Throwable throwable) {
                context.getFuture().completeExceptionally(throwable);
            }
        };
    }

    private boolean handleGroupRequestError(Errors error, KafkaFutureImpl<?> future) {
        if (error != Errors.COORDINATOR_LOAD_IN_PROGRESS && error != Errors.COORDINATOR_NOT_AVAILABLE) {
            if (error != Errors.NONE) {
                future.completeExceptionally(error.exception());
                return true;
            } else {
                return false;
            }
        } else {
            throw error.exception();
        }
    }

    private void rescheduleTask(MyKafkaAdminClient.ConsumerGroupOperationContext<?, ?> context, Supplier<MyKafkaAdminClient.Call> nextCall) {
        log.info("Node {} is no longer the Coordinator. Retrying with new coordinator.", context.getNode().orElse(null));
        context.setNode((Node)null);
        MyKafkaAdminClient.Call findCoordinatorCall = this.getFindCoordinatorCall(context, nextCall);
        this.runnable.call(findCoordinatorCall, this.time.milliseconds());
    }
}
