package com.meiya.whalex.util.concurrent;

import cn.hutool.core.date.DateUtil;
import cn.hutool.core.map.MapUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.*;

/**
 * 监控线程池统一创建工具类
 *
 * @author 黄河森
 * @date 2019/11/22
 * @project whale-cloud-platformX
 */
@Slf4j(topic = "monitorExecutor")
public class MonitorExecutor {

    /**
     * 定时任务线程，定时打印所有线程池状态
     */
    private static final ScheduledExecutorService SCHEDULED;

    /**
     * 缓存注册的线程池
     */
    private static final Map<String, ThreadPoolExecutor> CACHE_THREAD_POOL_EXECUTOR;

    /**
     * 监控任务周期 s
     */
    private static final long MONITOR_PERMITS_PERIOD = 30L;

    /**
     * 定时线程池监控日志输出格式
     */
    private static final String THREAD_POOL_FORMAT;

    static {
        THREAD_POOL_FORMAT = "threadPoolName: [%s], activeThread: [%s], currentThread: [%s], corePool: [%s], maximumPool: [%s], workQueue: [%s]";
        SCHEDULED = new ScheduledThreadPoolExecutor(1, new ThreadNamedFactory("monitor-thread-pool-status"));
        CACHE_THREAD_POOL_EXECUTOR = new ConcurrentHashMap<>();
        startMonitorExecutor();
    }

    /**
     * 创建 缓存线程池
     *
     * @param corePoolSize
     * @param maximumPoolSize
     * @param keepAliveTime
     * @param unit
     * @param threadFactory
     * @param handler
     * @return
     */
    public static ThreadPoolExecutor newCachedThreadPool(Integer corePoolSize,
                                                         Integer maximumPoolSize,
                                                         Long keepAliveTime,
                                                         TimeUnit unit,
                                                         ThreadNamedFactory threadFactory,
                                                         RejectedExecutionHandler handler) {
        if (corePoolSize == null) {
            corePoolSize = 0;
        }
        if (keepAliveTime == null) {
            keepAliveTime = 60L;
            unit = TimeUnit.SECONDS;
        }
        return newThreadPool(corePoolSize, maximumPoolSize, keepAliveTime, unit, new SynchronousQueue<Runnable>(), threadFactory, handler);
    }

    /**
     * 创建线程池，并注册到监控任务中
     *
     * @param corePoolSize
     * @param maximumPoolSize
     * @param keepAliveTime
     * @param unit
     * @param workQueue
     * @param threadFactory
     * @param handler
     * @return
     */
    public static ThreadPoolExecutor newThreadPool(Integer corePoolSize,
                                                   Integer maximumPoolSize,
                                                   Long keepAliveTime,
                                                   TimeUnit unit,
                                                   BlockingQueue<Runnable> workQueue,
                                                   ThreadNamedFactory threadFactory,
                                                   RejectedExecutionHandler handler) {

        if (corePoolSize == null || maximumPoolSize == null || threadFactory == null) {
            throw new IllegalArgumentException("create thread pool need corePoolSize and maximumPoolSize and threadFactory");
        }

        if (keepAliveTime == null) {
            keepAliveTime = 0L;
            unit = TimeUnit.MILLISECONDS;
        }

        if (unit == null) {
            unit = TimeUnit.MILLISECONDS;
        }

        if (workQueue == null) {
            workQueue = new LinkedBlockingDeque(50);
        }

        ThreadPoolExecutor threadPoolExecutor;
        if (handler == null) {
            threadPoolExecutor = new ThreadPoolExecutor(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
        } else {
            threadPoolExecutor = new ThreadPoolExecutor(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, handler);
        }
        CACHE_THREAD_POOL_EXECUTOR.put(threadFactory.getPrefix(), threadPoolExecutor);
        return threadPoolExecutor;
    }

    /**
     * 开启线程池监控
     */
    private static void startMonitorExecutor() {
        SCHEDULED.scheduleAtFixedRate(() -> {
            if (MapUtil.isNotEmpty(CACHE_THREAD_POOL_EXECUTOR)) {
                StringBuffer logBuffer = new StringBuffer();
                logBuffer.append("date: [{}] thread pool status: {\n");
                Iterator<Map.Entry<String, ThreadPoolExecutor>> iter = CACHE_THREAD_POOL_EXECUTOR.entrySet().iterator();
                while (iter.hasNext()) {
                    Map.Entry<String, ThreadPoolExecutor> entry = iter.next();
                    String threadName = entry.getKey();
                    ThreadPoolExecutor executorService = entry.getValue();
                    String threadPoolLog = String.format(THREAD_POOL_FORMAT,
                            threadName,
                            executorService.getActiveCount(),
                            executorService.getPoolSize(),
                            executorService.getCorePoolSize(),
                            executorService.getMaximumPoolSize(),
                            executorService.getQueue().size());
                    logBuffer.append(threadPoolLog).append("\n");
                }
                logBuffer.append("};");
                log.info(logBuffer.toString(), DateUtil.now());
            }
        }, 0, MONITOR_PERMITS_PERIOD, TimeUnit.SECONDS);
    }

    /**
     * 注册线程池
     *
     * @param threadPoolName
     * @param threadPoolExecutor
     */
    public static void registerExecutor(String threadPoolName ,ThreadPoolExecutor threadPoolExecutor) {
        if (StringUtils.isBlank(threadPoolName) || threadPoolExecutor == null) {
            throw new IllegalArgumentException("register thread pool need threadPoolName and threadPoolExecutor");
        }
        CACHE_THREAD_POOL_EXECUTOR.put(threadPoolName, threadPoolExecutor);
    }


}
