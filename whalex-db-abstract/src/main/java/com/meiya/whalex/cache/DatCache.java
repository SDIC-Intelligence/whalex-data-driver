package com.meiya.whalex.cache;

import cn.hutool.core.collection.ConcurrentHashSet;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Policy;
import com.github.benmanes.caffeine.cache.stats.CacheStats;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * @author 黄河森
 * @date 2022/5/16
 * @package com.meiya.whalex
 * @project whalex-db-relyon-demo
 */
public class DatCache<K, V> implements Cache<K, V> {

    private Cache<K, V> cache;

    private Set<K> keys = new ConcurrentHashSet<>();

    private ScheduledThreadPoolExecutor scheduledThreadPoolExecutor;

    public DatCache(Cache<K, V> cache, long duration, TimeUnit unit) {
        this.cache = cache;
        // 如果为负数，说明没有过期时间，不需要开启定时任务
        if (duration > 0) {
            startScheduled(duration, unit);
        }
    }

    private void startScheduled(long duration, TimeUnit unit) {
        long delay = duration;
        switch (unit) {
            case SECONDS:
                if (duration < 60) {
                    delay = 1;
                } else {
                    long start = duration / 60;
                    delay = start + 1;
                }
                break;
            case MILLISECONDS:
                if (duration < (60 * 1000)) {
                    delay = 1;
                } else {
                    long start = duration / (60 * 1000);
                    delay = start + 1;
                }
                break;
            case MICROSECONDS:
                if (duration < (60 * 1000 * 1000)) {
                    delay = 1;
                } else {
                    long start = duration / (60 * 1000 * 1000);
                    delay = start + 1;
                }
                break;
            case NANOSECONDS:
                if (duration < (60 * 1000 * 1000 * 1000)) {
                    delay = 1;
                } else {
                    long start = duration / (60 * 1000 * 1000 * 1000);
                    delay = start + 1;
                }
                break;
            case MINUTES:
                delay = duration + 1;
                break;
            case HOURS:
                delay = (duration * 60) + 10;
                break;
            case DAYS:
                delay = (duration * 60 * 24) + 10;
                break;
        }
        scheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(1);
        scheduledThreadPoolExecutor.scheduleAtFixedRate(() -> {
            // 遍历缓存key,去触发过期处理逻辑
            Set<K> temp = new ConcurrentHashSet<>();
            for (K key : keys) {
                V o = cache.getIfPresent(key);
                if (o == null) {
                    temp.add(key);
                }
            }
            // 删除已经过期的key
            keys.removeAll(temp);
        }, delay, delay, TimeUnit.MINUTES);
    }

    @Override
    public V getIfPresent(Object o) {
        return cache.getIfPresent(o);
    }

    @Override
    public V get(K k, Function<? super K, ? extends V> function) {
        V v = cache.get(k, function);
        if (!keys.contains(k)) {
            keys.add(k);
        }
        return v;
    }

    @Override
    public Map<K, V> getAllPresent(Iterable<?> iterable) {
        return cache.getAllPresent(iterable);
    }

    @Override
    public void put(K k, V v) {
        cache.put(k, v);
        keys.add(k);
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map) {
        cache.putAll(map);
        keys.addAll(map.keySet());
    }

    @Override
    public void invalidate(Object o) {
        cache.invalidate(o);
        keys.remove(o);
    }

    @Override
    public void invalidateAll(Iterable<?> iterable) {

        Set<Object> cacheSet = new HashSet<>();
        Set<Object> keysSet = new HashSet<>();
        Iterator<?> iterator = iterable.iterator();
        while (iterator.hasNext()) {
            Object next = iterator.next();
            cacheSet.add(next);
            keysSet.add(next);
        }
        cache.invalidateAll(cacheSet);
        keys.removeAll(keysSet);
    }

    @Override
    public void invalidateAll() {
        cache.invalidateAll();
        keys.clear();
    }

    @Override
    public long estimatedSize() {
        return cache.estimatedSize();
    }

    @Override
    public CacheStats stats() {
        return cache.stats();
    }

    @Override
    public ConcurrentMap<K, V> asMap() {
        return cache.asMap();
    }

    @Override
    public void cleanUp() {
        cache.cleanUp();
        keys.clear();
    }

    @Override
    public Policy<K, V> policy() {
        return cache.policy();
    }

    @Override
    protected void finalize() throws Throwable {
        scheduledThreadPoolExecutor.shutdownNow();
        super.finalize();
    }
}
