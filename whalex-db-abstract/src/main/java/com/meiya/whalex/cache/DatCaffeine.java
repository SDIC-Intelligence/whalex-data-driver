package com.meiya.whalex.cache;

import com.github.benmanes.caffeine.cache.*;
import com.github.benmanes.caffeine.cache.stats.StatsCounter;

import java.time.Duration;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * @author 黄河森
 * @date 2022/5/16
 * @package com.meiya.whalex
 * @project whalex-db-relyon-demo
 */
public class DatCaffeine<K, V> {

    private Caffeine<K, V> caffeine;

    // 过期时间
    private long duration = -1L;

    // 单位
    private TimeUnit unit = TimeUnit.NANOSECONDS;

    private DatCaffeine() {
        caffeine = (Caffeine<K, V>) Caffeine.newBuilder();
    }
    
    public static <K, V> DatCaffeine<K, V> newBuilder() {
        return new DatCaffeine<>();
    }
    
    public DatCaffeine<K, V> initialCapacity(int initialCapacity) {
        caffeine.initialCapacity(initialCapacity);
        return this;
    }
    
    public DatCaffeine<K, V> executor(Executor executor) {
        caffeine.executor(executor);
        return this;
    }

    public DatCaffeine<K, V> maximumSize(long maximumSize) {
        caffeine.maximumSize(maximumSize);
        return this;
    }

    public DatCaffeine<K, V> maximumWeight(long maximumWeight) {
        caffeine.maximumWeight(maximumWeight);
        return this;
    }

    public <K1 extends K, V1 extends V> DatCaffeine<K1, V1> weigher(Weigher<? super K1, ? super V1> weigher) {
        caffeine.weigher(weigher);
        return (DatCaffeine<K1, V1>) this;
    }

    public DatCaffeine<K, V> weakKeys() {
        caffeine.weakKeys();
        return this;
    }

    public DatCaffeine<K, V> weakValues() {
        caffeine.weakValues();
        return this;
    }

    public DatCaffeine<K, V> softValues() {
        caffeine.softValues();
        return this;
    }

    public DatCaffeine<K, V> expireAfterWrite(Duration duration) {
        this.duration = duration.toNanos();
        this.unit = TimeUnit.NANOSECONDS;
        caffeine.expireAfterWrite(duration);
        return this;
    }

    public DatCaffeine<K, V> expireAfterWrite(long duration, TimeUnit unit) {
        this.duration = duration;
        this.unit = unit;
        caffeine.expireAfterWrite(duration, unit);
        return this;
    }

    public DatCaffeine<K, V> expireAfterAccess(Duration duration) {
        this.duration = duration.toNanos();
        this.unit = TimeUnit.NANOSECONDS;
        caffeine.expireAfterAccess(duration);
        return this;
    }

    public DatCaffeine<K, V> expireAfterAccess(long duration, TimeUnit unit) {
        this.duration = duration;
        this.unit = unit;
        caffeine.expireAfterAccess(duration, unit);
        return this;
    }

    public <K1 extends K, V1 extends V> DatCaffeine<K1, V1> expireAfter(Expiry<? super K1, ? super V1> expiry) {
        caffeine.expireAfter(expiry);
        return (DatCaffeine<K1, V1>) this;
    }

    public DatCaffeine<K, V> refreshAfterWrite(Duration duration) {
        this.duration = duration.toNanos();
        this.unit = TimeUnit.NANOSECONDS;
        caffeine.refreshAfterWrite(duration);
        return this;
    }

    public DatCaffeine<K, V> refreshAfterWrite(long duration, TimeUnit unit) {
        this.duration = duration;
        this.unit = unit;
        caffeine.refreshAfterWrite(duration, unit);
        return this;
    }

    public DatCaffeine<K, V> ticker(Ticker ticker) {
        caffeine.ticker(ticker);
        return this;
    }

    public <K1 extends K, V1 extends V> DatCaffeine<K1, V1> removalListener(RemovalListener<? super K1, ? super V1> removalListener) {
        caffeine.removalListener(removalListener);
        return (DatCaffeine<K1, V1>) this;
    }

    public <K1 extends K, V1 extends V> DatCaffeine<K1, V1> writer(CacheWriter<? super K1, ? super V1> writer) {
        caffeine.writer(writer);
        return (DatCaffeine<K1, V1>) this;
    }

    public DatCaffeine<K, V> recordStats() {
        caffeine.recordStats();
        return this;
    }

    public DatCaffeine<K, V> recordStats(Supplier<? extends StatsCounter> statsCounterSupplier) {
        caffeine.recordStats(statsCounterSupplier);
        return this;
    }

    public <K1 extends K, V1 extends V> Cache<K1, V1> build() {
        Cache<K1, V1> cache = caffeine.build();
        return new DatCache<>(cache, duration, unit);
    }

}
