package com.meiya.whalex.graph.entity;

import java.time.Duration;
import java.time.Period;
import java.time.temporal.ChronoUnit;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalUnit;
import java.time.temporal.UnsupportedTemporalTypeException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * @author 黄河森
 * @date 2024/3/8
 * @package com.meiya.whalex.graph.entity
 * @project whalex-data-driver
 * @description QlInternalIsoDuration
 */
public class QlInternalIsoDuration implements QlIsoDuration {

    private static final long NANOS_PER_SECOND = 1000000000L;
    private static final List<TemporalUnit> SUPPORTED_UNITS;
    private final long months;
    private final long days;
    private final long seconds;
    private final int nanoseconds;

    public QlInternalIsoDuration(Period period) {
        this(period.toTotalMonths(), (long)period.getDays(), Duration.ZERO);
    }

    public QlInternalIsoDuration(Duration duration) {
        this(0L, 0L, duration);
    }

    public QlInternalIsoDuration(long months, long days, long seconds, int nanoseconds) {
        this(months, days, Duration.ofSeconds(seconds, (long)nanoseconds));
    }

    QlInternalIsoDuration(long months, long days, Duration duration) {
        this.months = months;
        this.days = days;
        this.seconds = duration.getSeconds();
        this.nanoseconds = duration.getNano();
    }

    public long months() {
        return this.months;
    }

    public long days() {
        return this.days;
    }

    public long seconds() {
        return this.seconds;
    }

    public int nanoseconds() {
        return this.nanoseconds;
    }

    public long get(TemporalUnit unit) {
        if (unit == ChronoUnit.MONTHS) {
            return this.months;
        } else if (unit == ChronoUnit.DAYS) {
            return this.days;
        } else if (unit == ChronoUnit.SECONDS) {
            return this.seconds;
        } else if (unit == ChronoUnit.NANOS) {
            return (long)this.nanoseconds;
        } else {
            throw new UnsupportedTemporalTypeException("Unsupported unit: " + unit);
        }
    }

    public List<TemporalUnit> getUnits() {
        return SUPPORTED_UNITS;
    }

    public Temporal addTo(Temporal temporal) {
        if (this.months != 0L) {
            temporal = temporal.plus(this.months, ChronoUnit.MONTHS);
        }

        if (this.days != 0L) {
            temporal = temporal.plus(this.days, ChronoUnit.DAYS);
        }

        if (this.seconds != 0L) {
            temporal = temporal.plus(this.seconds, ChronoUnit.SECONDS);
        }

        if (this.nanoseconds != 0) {
            temporal = temporal.plus((long)this.nanoseconds, ChronoUnit.NANOS);
        }

        return temporal;
    }

    public Temporal subtractFrom(Temporal temporal) {
        if (this.months != 0L) {
            temporal = temporal.minus(this.months, ChronoUnit.MONTHS);
        }

        if (this.days != 0L) {
            temporal = temporal.minus(this.days, ChronoUnit.DAYS);
        }

        if (this.seconds != 0L) {
            temporal = temporal.minus(this.seconds, ChronoUnit.SECONDS);
        }

        if (this.nanoseconds != 0) {
            temporal = temporal.minus((long)this.nanoseconds, ChronoUnit.NANOS);
        }

        return temporal;
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o != null && this.getClass() == o.getClass()) {
            QlInternalIsoDuration that = (QlInternalIsoDuration)o;
            return this.months == that.months && this.days == that.days && this.seconds == that.seconds && this.nanoseconds == that.nanoseconds;
        } else {
            return false;
        }
    }

    public int hashCode() {
        return Objects.hash(new Object[]{this.months, this.days, this.seconds, this.nanoseconds});
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append('P');
        sb.append(this.months).append('M');
        sb.append(this.days).append('D');
        sb.append('T');
        if (this.seconds < 0L && this.nanoseconds > 0) {
            if (this.seconds == -1L) {
                sb.append("-0");
            } else {
                sb.append(this.seconds + 1L);
            }
        } else {
            sb.append(this.seconds);
        }

        if (this.nanoseconds > 0) {
            int pos = sb.length();
            if (this.seconds < 0L) {
                sb.append(2000000000L - (long)this.nanoseconds);
            } else {
                sb.append(1000000000L + (long)this.nanoseconds);
            }

            sb.setCharAt(pos, '.');
        }

        sb.append('S');
        return sb.toString();
    }

    static {
        SUPPORTED_UNITS = Collections.unmodifiableList(Arrays.asList(ChronoUnit.MONTHS, ChronoUnit.DAYS, ChronoUnit.SECONDS, ChronoUnit.NANOS));
    }

}
