package com.meiya.whalex.db.entity.graph.rawstatement;

import com.meiya.whalex.db.entity.table.rawstatement.nebulagraph.NebulaGraphTime;
import com.meiya.whalex.db.entity.table.rawstatement.nebulagraph.NebulaGraphTimeWrapper;
import com.vesoft.nebula.Time;
import com.vesoft.nebula.client.graph.data.TimeWrapper;

/**
 * @author 黄河森
 * @date 2024/3/15
 * @package com.meiya.whalex.db.entity.graph.rawstatement
 * @project whalex-data-driver
 * @description NebulaGraphTimeWrapperImpl
 */
public class NebulaGraphTimeWrapperImpl implements NebulaGraphTimeWrapper {

    private final TimeWrapper timeWrapper;

    public NebulaGraphTimeWrapperImpl(TimeWrapper timeWrapper) {
        this.timeWrapper = timeWrapper;
    }

    @Override
    public String getDecodeType() {
        return timeWrapper.getDecodeType();
    }

    @Override
    public int getTimezoneOffset() {
        return timeWrapper.getTimezoneOffset();
    }

    @Override
    public byte getHour() {
        return timeWrapper.getHour();
    }

    @Override
    public byte getMinute() {
        return timeWrapper.getMinute();
    }

    @Override
    public byte getSecond() {
        return timeWrapper.getSecond();
    }

    @Override
    public int getMicrosec() {
        return timeWrapper.getMicrosec();
    }

    @Override
    public NebulaGraphTime getLocalTime() {
        Time localTime = timeWrapper.getLocalTime();
        return new NebulaGraphTimeImpl(localTime);
    }

    @Override
    public NebulaGraphTime getTimeWithTimezoneOffset(int timezoneOffset) {
        return new NebulaGraphTimeImpl(timeWrapper.getTimeWithTimezoneOffset(timezoneOffset));
    }

    @Override
    public String getLocalTimeStr() {
        return timeWrapper.getLocalTimeStr();
    }

    @Override
    public String getUTCTimeStr() {
        return timeWrapper.getUTCTimeStr();
    }
}
