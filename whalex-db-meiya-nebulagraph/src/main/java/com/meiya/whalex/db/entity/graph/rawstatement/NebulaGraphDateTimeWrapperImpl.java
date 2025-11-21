package com.meiya.whalex.db.entity.graph.rawstatement;

import com.meiya.whalex.db.entity.table.rawstatement.nebulagraph.NebulaGraphDateTimeWrapper;
import com.vesoft.nebula.client.graph.data.DateTimeWrapper;

/**
 * @author 黄河森
 * @date 2024/3/15
 * @package com.meiya.whalex.db.entity.graph.rawstatement
 * @project whalex-data-driver
 * @description NebulaGraphDateTimeWrapperImpl
 */
public class NebulaGraphDateTimeWrapperImpl implements NebulaGraphDateTimeWrapper {

    private final DateTimeWrapper dateTimeWrapper;

    public NebulaGraphDateTimeWrapperImpl(DateTimeWrapper dateTimeWrapper) {
        this.dateTimeWrapper = dateTimeWrapper;
    }

    @Override
    public String getDecodeType() {
        return dateTimeWrapper.getDecodeType();
    }

    @Override
    public int getTimezoneOffset() {
        return dateTimeWrapper.getTimezoneOffset();
    }

    @Override
    public short getYear() throws Exception {
        return dateTimeWrapper.getYear();
    }

    @Override
    public byte getMonth() throws Exception {
        return dateTimeWrapper.getMonth();
    }

    @Override
    public byte getDay() throws Exception {
        return dateTimeWrapper.getDay();
    }

    @Override
    public byte getHour() throws Exception {
        return dateTimeWrapper.getHour();
    }

    @Override
    public byte getMinute() throws Exception {
        return dateTimeWrapper.getMinute();
    }

    @Override
    public byte getSecond() throws Exception {
        return dateTimeWrapper.getSecond();
    }

    @Override
    public int getMicrosec() throws Exception {
        return dateTimeWrapper.getMicrosec();
    }

    @Override
    public String getLocalDateTimeStr() {
        return dateTimeWrapper.getLocalDateTimeStr();
    }

    @Override
    public String getUTCDateTimeStr() {
        return dateTimeWrapper.getUTCDateTimeStr();
    }
}
