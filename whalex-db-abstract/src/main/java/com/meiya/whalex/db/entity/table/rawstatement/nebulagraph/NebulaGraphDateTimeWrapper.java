package com.meiya.whalex.db.entity.table.rawstatement.nebulagraph;

/**
 * @author 黄河森
 * @date 2024/3/12
 * @package com.meiya.whalex.db.entity.table.rawstatement.nebulagraph
 * @project whalex-data-driver
 * @description NebulaGraphDateTimeWrapper
 */
public interface NebulaGraphDateTimeWrapper extends NebulaGraphBaseDataObject {

    short getYear() throws Exception;

    byte getMonth() throws Exception;

    byte getDay() throws Exception;

    byte getHour() throws Exception;

    byte getMinute() throws Exception;

    byte getSecond() throws Exception;

    int getMicrosec() throws Exception;

    String getLocalDateTimeStr();

    String getUTCDateTimeStr();
}
