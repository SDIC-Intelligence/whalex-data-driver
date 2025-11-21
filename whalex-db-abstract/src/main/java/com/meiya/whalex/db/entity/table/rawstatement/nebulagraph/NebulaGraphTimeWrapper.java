package com.meiya.whalex.db.entity.table.rawstatement.nebulagraph;

/**
 * @author 黄河森
 * @date 2024/3/12
 * @package com.meiya.whalex.db.entity.table.rawstatement.nebulagraph
 * @project whalex-data-driver
 * @description NebulaGraphTimeWrapper
 */
public interface NebulaGraphTimeWrapper extends NebulaGraphBaseDataObject {

    byte getHour();

    byte getMinute();

    byte getSecond();

    int getMicrosec();

    NebulaGraphTime getLocalTime();

    NebulaGraphTime getTimeWithTimezoneOffset(int timezoneOffset);

    String getLocalTimeStr();

    String getUTCTimeStr();
}
