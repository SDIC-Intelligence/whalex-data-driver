package com.meiya.whalex.db.entity.table.rawstatement.nebulagraph;

/**
 * @author 黄河森
 * @date 2024/3/12
 * @package com.meiya.whalex.db.entity.table.rawstatement.nebulagraph
 * @project whalex-data-driver
 * @description NebulaGraphTime
 */
public interface NebulaGraphTime extends Comparable<NebulaGraphTime> {

    NebulaGraphTime deepCopy();

    byte getHour();

    boolean isSetHour();

    byte getMinute();

    boolean isSetMinute();

    byte getSec();

    boolean isSetSec();

    int getMicrosec();

    boolean isSetMicrosec();

    Object getFieldValue(int fieldID);

}
