package com.meiya.whalex.db.entity.table.rawstatement.nebulagraph;

/**
 * @author 黄河森
 * @date 2024/3/12
 * @package com.meiya.whalex.db.entity.table.rawstatement.nebulagraph
 * @project whalex-data-driver
 * @description NebulaGraphDurationWrapper
 */
public interface NebulaGraphDurationWrapper extends NebulaGraphBaseDataObject {

    long getSeconds();

    int getMicroseconds();

    int getMonths();

    String getDurationString();

}
