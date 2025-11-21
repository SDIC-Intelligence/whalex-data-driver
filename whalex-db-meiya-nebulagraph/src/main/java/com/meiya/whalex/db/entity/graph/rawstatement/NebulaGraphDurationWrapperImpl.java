package com.meiya.whalex.db.entity.graph.rawstatement;

import com.meiya.whalex.db.entity.table.rawstatement.nebulagraph.NebulaGraphDurationWrapper;
import com.vesoft.nebula.client.graph.data.DurationWrapper;

/**
 * @author 黄河森
 * @date 2024/3/15
 * @package com.meiya.whalex.db.entity.graph.rawstatement
 * @project whalex-data-driver
 * @description NebulaGraphDurationWrapperImpl
 */
public class NebulaGraphDurationWrapperImpl implements NebulaGraphDurationWrapper {

    private final DurationWrapper durationWrapper;

    public NebulaGraphDurationWrapperImpl(DurationWrapper durationWrapper) {
        this.durationWrapper = durationWrapper;
    }

    @Override
    public String getDecodeType() {
        return durationWrapper.getDecodeType();
    }

    @Override
    public int getTimezoneOffset() {
        return durationWrapper.getTimezoneOffset();
    }

    @Override
    public long getSeconds() {
        return durationWrapper.getSeconds();
    }

    @Override
    public int getMicroseconds() {
        return durationWrapper.getMicroseconds();
    }

    @Override
    public int getMonths() {
        return durationWrapper.getMonths();
    }

    @Override
    public String getDurationString() {
        return durationWrapper.getDurationString();
    }
}
