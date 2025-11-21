package com.meiya.whalex.db.entity.graph.rawstatement;

import com.meiya.whalex.db.entity.table.rawstatement.nebulagraph.NebulaGraphDateWrapper;
import com.vesoft.nebula.client.graph.data.DateWrapper;

/**
 * @author 黄河森
 * @date 2024/3/15
 * @package com.meiya.whalex.db.entity.graph.rawstatement
 * @project whalex-data-driver
 * @description NebulaGraphDateWrapperImpl
 */
public class NebulaGraphDateWrapperImpl implements NebulaGraphDateWrapper {

    private final DateWrapper dateWrapper;

    public NebulaGraphDateWrapperImpl(DateWrapper dateWrapper) {
        this.dateWrapper = dateWrapper;
    }

    @Override
    public String getDecodeType() {
        return dateWrapper.getDecodeType();
    }

    @Override
    public int getTimezoneOffset() {
        return dateWrapper.getTimezoneOffset();
    }

    @Override
    public short getYear() {
        return dateWrapper.getYear();
    }

    @Override
    public byte getMonth() {
        return dateWrapper.getMonth();
    }

    @Override
    public byte getDay() {
        return dateWrapper.getDay();
    }
}
