package com.meiya.whalex.db.entity.graph.rawstatement;

import com.meiya.whalex.db.entity.table.rawstatement.nebulagraph.NebulaGraphTime;
import com.vesoft.nebula.Time;

/**
 * @author 黄河森
 * @date 2024/3/15
 * @package com.meiya.whalex.db.entity.graph.rawstatement
 * @project whalex-data-driver
 * @description NebulaGraphTimeImpl
 */
public class NebulaGraphTimeImpl implements NebulaGraphTime {

    private final Time time;

    public Time getTime() {
        return time;
    }

    public NebulaGraphTimeImpl(Time time) {
        this.time = time;
    }

    @Override
    public NebulaGraphTime deepCopy() {
        return new NebulaGraphTimeImpl(time.deepCopy());
    }

    @Override
    public byte getHour() {
        return time.getHour();
    }

    @Override
    public boolean isSetHour() {
        return time.isSetHour();
    }

    @Override
    public byte getMinute() {
        return time.getMinute();
    }

    @Override
    public boolean isSetMinute() {
        return time.isSetMinute();
    }

    @Override
    public byte getSec() {
        return time.getSec();
    }

    @Override
    public boolean isSetSec() {
        return time.isSetSec();
    }

    @Override
    public int getMicrosec() {
        return time.getMicrosec();
    }

    @Override
    public boolean isSetMicrosec() {
        return time.isSetMicrosec();
    }

    @Override
    public Object getFieldValue(int fieldID) {
        return time.getFieldValue(fieldID);
    }

    @Override
    public int compareTo(NebulaGraphTime o) {
        return time.compareTo(((NebulaGraphTimeImpl) o).getTime());
    }
}
