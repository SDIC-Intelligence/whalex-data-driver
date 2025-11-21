package com.meiya.whalex.db.entity.file;

import lombok.Data;
import org.apache.hadoop.hive.ql.io.StatsProvidingRecordWriter;

/**
 * @author 黄河森
 * @date 2023/5/17
 * @package com.meiya.whalex.db.entity.file
 * @project whalex-data-driver
 */
@Data
public class OrcWriter<W extends StatsProvidingRecordWriter> {

    private W writer;

    private long dataSize = 0;

    private int statisticIndex = 0;

    public OrcWriter(W writer) {
        this.writer = writer;
    }

    public void increment() {
        statisticIndex++;
    }
}
