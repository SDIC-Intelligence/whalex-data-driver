package com.meiya.whalex.db.entity.graph;

import com.meiya.whalex.graph.entity.*;

import java.util.List;

/**
 * @author 黄河森
 * @date 2024/3/8
 * @package com.meiya.whalex.db.entity.graph
 * @project whalex-data-driver
 * @description NebulaGraphQlRecord
 */
public class NebulaGraphQlRecord extends QlInternalRecord {

    public NebulaGraphQlRecord(List<String> keys, QlValue[] values) {
        super(keys, values);
    }
}
