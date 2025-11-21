package com.meiya.whalex.db.entity.table.rawstatement.nebulagraph;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;

/**
 * @author 黄河森
 * @date 2024/3/12
 * @package com.meiya.whalex.db.entity.table.rawstatement.nebulagraph
 * @project whalex-data-driver
 * @description NebulaGraphRelationship
 */
public interface NebulaGraphRelationship extends NebulaGraphBaseDataObject {

    NebulaGraphValueWrapper srcId();

    NebulaGraphValueWrapper dstId();

    String edgeName();

    long ranking();

    List<String> keys() throws UnsupportedEncodingException;

    List<NebulaGraphValueWrapper> values();

    HashMap<String, NebulaGraphValueWrapper> properties() throws UnsupportedEncodingException;

}
