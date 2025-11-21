package com.meiya.whalex.db.entity.table.rawstatement.nebulagraph;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;

/**
 * @author 黄河森
 * @date 2024/3/12
 * @package com.meiya.whalex.db.entity.table.rawstatement.nebulagraph
 * @project whalex-data-driver
 * @description Node
 */
public interface NebulaGraphNode {

    NebulaGraphValueWrapper getId();

    List<String> tagNames();

    List<String> labels();

    boolean hasTagName(String tagName);

    boolean hasLabel(String tagName);

    List<NebulaGraphValueWrapper> values(String tagName);

    List<String> keys(String tagName) throws UnsupportedEncodingException;

    HashMap<String, NebulaGraphValueWrapper> properties(String tagName) throws UnsupportedEncodingException;

}
