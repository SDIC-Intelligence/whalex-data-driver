package com.meiya.whalex.db.entity.table.rawstatement.nebulagraph;

import java.util.List;

/**
 * @author 黄河森
 * @date 2024/3/12
 * @package com.meiya.whalex.db.entity.table.rawstatement.nebulagraph
 * @project whalex-data-driver
 * @description NebulaGraphLineStringWrapper
 */
public interface NebulaGraphLineStringWrapper extends NebulaGraphBaseDataObject {

    List<NebulaGraphCoordinateWrapper> getCoordinateList();

}
