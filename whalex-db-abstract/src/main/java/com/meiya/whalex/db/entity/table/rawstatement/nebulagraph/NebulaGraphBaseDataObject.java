package com.meiya.whalex.db.entity.table.rawstatement.nebulagraph;

import java.io.Serializable;

/**
 * @author 黄河森
 * @date 2024/3/12
 * @package com.meiya.whalex.db.entity.table.rawstatement.nebulagraph
 * @project whalex-data-driver
 * @description NebulaGraphBaseDataObject
 */
public interface NebulaGraphBaseDataObject extends Serializable {

    String getDecodeType();

    int getTimezoneOffset();
}
