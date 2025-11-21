package com.meiya.whalex.db.entity.ani;

import com.meiya.whalex.db.entity.AbstractDbTableInfo;
import lombok.Data;

import java.util.Map;

/**
 * @author 黄河森
 * @date 2022/7/6
 * @package com.meiya.whalex.db.entity.ani
 * @project whalex-data-driver
 */
@Data
public class ClickHouseTableInfo extends AbstractDbTableInfo {

    private EngineType engine = EngineType.TINY_LOG;

    private boolean openDistributed = Boolean.FALSE;

    private boolean openReplica = Boolean.FALSE;

    private Map<String, String> engineParamMap;

}
