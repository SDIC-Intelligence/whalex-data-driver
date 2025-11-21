package com.meiya.whalex.db.entity.graph;

import com.meiya.whalex.db.entity.AbstractDbHandler;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * @author 黄河森
 * @date 2024/3/7
 * @package com.meiya.whalex.db.entity.graph
 * @project whalex-data-driver
 * @description NebulaGraphHandler
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class NebulaGraphHandler extends AbstractDbHandler {

    private GqlQuery gqlQuery;

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class GqlQuery {
        private String gql;
        private Map<String, Object> params;
    }

}
