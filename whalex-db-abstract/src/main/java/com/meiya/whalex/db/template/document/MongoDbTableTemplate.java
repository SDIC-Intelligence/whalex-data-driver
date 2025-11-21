package com.meiya.whalex.db.template.document;

import lombok.Builder;
import lombok.Data;

/**
 * @author 黄河森
 * @date 2022/3/18
 * @package com.meiya.whalex.db.template.document
 * @project whalex-data-driver
 */
@Data
@Builder
public class MongoDbTableTemplate {

    private Integer numInitialChunks;

    private Integer replica;

    private Boolean capped;

    private Long sizeInBytes;

    private Long maxDocuments;

}
