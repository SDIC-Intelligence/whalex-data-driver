package com.meiya.whalex.db.constant;

/**
 * 索引类型
 *
 * @author 黄河森
 * @date 2023/6/14
 * @package com.meiya.whalex.db.constant
 * @project whalex-data-driver
 */
public enum  IndexType {

    /**
     * 唯一索引
     */
    UNIQUE,

    /**
     * 倒排索引
     */
    INVERTED,

    /**
     * 布隆过波
     */
    BLOOM_FILTER,

    NGRAM_BLOOM_FILTER;

}
