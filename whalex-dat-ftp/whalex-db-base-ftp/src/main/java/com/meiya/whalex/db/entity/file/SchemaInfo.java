package com.meiya.whalex.db.entity.file;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * hdfs schema 信息
 *
 * @author wangkm1
 * @date 2025/03/31
 * @package com.meiya.whalex.db.entity.file
 * @project whalex-data-driver
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class SchemaInfo<T> {

    /**
     * 分区字段
     */
    private String partitionFiled;

    /**
     * 分区格式
     */
    private String format;

    /**
     * 分区路径
     */
    private String path;

    /**
     * 结构对象
     */
    private T schema;

}
