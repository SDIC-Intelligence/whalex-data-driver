package com.meiya.whalex.db.entity.file;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Set;

/**
 * @author 黄河森
 * @date 2023/9/1
 * @package com.meiya.whalex.db.entity.file
 * @project whalex-data-driver
 * @description MergeDataTask
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class MergeDataTask<W> {

    private String taskId;

    private W write;

    /**
     * 合并生成的文件集合
     */
    private Set<String> mergeFiles;

    /**
     * 已经完成合并的小文件集合
     */
    private Set<String> targetFiles;

}
