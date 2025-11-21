package com.meiya.whalex.db.entity.file;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * 文件合并结果
 *
 * @author 黄河森
 * @date 2023/9/4
 * @package com.meiya.whalex.db.entity.file
 * @project whalex-data-driver
 * @description MergeDataResult
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MergeDataResult {

    /**
     * 上级目录
     */
    private String parentPath;

    /**
     * 被合并的目标小文件
     */
    private List<String> mergeTargetFiles;

    /**
     * 合并生成的文件
     */
    private List<String> mergeGenerateFiles;

}
