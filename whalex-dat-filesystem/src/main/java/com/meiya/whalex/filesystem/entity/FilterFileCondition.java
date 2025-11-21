package com.meiya.whalex.filesystem.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author 黄河森
 * @date 2023/9/1
 * @package com.meiya.whalex.filesystem.entity
 * @project whalex-data-driver
 * @description FilterFileCondition
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class FilterFileCondition {

    /**
     * 文件大小限制
     */
    private FileLengthCondition fileLength;


    /**
     * 文件大小限制
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class FileLengthCondition {
        private Long maxLength;
        private Long minLength;
    }
}
