package com.meiya.whalex.db.template.dwh;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

/**
 * @author 黄河森
 * @date 2020/7/6
 * @project whalex-data-driver
 */
@Data
@Builder
@AllArgsConstructor
public class HiveTableTemplate {

    /**
     * 文件类型（orc、parquet） (必填)
     */
    private String fileType;
    /**
     * hdfs文件路径 (必填)
     */
    private String path;
    /**
     * 存放分区键格式
     * /field={{itemCode#yyyyMMdd}}
     */
    private String format;

    /**
     * 运行内存 mb
     */
    private Integer randomMemory;

    /**
     * 字段分隔符
     */
    private String fieldsTerminated;

    /**
     * 数组分隔符
     */
    private String collectionItemsTerminated;

    /**
     * K-V 分隔符
     */
    private String mapKeysTerminated;

    /**
     * 行分隔符
     */
    private String linesTerminated;

    public HiveTableTemplate() {
    }

    public HiveTableTemplate(String fileType, String path, String format) {
        this.fileType = fileType;
        this.path = path;
        this.format = format;
    }
}
