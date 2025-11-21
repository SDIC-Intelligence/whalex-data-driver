package com.meiya.whalex.db.template.file;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

/**
 * @author wangkm1
 * @date 2025/04/01
 * @project whalex-data-driver
 */
@Data
@Builder
@AllArgsConstructor
public class FtpTableTemplate {

    public static final String ORC_TYPE = "orc";

    public static final String PARQUET_TYPE = "parquet";

    public static final String COMPRESSION_SNAPPY = "SNAPPY";
    public static final String COMPRESSION_GZIP = "GZIP";
    public static final String COMPRESSION_LZO = "LZO";
    public static final String COMPRESSION_BROTLI = "BROTLI";
    public static final String COMPRESSION_LZ4 = "LZ4";
    public static final String COMPRESSION_ZSTD = "ZSTD";
    public static final String COMPRESSION_ZLIB = "ZLIB";
    public static final String COMPRESSION_NONE = "NONE";

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
     * 当个文件块大小
     */
    private Long blockSize;

    /**
     * 压缩方式
     */
    private String compressionCode;

    public FtpTableTemplate() {
    }

    public FtpTableTemplate(String fileType, String path, String format) {
        this.fileType = fileType;
        this.path = path;
        this.format = format;
    }
}
