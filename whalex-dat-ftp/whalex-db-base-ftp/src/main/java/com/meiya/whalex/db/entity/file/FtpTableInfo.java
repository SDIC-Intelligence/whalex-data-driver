package com.meiya.whalex.db.entity.file;

import com.meiya.whalex.db.entity.AbstractDbTableInfo;
import lombok.Builder;
import lombok.Data;

/**
 * 表信息
 *
 * @author wangkm1
 * @date 2025/03/31
 * @project whale-cloud-platformX
 */
@Builder
@Data
public class FtpTableInfo extends AbstractDbTableInfo {

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
     * 路径
     */
    private String path;

    /**
     * 文件类型
     */
    private String fileType;

    /**
     * 分区格式，废弃
     */
    private String format;

    /**
     * 压缩方式
     */
    private String compressionCode;

    /**
     * 最大块大小
     */
    private Long blockSize;

//    private StructObjectInspector structObjectInspector;
}
