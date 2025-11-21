package com.meiya.whalex.db.entity.ani;

import lombok.Data;

import java.util.Map;

/**
 * S3文件记录映射
 *
 * @author xult
 * @date 2020/3/26
 * @project whale-cloud-platformX
 */
@Data
public class S3RecordInfo {

    public static final String KEY = "key";
    public static final String DATA = "data";
    public static final String METADATA = "metadata";
    /**
     * 对象key
     */
    private String key;
    /**
     * 文件数据base64字符串
     */
    private String data;
    /**
     * 文件数据 MultipartFile
     */
//    private MultipartFile file;
    /**
     * 用户元数据
     */
    private Map<String, Object> metadata;
}
