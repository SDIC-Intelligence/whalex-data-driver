package com.meiya.whalex.db.entity.dwh;

import com.meiya.whalex.db.entity.AbstractDbTableInfo;

/**
 * Hive 表信息实体
 *
 * @author 黄河森
 * @date 2019/12/26
 * @project whale-cloud-platformX
 */
public class HiveTableInfo extends AbstractDbTableInfo {
    /**
     * 文件类型（orc、parquet）（必填）
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

    public String getFileType() {
        return fileType;
    }

    public void setFileType(String fileType) {
        this.fileType = fileType;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public Integer getRandomMemory() {
        return randomMemory;
    }

    public void setRandomMemory(Integer randomMemory) {
        this.randomMemory = randomMemory;
    }

    public String getFieldsTerminated() {
        return fieldsTerminated;
    }

    public void setFieldsTerminated(String fieldsTerminated) {
        this.fieldsTerminated = fieldsTerminated;
    }

    public String getCollectionItemsTerminated() {
        return collectionItemsTerminated;
    }

    public void setCollectionItemsTerminated(String collectionItemsTerminated) {
        this.collectionItemsTerminated = collectionItemsTerminated;
    }

    public String getMapKeysTerminated() {
        return mapKeysTerminated;
    }

    public void setMapKeysTerminated(String mapKeysTerminated) {
        this.mapKeysTerminated = mapKeysTerminated;
    }

    public String getLinesTerminated() {
        return linesTerminated;
    }

    public void setLinesTerminated(String linesTerminated) {
        this.linesTerminated = linesTerminated;
    }
}
