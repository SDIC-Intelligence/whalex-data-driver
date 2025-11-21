package com.meiya.whalex.db.entity.document;


import com.meiya.whalex.db.entity.AbstractDbTableInfo;

/**
 * MONGO 组件 表配置信息
 *
 * @author 黄河森
 * @date 2019/9/19
 * @project whale-cloud-platformX
 */
public class MongoTableInfo extends AbstractDbTableInfo {

    private Integer numInitialChunks = 8191;

    private Integer replica;

    private Boolean capped;

    private Long sizeInBytes;

    private Long maxDocuments;

    public Integer getReplica() {
        return replica;
    }

    public void setReplica(Integer replica) {
        this.replica = replica;
    }

    public Integer getNumInitialChunks() {
        return numInitialChunks;
    }

    public void setNumInitialChunks(Integer numInitialChunks) {
        this.numInitialChunks = numInitialChunks;
    }


    public Boolean getCapped() {
        return capped;
    }

    public void setCapped(Boolean capped) {
        this.capped = capped;
    }

    public Long getSizeInBytes() {
        return sizeInBytes;
    }

    public void setSizeInBytes(Long sizeInBytes) {
        this.sizeInBytes = sizeInBytes;
    }

    public Long getMaxDocuments() {
        return maxDocuments;
    }

    public void setMaxDocuments(Long maxDocuments) {
        this.maxDocuments = maxDocuments;
    }
}
