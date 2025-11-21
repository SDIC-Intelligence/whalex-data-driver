package com.meiya.whalex.db.entity.bigtable;

import com.meiya.whalex.db.entity.AbstractDbHandler;
import lombok.Data;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;

import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * HBase 操作实体
 *
 * @author 黄河森
 * @date 2019/12/16
 * @project whale-cloud-platformX
 */
public class HBaseHandler extends AbstractDbHandler {

    private HBaseQuery query;

    private HBaseInsert insert;

    private HBaseCreateTable createTable;

    private HBaseDropTable dropTable;

    private HBaseListTable listTable;

    private HBaseListDatabase listDatabase;

    public HBaseListDatabase getListDatabase() {
        return listDatabase;
    }

    public void setListDatabase(HBaseListDatabase listDatabase) {
        this.listDatabase = listDatabase;
    }

    public HBaseQuery getQuery() {
        return query;
    }

    public void setQuery(HBaseQuery query) {
        this.query = query;
    }

    public HBaseInsert getInsert() {
        return insert;
    }

    public void setInsert(HBaseInsert insert) {
        this.insert = insert;
    }

    public HBaseCreateTable getCreateTable() {
        return createTable;
    }

    public void setCreateTable(HBaseCreateTable createTable) {
        this.createTable = createTable;
    }

    public HBaseDropTable getDropTable() {
        return dropTable;
    }

    public void setDropTable(HBaseDropTable dropTable) {
        this.dropTable = dropTable;
    }

    public HBaseListTable getListTable() {
        return listTable;
    }

    public void setListTable(HBaseListTable listTable) {
        this.listTable = listTable;
    }

    /**
     * HBase 查询实体类
     */
    public static class HBaseQuery {
        /**
         * 存放多个 rowKey
         */
        private List<Get> getList;

        private Scan scan;

        /**
         * 查询起始时间
         */
        private String startTime;
        /**
         * 查询结束时间
         */
        private String stopTime;

        /**
         * 位移数（scan 使用）
         */
        private int skip;

        /**
         * 结果数（scan 使用）
         */
        private int pageSize;

        /**
         * 返回字段最大版本数
         */
        private Integer maxVersion;

        private Boolean isCount = Boolean.FALSE;

        /**
         * 游标滚动批次大小
         */
        private Integer batchSize;

        /**
         * 是否返回rowKey
         */
        private boolean returnRowKey = true;

        public Boolean getCount() {
            return isCount;
        }

        public void setCount(Boolean count) {
            isCount = count;
        }

        public Integer getBatchSize() {
            return batchSize;
        }

        public void setBatchSize(Integer batchSize) {
            this.batchSize = batchSize;
        }

        public List<Get> getGetList() {
            return getList;
        }

        public void setGetList(List<Get> getList) {
            this.getList = getList;
        }

        public String getStartTime() {
            return startTime;
        }

        public void setStartTime(String startTime) {
            this.startTime = startTime;
        }

        public String getStopTime() {
            return stopTime;
        }

        public void setStopTime(String stopTime) {
            this.stopTime = stopTime;
        }

        public int getSkip() {
            return skip;
        }

        public void setSkip(int skip) {
            this.skip = skip;
        }

        public int getPageSize() {
            return pageSize;
        }

        public void setPageSize(int pageSize) {
            this.pageSize = pageSize;
        }

        public Integer getMaxVersion() {
            return maxVersion;
        }

        public void setMaxVersion(Integer maxVersion) {
            this.maxVersion = maxVersion;
        }

        public boolean isReturnRowKey() {
            return returnRowKey;
        }

        public void setReturnRowKey(boolean returnRowKey) {
            this.returnRowKey = returnRowKey;
        }

        public Scan getScan() {
            return scan;
        }

        public void setScan(Scan scan) {
            this.scan = scan;
        }
    }

    /**
     * HBase 入库实体类
     */
    public static class HBaseInsert {
        /**
         * 入库数据
         */
        private List<Put> putList;

        private List<Map<String, Object>> fieldValueList;

        /**
         * 入库分表时间
         */
        private Long captureTime;

        public List<Put> getPutList() {
            return putList;
        }

        public void setPutList(List<Put> putList) {
            this.putList = putList;
        }

        public Long getCaptureTime() {
            return captureTime;
        }

        public void setCaptureTime(Long captureTime) {
            this.captureTime = captureTime;
        }

        public List<Map<String, Object>> getFieldValueList() {
            return fieldValueList;
        }

        public void setFieldValueList(List<Map<String, Object>> fieldValueList) {
            this.fieldValueList = fieldValueList;
        }
    }

    /**
     * HBase 创建表
     */
    public static class HBaseCreateTable {
        /**
         * 表结构
         */
        private List<Map<String, String>> schemaTemPlate;
        /**
         * 分片算法
         */
        private String splitALgo;

        /**
         * 分片数
         */
        private Integer numRegions;

        /**
         * 建表开始时间
         */
        private Date startTime;
        /**
         * 建表结束时间
         */
        private Date stopTime;

        public List<Map<String, String>> getSchemaTemPlate() {
            return schemaTemPlate;
        }

        public void setSchemaTemPlate(List<Map<String, String>> schemaTemPlate) {
            this.schemaTemPlate = schemaTemPlate;
        }

        public String getSplitALgo() {
            return splitALgo;
        }

        public void setSplitALgo(String splitALgo) {
            this.splitALgo = splitALgo;
        }

        public Integer getNumRegions() {
            return numRegions;
        }

        public void setNumRegions(Integer numRegions) {
            this.numRegions = numRegions;
        }

        public Date getStartTime() {
            return startTime;
        }

        public void setStartTime(Date startTime) {
            this.startTime = startTime;
        }

        public Date getStopTime() {
            return stopTime;
        }

        public void setStopTime(Date stopTime) {
            this.stopTime = stopTime;
        }
    }

    /**
     * HBase 删除表
     */
    public static class HBaseDropTable {
        /**
         * 建表开始时间
         */
        private Date startTime;
        /**
         * 建表结束时间
         */
        private Date stopTime;

        public Date getStartTime() {
            return startTime;
        }

        public void setStartTime(Date startTime) {
            this.startTime = startTime;
        }

        public Date getStopTime() {
            return stopTime;
        }

        public void setStopTime(Date stopTime) {
            this.stopTime = stopTime;
        }

        public HBaseDropTable() {
        }

        public HBaseDropTable(Date startTime, Date stopTime) {
            this.startTime = startTime;
            this.stopTime = stopTime;
        }
    }

    @Data
    public static class HBaseListTable {
        private String tableMatch;
    }

    @Data
    public static class HBaseListDatabase {
        private String databaseMatch;
    }
}
