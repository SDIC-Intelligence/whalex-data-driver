package com.meiya.whalex.db.entity.lucene;

import com.meiya.whalex.db.entity.AbstractDbHandler;
import lombok.Data;
import org.apache.http.HttpEntity;

import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Es 操作实体
 *
 * @author 黄河森
 * @date 2019/12/19
 * @project whale-cloud-platformX
 */
public class EsHandler extends AbstractDbHandler {

    private EsQuery esQuery;

    private EsInsert esInsert;

    private EsDel esDel;

    private EsUpdate esUpdate;

    private EsCreateTable createTable;

    private EsDropTable dropTable;

    private EsUpsert esUpsert;

    private EsAlterField esAlterField;

    private EsListTable esListTable;

    private EsUpsertBatch esUpsertBatch;

    public EsAlterField getEsAlterField() {
        return esAlterField;
    }

    public void setEsAlterField(EsAlterField esAlterField) {
        this.esAlterField = esAlterField;
    }

    public EsQuery getEsQuery() {
        return esQuery;
    }

    public void setEsQuery(EsQuery esQuery) {
        this.esQuery = esQuery;
    }

    public EsInsert getEsInsert() {
        return esInsert;
    }

    public void setEsInsert(EsInsert esInsert) {
        this.esInsert = esInsert;
    }

    public EsDel getEsDel() {
        return esDel;
    }

    public void setEsDel(EsDel esDel) {
        this.esDel = esDel;
    }

    public EsUpdate getEsUpdate() {
        return esUpdate;
    }

    public void setEsUpdate(EsUpdate esUpdate) {
        this.esUpdate = esUpdate;
    }

    public EsCreateTable getCreateTable() {
        return createTable;
    }

    public void setCreateTable(EsCreateTable createTable) {
        this.createTable = createTable;
    }

    public EsDropTable getDropTable() {
        return dropTable;
    }

    public void setDropTable(EsDropTable dropTable) {
        this.dropTable = dropTable;
    }

    public EsUpsert getEsUpsert() {
        return esUpsert;
    }

    public void setEsUpsert(EsUpsert esUpsert) {
        this.esUpsert = esUpsert;
    }

    public EsListTable getEsListTable() {
        return esListTable;
    }

    public void setEsListTable(EsListTable esListTable) {
        this.esListTable = esListTable;
    }

    public EsUpsertBatch getEsUpsertBatch() {
        return esUpsertBatch;
    }

    public void setEsUpsertBatch(EsUpsertBatch esUpsertBatch) {
        this.esUpsertBatch = esUpsertBatch;
    }

    /**
     * 查询实体
     */
    public static class EsQuery {

        private List<String> fields;

        private List<String> hiddenFields;

        private String queryJson;

        private HttpEntity queryHttpEntity;

        private String startTime;

        private String stopTime;

        private int from = 0;

        private int size = 15;

        private boolean agg;

        private boolean isCount;

        private Integer batchSize;

        private Object routingValue;

        private boolean primaryNode = false;

        private String preference;

        private Integer terminateAfter;

        public String getPreference() {
            return preference;
        }

        public void setPreference(String preference) {
            this.preference = preference;
        }

        public Integer getTerminateAfter() {
            return terminateAfter;
        }

        public void setTerminateAfter(Integer terminateAfter) {
            this.terminateAfter = terminateAfter;
        }

        public boolean isPrimaryNode() {
            return primaryNode;
        }

        public void setPrimaryNode(boolean primaryNode) {
            this.primaryNode = primaryNode;
        }

        public Object getRoutingValue() {
            return routingValue;
        }

        public void setRoutingValue(Object routingValue) {
            this.routingValue = routingValue;
        }

        public HttpEntity getQueryHttpEntity() {
            return queryHttpEntity;
        }

        public void setQueryHttpEntity(HttpEntity queryHttpEntity) {
            this.queryHttpEntity = queryHttpEntity;
        }

        public String getQueryJson() {
            return queryJson;
        }

        public void setQueryJson(String queryJson) {
            this.queryJson = queryJson;
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

        public int getFrom() {
            return from;
        }

        public void setFrom(int from) {
            this.from = from;
        }

        public int getSize() {
            return size;
        }

        public void setSize(int size) {
            this.size = size;
        }

        public boolean isAgg() {
            return agg;
        }

        public void setAgg(boolean agg) {
            this.agg = agg;
        }

        public List<String> getFields() {
            return fields;
        }

        public void setFields(List<String> fields) {
            this.fields = fields;
        }

        public boolean isCount() {
            return isCount;
        }

        public void setCount(boolean count) {
            isCount = count;
        }

        public Integer getBatchSize() {
            return batchSize;
        }

        public void setBatchSize(Integer batchSize) {
            this.batchSize = batchSize;
        }

        public List<String> getHiddenFields() {
            return hiddenFields;
        }

        public void setHiddenFields(List<String> hiddenFields) {
            this.hiddenFields = hiddenFields;
        }
    }

    /**
     * es 新增数据实体
     */
    public static class EsInsert {
        /**
         * id，put 操作下使用
         */
        private String id;
        /**
         * 数据报文
         */
        private String addStr;
        /**
         * 分表时间
         */
        private Long captureTime;

        /**
         * 文档大小
         */
        private long docSize;

        /**
         * 是否是路由插入
         */
        private Boolean isRouteInsert = Boolean.FALSE;

        /**
         * 路由字段
         */
        private String routingField;

        /**
         * 是否立即写入
         */
        private boolean refresh = false;

        public String getRoutingField() {
            return routingField;
        }

        public void setRoutingField(String routingField) {
            this.routingField = routingField;
        }

        public Boolean getRouteInsert() {
            return isRouteInsert;
        }

        public void setRouteInsert(Boolean routeInsert) {
            isRouteInsert = routeInsert;
        }

        public String getAddStr() {
            return addStr;
        }

        public void setAddStr(String addStr) {
            this.addStr = addStr;
        }

        public Long getCaptureTime() {
            return captureTime;
        }

        public void setCaptureTime(Long captureTime) {
            this.captureTime = captureTime;
        }

        public long getDocSize() {
            return docSize;
        }

        public void setDocSize(long docSize) {
            this.docSize = docSize;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public boolean isRefresh() {
            return refresh;
        }

        public void setRefresh(boolean refresh) {
            this.refresh = refresh;
        }
    }

    /**
     * es 删除数据
     */
    public static class EsDel {
        private HttpEntity queryHttpEntity;

        private String startTime;

        private String stopTime;

        public HttpEntity getQueryHttpEntity() {
            return queryHttpEntity;
        }

        public void setQueryHttpEntity(HttpEntity queryHttpEntity) {
            this.queryHttpEntity = queryHttpEntity;
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
    }

    /**
     * es 更新数据
     */
    public static class EsUpdate {

        private HttpEntity updateHttpEntity;

        private String startTime;

        private String stopTime;

        /**
         * 是否立即刷新
         */
        private boolean refresh = false;

        public HttpEntity getUpdateHttpEntity() {
            return updateHttpEntity;
        }

        public void setUpdateHttpEntity(HttpEntity updateHttpEntity) {
            this.updateHttpEntity = updateHttpEntity;
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

        public boolean isRefresh() {
            return refresh;
        }

        public void setRefresh(boolean refresh) {
            this.refresh = refresh;
        }
    }

    /**
     * es 建表
     */
    public static class EsCreateTable {
        /**
         * 表结构
         */
        private Map<String,Object> schemaTemPlate;
        /**
         * 备份数
         */
        private Integer numReplicas = -1;

        /**
         * 分片数
         */
        private Integer numShard;

        /**
         * 建表开始时间
         */
        private Date startTime;
        /**
         * 建表结束时间
         */
        private Date stopTime;

        public Map<String,Object> getSchemaTemPlate() {
            return schemaTemPlate;
        }

        public void setSchemaTemPlate(Map<String,Object> schemaTemPlate) {
            this.schemaTemPlate = schemaTemPlate;
        }

        public Integer getNumReplicas() {
            return numReplicas;
        }

        public void setNumReplicas(Integer numReplicas) {
            this.numReplicas = numReplicas;
        }

        public Integer getNumShard() {
            return numShard;
        }

        public void setNumShard(Integer numShard) {
            this.numShard = numShard;
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
     * es 删表
     */
    @Data
    public static class EsDropTable {
        /**
         * 建表开始时间
         */
        private Date startTime;
        /**
         * 建表结束时间
         */
        private Date stopTime;

        private String alias;
    }

    /**
     * es 有则更新无则新增
     */
    public static class EsUpsert {
        /**
         * id，put 操作下使用
         */
        private String id;
        /**
         * 数据报文
         */
        private String upsertStr;
        /**
         * 分表时间
         */
        private Long captureTime;

        /**
         * 是否立即写入
         */
        private boolean refresh = false;

        public Long getCaptureTime() {
            return captureTime;
        }

        public void setCaptureTime(Long captureTime) {
            this.captureTime = captureTime;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getUpsertStr() {
            return upsertStr;
        }

        public void setUpsertStr(String upsertStr) {
            this.upsertStr = upsertStr;
        }

        public boolean isRefresh() {
            return refresh;
        }

        public void setRefresh(boolean refresh) {
            this.refresh = refresh;
        }
    }

    /**
     * es 有则更新无则新增
     */
    public static class EsUpsertBatch {
        /**
         * 数据报文
         */
        private String upsertStr;
        /**
         * 分表时间
         */
        private Long captureTime;

        /**
         * 是否立即写入
         */
        private boolean refresh = false;

        /**
         * 总文档数
         */
        private long docCount;

        public Long getCaptureTime() {
            return captureTime;
        }

        public void setCaptureTime(Long captureTime) {
            this.captureTime = captureTime;
        }

        public String getUpsertStr() {
            return upsertStr;
        }

        public void setUpsertStr(String upsertStr) {
            this.upsertStr = upsertStr;
        }

        public boolean isRefresh() {
            return refresh;
        }

        public void setRefresh(boolean refresh) {
            this.refresh = refresh;
        }

        public long getDocCount() {
            return docCount;
        }

        public void setDocCount(long docCount) {
            this.docCount = docCount;
        }
    }

    /**
     * es 字段修改实体
     */
    @Data
    public static class EsAlterField {
        private Map<String,Object> addFieldMap;
        private List<String> deleteFieldList;
        private String alias;
    }

    /**
     * 索引查询
     */
    @Data
    public static class EsListTable {
        private String indexMatch;
    }
}
