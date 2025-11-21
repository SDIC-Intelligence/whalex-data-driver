package com.meiya.whalex.db.entity.ani;

import com.meiya.whalex.db.entity.AbstractDbHandler;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;
import java.util.List;

/**
 * 关系型数据库 操作实体
 *
 * @author 黄河森
 * @date 2019/12/23
 * @project whale-cloud-platformX
 */
@Data
public class AniHandler<QUERY extends AniHandler.AniQuery
        , INSERT extends AniHandler.AniInsert
        , DEL extends AniHandler.AniDel
        , UPDATE extends AniHandler.AniUpdate
        , CTABLE extends AniHandler.AniCreateTable
        , CINDEX extends AniHandler.AniCreateIndex
        , DINDEX extends AniHandler.AniDropIndex
        , UPSERT extends AniHandler.AniUpsert
        , ATABLE extends AniHandler.AniAlterTable
        , DTABLE extends AniHandler.AniDropTable
        , LTABLE extends AniHandler.AniListTable
        , LDATABASE extends AniHandler.AniListDatabase
        , UPSERTBACTH extends AniHandler.AniUpsertBatch
        , CREATEDATABASE extends AniHandler.AniCreateDatabase
        , UPDATEDATABASE extends AniHandler.AniUpdateDatabase
        , DROPDATABASE extends AniHandler.AniDropDatabase> extends AbstractDbHandler {

    private QUERY aniQuery;

    private INSERT aniInsert;

    private DEL aniDel;

    private UPDATE aniUpdate;

    private CTABLE aniCreateTable;

    private CINDEX aniCreateIndex;

    private DINDEX aniDropIndex;

    private UPSERT aniUpsert;

    private ATABLE aniAlterTable;

    private DTABLE aniDropTable;

    private LTABLE listTable;

    private LDATABASE listDatabase;

    private UPSERTBACTH aniUpsertBatch;

    private CREATEDATABASE createdatabase;

    private UPDATEDATABASE updatedatabase;

    private DROPDATABASE dropdatabase;

    /**
     * MySql 查询
     */
    public static class AniQuery {
        private String sql;
        private String sqlCount;
        private Object[] paramArray;
        private Boolean isCount = Boolean.FALSE;
        private Integer batchSize;
        private Integer offset;
        private Integer limit;
        private boolean agg;
        private List<String> aggNames;

        public String getSql() {
            return sql;
        }

        public void setSql(String sql) {
            this.sql = sql;
        }

        public String getSqlCount() {
            return sqlCount;
        }

        public void setSqlCount(String sqlCount) {
            this.sqlCount = sqlCount;
        }

        public Boolean getCount() {
            return isCount;
        }

        public void setCount(Boolean count) {
            isCount = count;
        }

        public Object[] getParamArray() {
            return paramArray;
        }

        public void setParamArray(Object[] paramArray) {
            this.paramArray = paramArray;
        }

        public Integer getBatchSize() {
            return batchSize;
        }

        public void setBatchSize(Integer batchSize) {
            this.batchSize = batchSize;
        }

        public Integer getOffset() {
            return offset;
        }

        public void setOffset(Integer offset) {
            this.offset = offset;
        }

        public Integer getLimit() {
            return limit;
        }

        public void setLimit(Integer limit) {
            this.limit = limit;
        }

        public boolean isAgg() {
            return agg;
        }

        public void setAgg(boolean agg) {
            this.agg = agg;
        }

        public List<String> getAggNames() {
            return aggNames;
        }

        public void setAggNames(List<String> aggNames) {
            this.aggNames = aggNames;
        }
    }

    /**
     * 数据插入
     */
    public static class AniInsert {
        private String sql;

        private Object[] paramArray;

        private boolean returnGeneratedKey = Boolean.FALSE;

        private List<String> returnFields;

        /**
         * 新增总数，某些组件使用
         */
        private Integer addTotal;

        public String getSql() {
            return sql;
        }

        public void setSql(String sql) {
            this.sql = sql;
        }

        public boolean isReturnGeneratedKey() {
            return returnGeneratedKey;
        }

        public void setReturnGeneratedKey(boolean returnGeneratedKey) {
            this.returnGeneratedKey = returnGeneratedKey;
        }

        public List<String> getReturnFields() {
            return returnFields;
        }

        public void setReturnFields(List<String> returnFields) {
            this.returnFields = returnFields;
        }

        public Object[] getParamArray() {
            return paramArray;
        }

        public void setParamArray(Object[] paramArray) {
            this.paramArray = paramArray;
        }

        public Integer getAddTotal() {
            return addTotal;
        }

        public void setAddTotal(Integer addTotal) {
            this.addTotal = addTotal;
        }
    }

    /**
     * 数据更新
     */
    public static class AniUpdate {
        private String sql;

        private Object[] paramArray;

        private List<String> subSQL;

        private List<Object[]> subParamArray;

        public String getSql() {
            return sql;
        }

        public void setSql(String sql) {
            this.sql = sql;
        }

        public List<String> getSubSQL() {
            return subSQL;
        }

        public void setSubSQL(List<String> subSQL) {
            this.subSQL = subSQL;
        }

        public Object[] getParamArray() {
            return paramArray;
        }

        public void setParamArray(Object[] paramArray) {
            this.paramArray = paramArray;
        }

        public List<Object[]> getSubParamArray() {
            return subParamArray;
        }

        public void setSubParamArray(List<Object[]> subParamArray) {
            this.subParamArray = subParamArray;
        }
    }

    /**
     * 数据删除
     */
    public static class AniDel {
        private String sql;

        private Object[] paramArray;

        public String getSql() {
            return sql;
        }

        public void setSql(String sql) {
            this.sql = sql;
        }

        public Object[] getParamArray() {
            return paramArray;
        }

        public void setParamArray(Object[] paramArray) {
            this.paramArray = paramArray;
        }
    }

    /**
     * 创建表
     */
    public static class AniCreateTable {
        private String sql;
        /**
         * 修复表操作
         */
        private boolean repair;

        /**
         * 是否分区
         */
        private boolean partition;

        /**
         * 设置字段备注，PostGreSQL使用
         */
        private List<String> fieldCommentList;

        /**
         * 分布键
         */
        private DistributedField distributedField;

        /**
         * 表备注
         */
        private String tableComment;

        private List<String> sequenceSqlList;

        private List<String> onUpdateFuncSqlList;

        private boolean isNotExists;

        /**
         * 创建分区表的sql
         */
        private List<String> partitionSqlList;

        public List<String> getOnUpdateFuncSqlList() {
            return onUpdateFuncSqlList;
        }

        public void setOnUpdateFuncSqlList(List<String> onUpdateFuncSqlList) {
            this.onUpdateFuncSqlList = onUpdateFuncSqlList;
        }

        public boolean isNotExists() {
            return isNotExists;
        }

        public void setNotExists(boolean notExists) {
            isNotExists = notExists;
        }

        public List<String> getPartitionSqlList() {
            return partitionSqlList;
        }

        public void setPartitionSqlList(List<String> partitionSqlList) {
            this.partitionSqlList = partitionSqlList;
        }

        public String getSql() {
            return sql;
        }

        public void setSql(String sql) {
            this.sql = sql;
        }

        public List<String> getFieldCommentList() {
            return fieldCommentList;
        }

        public void setFieldCommentList(List<String> fieldCommentList) {
            this.fieldCommentList = fieldCommentList;
        }

        public boolean isRepair() {
            return repair;
        }

        public void setRepair(boolean repair) {
            this.repair = repair;
        }

        public DistributedField getDistributedField() {
            return distributedField;
        }

        public void setDistributedField(DistributedField distributedField) {
            this.distributedField = distributedField;
        }

        public String getTableComment() {
            return tableComment;
        }

        public void setTableComment(String tableComment) {
            this.tableComment = tableComment;
        }

        public List<String> getSequenceSqlList() {
            return sequenceSqlList;
        }

        public void setSequenceSqlList(List<String> sequenceSqlList) {
            this.sequenceSqlList = sequenceSqlList;
        }

        public boolean isPartition() {
            return partition;
        }

        public void setPartition(boolean partition) {
            this.partition = partition;
        }
    }

    /**
     * 分布键信息
     */
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class DistributedField {
        private String fieldName;
    }

    /**
     * 创建索引
     */
    public static class AniCreateIndex {
        private String sql;

        private boolean isNotExists;

        private String indexName;

        public String getSql() {
            return sql;
        }

        public void setSql(String sql) {
            this.sql = sql;
        }

        public boolean isNotExists() {
            return isNotExists;
        }

        public void setNotExists(boolean notExists) {
            isNotExists = notExists;
        }

        public String getIndexName() {
            return indexName;
        }

        public void setIndexName(String indexName) {
            this.indexName = indexName;
        }
    }

    /**
     * 删除索引
     */
    public static class AniDropIndex {
        private String sql;

        public String getSql() {
            return sql;
        }

        public void setSql(String sql) {
            this.sql = sql;
        }
    }

    /**
     * 新增或者更新
     */
    public static class AniUpsert {
        private String sql;

        private String updateSql;

        private String insertSql;

        private Object[] paramArray;

        private Object[] updateParamArray;

        public String getSql() {
            return sql;
        }

        public void setSql(String sql) {
            this.sql = sql;
        }

        public Object[] getParamArray() {
            return paramArray;
        }

        public void setParamArray(Object[] paramArray) {
            this.paramArray = paramArray;
        }

        public String getUpdateSql() {
            return updateSql;
        }

        public void setUpdateSql(String updateSql) {
            this.updateSql = updateSql;
        }

        public String getInsertSql() {
            return insertSql;
        }

        public void setInsertSql(String insertSql) {
            this.insertSql = insertSql;
        }

        public Object[] getUpdateParamArray() {
            return updateParamArray;
        }

        public void setUpdateParamArray(Object[] updateParamArray) {
            this.updateParamArray = updateParamArray;
        }

        public AniUpsert() {
        }
    }

    /**
     * 新增或者更新
     */
    public static class AniUpsertBatch {
        private List<String> sql;

        private List<Object[]> paramArray;

        public List<String> getSql() {
            return sql;
        }

        public void setSql(List<String> sql) {
            this.sql = sql;
        }

        public List<Object[]> getParamArray() {
            return paramArray;
        }

        public void setParamArray(List<Object[]> paramArray) {
            this.paramArray = paramArray;
        }

        public AniUpsertBatch() {
        }
    }

    @Data
    public static class AlterTableSqlInfo {
        private String sql;
        private boolean isNotExists;
        private String columnName;
    }

    /**
     * 修改表字段
     */
    public static class AniAlterTable {
        private String addSql;

        private String delSql;

        private List<String> delFieldList;

        private List<String> fieldCommentList;

        private List<String> sqlList;

        private List<AlterTableSqlInfo> alterTableSqlInfos;

        private boolean delPrimaryKey;

        public List<AlterTableSqlInfo> getAlterTableSqlInfos() {
            return alterTableSqlInfos;
        }

        public void setAlterTableSqlInfos(List<AlterTableSqlInfo> alterTableSqlInfos) {
            this.alterTableSqlInfos = alterTableSqlInfos;
        }

        public boolean isDelPrimaryKey() {
            return delPrimaryKey;
        }

        public void setDelPrimaryKey(boolean delPrimaryKey) {
            this.delPrimaryKey = delPrimaryKey;
        }

        public List<String> getFieldCommentList() {
            return fieldCommentList;
        }

        public void setFieldCommentList(List<String> fieldCommentList) {
            this.fieldCommentList = fieldCommentList;
        }

        public String getDelSql() {
            return delSql;
        }

        public void setDelSql(String delSql) {
            this.delSql = delSql;
        }

        public String getAddSql() {
            return addSql;
        }

        public void setAddSql(String addSql) {
            this.addSql = addSql;
        }

        public List<String> getDelFieldList() {
            return delFieldList;
        }

        public void setDelFieldList(List<String> delFieldList) {
            this.delFieldList = delFieldList;
        }

        public List<String> getSqlList() {
            return sqlList;
        }

        public void setSqlList(List<String> sqlList) {
            this.sqlList = sqlList;
        }
    }

    /**
     * 删除表
     */
    @Data
    public static class AniDropTable {
        /**
         * 建表开始时间
         */
        private Date startTime;
        /**
         * 建表结束时间
         */
        private Date stopTime;

        private boolean ifExists;
    }

    @Data
    public static class AniListTable {
        private String tableMatch;
        private String databaseName;
    }

    @Data
    public static class AniListDatabase {
        private String databaseMatch;
    }

    @Data
    public static class AniCreateDatabase {
        private String dbName;
        private String character;
        private String collate;
    }

    @Data
    public static class AniUpdateDatabase {
        private String dbName;
        private String newDbName;
    }

    @Data
    public static class AniDropDatabase {
        private String dbName;
    }
}
