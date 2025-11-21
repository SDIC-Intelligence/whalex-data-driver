package com.meiya.whalex.db.entity.document;

import com.meiya.whalex.db.entity.AbstractDbHandler;
import com.mongodb.BasicDBObject;
import com.mongodb.WriteConcern;
import com.mongodb.client.model.*;
import lombok.Data;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.List;
import java.util.Map;

/**
 * MONGO 组件操作实体
 *
 * @author 黄河森
 * @date 2019/9/14
 * @project whale-cloud-platformX
 */
public class MongoHandle extends AbstractDbHandler {

    private MongoQuery mongoQuery;

    private MongoUpdate mongoUpdate;

    private MongoDel mongoDel;

    private MongoInsert mongoInsert;

    private MongoIndex mongoIndex;

    private MongoTable mongoTable;

    private MongoListTable mongoListTable;

    private MongoUpsertBatch mongoUpsertBatch;

    private MongoListDatabase mongoListDatabase;

    private MongoAlterTable alterTable;

    private MongoCreateDatabase mongoCreateDatabase;

    private MongoDropDatabase mongoDropDatabase;

    public MongoListDatabase getMongoListDatabase() {
        return mongoListDatabase;
    }

    public void setMongoListDatabase(MongoListDatabase mongoListDatabase) {
        this.mongoListDatabase = mongoListDatabase;
    }

    public MongoQuery getMongoQuery() {
        return mongoQuery;
    }

    public void setMongoQuery(MongoQuery mongoQuery) {
        this.mongoQuery = mongoQuery;
    }

    public MongoUpdate getMongoUpdate() {
        return mongoUpdate;
    }

    public void setMongoUpdate(MongoUpdate mongoUpdate) {
        this.mongoUpdate = mongoUpdate;
    }

    public MongoDel getMongoDel() {
        return mongoDel;
    }

    public void setMongoDel(MongoDel mongoDel) {
        this.mongoDel = mongoDel;
    }

    public MongoInsert getMongoInsert() {
        return mongoInsert;
    }

    public void setMongoInsert(MongoInsert mongoInsert) {
        this.mongoInsert = mongoInsert;
    }

    public MongoIndex getMongoIndex() {
        return mongoIndex;
    }

    public void setMongoIndex(MongoIndex mongoIndex) {
        this.mongoIndex = mongoIndex;
    }

    public MongoTable getMongoTable() {
        return mongoTable;
    }

    public void setMongoTable(MongoTable mongoTable) {
        this.mongoTable = mongoTable;
    }

    public MongoListTable getMongoListTable() {
        return mongoListTable;
    }

    public void setMongoListTable(MongoListTable mongoListTable) {
        this.mongoListTable = mongoListTable;
    }

    public MongoUpsertBatch getMongoUpsertBatch() {
        return mongoUpsertBatch;
    }

    public void setMongoUpsertBatch(MongoUpsertBatch mongoUpsertBatch) {
        this.mongoUpsertBatch = mongoUpsertBatch;
    }

    public MongoAlterTable getAlterTable() {
        return alterTable;
    }

    public void setAlterTable(MongoAlterTable alterTable) {
        this.alterTable = alterTable;
    }

    public MongoCreateDatabase getMongoCreateDatabase() {
        return mongoCreateDatabase;
    }

    public void setMongoCreateDatabase(MongoCreateDatabase mongoCreateDatabase) {
        this.mongoCreateDatabase = mongoCreateDatabase;
    }

    public MongoDropDatabase getMongoDropDatabase() {
        return mongoDropDatabase;
    }

    public void setMongoDropDatabase(MongoDropDatabase mongoDropDatabase) {
        this.mongoDropDatabase = mongoDropDatabase;
    }

    /**
     * mongo 查询实体
     */
    public static class MongoQuery {
        private BasicDBObject query;
        private BasicDBObject sort;
        private BasicDBObject hint;
        private String hintString;
        private BasicDBObject fields;
        private List<MongoAggQuery> aggregates;
        private int skip = 0;
        private int limit = -1;
        private Boolean isCount = Boolean.FALSE;
        private CountOptions countOptions;
        private Integer batchSize;
        private boolean limitReadTime = true;
        private Map<String, String> aggNameMap;

        public BasicDBObject getQuery() {
            return query;
        }

        public void setQuery(BasicDBObject query) {
            this.query = query;
        }

        public BasicDBObject getSort() {
            return sort;
        }

        public void setSort(BasicDBObject sort) {
            this.sort = sort;
        }

        public BasicDBObject getHint() {
            return hint;
        }

        public void setHint(BasicDBObject hint) {
            this.hint = hint;
        }

        public BasicDBObject getFields() {
            return fields;
        }

        public void setFields(BasicDBObject fields) {
            this.fields = fields;
        }

        public int getSkip() {
            return skip;
        }

        public void setSkip(int skip) {
            this.skip = skip;
        }

        public int getLimit() {
            return limit;
        }

        public void setLimit(int limit) {
            this.limit = limit;
        }

        public Boolean getCount() {
            return isCount;
        }

        public void setCount(Boolean count) {
            isCount = count;
        }

        public CountOptions getCountOptions() {
            return countOptions;
        }

        public void setCountOptions(CountOptions countOptions) {
            this.countOptions = countOptions;
        }

        public Integer getBatchSize() {
            return batchSize;
        }

        public void setBatchSize(Integer batchSize) {
            this.batchSize = batchSize;
        }

        public boolean isLimitReadTime() {
            return limitReadTime;
        }

        public void setLimitReadTime(boolean limitReadTime) {
            this.limitReadTime = limitReadTime;
        }

        public List<MongoAggQuery> getAggregates() {
            return aggregates;
        }

        public void setAggregates(List<MongoAggQuery> aggregates) {
            this.aggregates = aggregates;
        }

        public Map<String, String> getAggNameMap() {
            return aggNameMap;
        }

        public void setAggNameMap(Map<String, String> aggNameMap) {
            this.aggNameMap = aggNameMap;
        }

        public String getHintString() {
            return hintString;
        }

        public void setHintString(String hintString) {
            this.hintString = hintString;
        }
    }

    @Data
    public static class MongoAggQuery {
        private List<BasicDBObject> aggObj;
        private int limit;
    }

    /**
     * mongo 更新实体
     */
    public static class MongoUpdate {
        private BasicDBObject query;
        private BasicDBObject update;
        /**
         * 当更新记录不存在时，是否插入
         * true 为 不存在时就插入
         * false 为 不存在时不插入（默认）
         */
        private UpdateOptions options;
        /**
         * 当有多条匹配的更新行时，是否只更新第一条
         * true 为 全部更新
         * false 为只更新第一天（默认）
         */
        private Boolean multi = Boolean.TRUE;

        /**
         * 写模式
         */
        private WriteConcern writeConcern;

        public BasicDBObject getQuery() {
            return query;
        }

        public void setQuery(BasicDBObject query) {
            this.query = query;
        }

        public BasicDBObject getUpdate() {
            return update;
        }

        public void setUpdate(BasicDBObject update) {
            this.update = update;
        }

        public UpdateOptions getOptions() {
            return options;
        }

        public void setOptions(UpdateOptions options) {
            this.options = options;
        }

        public Boolean getMulti() {
            return multi;
        }

        public void setMulti(Boolean multi) {
            this.multi = multi;
        }

        public WriteConcern getWriteConcern() {
            return writeConcern;
        }

        public void setWriteConcern(WriteConcern writeConcern) {
            this.writeConcern = writeConcern;
        }
    }

    /**
     * mongo 插入实体
     */
    public static class MongoInsert {

        private List<Document> insertList;

        /**
         * 写模式
         */
        private WriteConcern writeConcern;

        public List<Document> getInsertList() {
            return insertList;
        }

        public void setInsertList(List<Document> insertList) {
            this.insertList = insertList;
        }

        public WriteConcern getWriteConcern() {
            return writeConcern;
        }

        public void setWriteConcern(WriteConcern writeConcern) {
            this.writeConcern = writeConcern;
        }
    }

    /**
     * mongo 删除实体
     */
    public static class MongoDel {
        private BasicDBObject query;

        /**
         * 写模式
         */
        private WriteConcern writeConcern;

        public BasicDBObject getQuery() {
            return query;
        }

        public void setQuery(BasicDBObject query) {
            this.query = query;
        }

        public WriteConcern getWriteConcern() {
            return writeConcern;
        }

        public void setWriteConcern(WriteConcern writeConcern) {
            this.writeConcern = writeConcern;
        }
    }

    /**
     * mongo 索引创建/删除
     */
    public static class MongoIndex {
        private BasicDBObject index;
        private IndexOptions options;

        public BasicDBObject getIndex() {
            return index;
        }

        public void setIndex(BasicDBObject index) {
            this.index = index;
        }

        public IndexOptions getOptions() {
            return options;
        }

        public void setOptions(IndexOptions options) {
            this.options = options;
        }
    }

    /**
     * mongo 建表/删表
     */
    public static class MongoTable {
        private CreateCollectionOptions options;

        public CreateCollectionOptions getOptions() {
            return options;
        }

        public void setOptions(CreateCollectionOptions options) {
            this.options = options;
        }
    }

    @Data
    public static class MongoListTable {
        private String tableMatch;
    }

    @Data
    public static class MongoUpsertBatch {
        private List<UpdateManyModel<Document>> updateOneModelList;
        /**
         * 写模式
         */
        private WriteConcern writeConcern;
    }

    @Data
    public static class MongoListDatabase {
        private String databaseMatch;
    }

    @Data
    public static class MongoAlterTable {

        private Document renameCommand;

        private List<Bson> addFields;

        private List<Bson> updateFields;

        private List<Bson> delFields;

    }

    @Data
    public static class MongoCreateDatabase {
        private String dbName;
    }

    @Data
    public static class MongoDropDatabase {
        private String dbName;
    }

}
