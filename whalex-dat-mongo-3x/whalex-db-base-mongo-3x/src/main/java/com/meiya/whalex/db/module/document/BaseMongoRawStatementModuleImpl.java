package com.meiya.whalex.db.module.document;

import com.meiya.whalex.db.entity.DataResult;
import com.meiya.whalex.db.entity.DatabaseSetting;
import com.meiya.whalex.db.entity.document.MongoCursorCache;
import com.meiya.whalex.db.entity.document.MongoDatabaseInfo;
import com.meiya.whalex.db.entity.document.MongoHandle;
import com.meiya.whalex.db.entity.document.MongoTableInfo;
import com.meiya.whalex.db.module.AbstractDbRawStatementModule;
import com.meiya.whalex.db.module.document.statement.MongoStatement;
import com.mongodb.MongoClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.util.List;
import java.util.Set;


@Slf4j
public abstract class BaseMongoRawStatementModuleImpl extends AbstractDbRawStatementModule<MongoClient,
        MongoHandle,
        MongoDatabaseInfo,
        MongoTableInfo,
        MongoCursorCache> {


    private MongoStatementParser mongoStatementParser = new MongoStatementParser();


    @Override
    public DataResult rawStatementExecute(DatabaseSetting databaseSetting, String rawStatement) throws Exception {
        return _execute(databaseSetting, (dataConf)->mongoStatementParser.parse(rawStatement), this::executor);
    }

    private <R, P> R executor(MongoDatabaseInfo mongoDatabaseInfo, MongoClient mongoClient, P p) {

        MongoStatement mongoStatement = (MongoStatement) p;

        List<Document> list = mongoStatement.execute(mongoDatabaseInfo, mongoClient);

        if(CollectionUtils.isNotEmpty(list)) {
            for (Document document : list) {
                Set<String> keySet = document.keySet();
                for (String key : keySet) {
                    Object value = document.get(key);
                    if(value instanceof ObjectId) {
                        document.put(key, value.toString());
                    }
                }
            }
        }

        DataResult dataResult = new DataResult();
        dataResult.setData(list);

        return (R) dataResult;
    }

}
