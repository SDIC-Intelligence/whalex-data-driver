package com.meiya.whalex.db.entity.document;

import com.meiya.whalex.db.entity.AbstractCursorCache;
import com.meiya.whalex.interior.db.search.in.Page;
import com.mongodb.client.MongoCursor;
import lombok.Data;
import org.bson.Document;

/**
 * Mongodb 游标操作对象
 *
 * @author 黄河森
 * @date 2020/9/7
 * @project whalex-data-driver
 */
@Data
public class MongoCursorCache extends AbstractCursorCache<MongoHandle> {

    /**
     * mongodb 游标对象
     */
    private MongoCursor<Document> cursor;

    public MongoCursorCache(Page lastPage, Integer batchSize, MongoCursor<Document> cursor) {
        super(lastPage, batchSize);
        this.cursor = cursor;
    }

    @Override
    public void closeCursor() {
        cursor.close();
    }
}
