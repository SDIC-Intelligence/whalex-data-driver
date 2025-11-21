package com.meiya.whalex.db.entity.lucene;

import com.meiya.whalex.db.entity.AbstractCursorCache;
import com.meiya.whalex.interior.db.search.in.Page;
import lombok.Data;

import java.util.List;

/**
 * @author xult
 * @date 2020/8/28
 * @project whale-cloud-platformX
 */
@Data
public class EsCursorCache extends AbstractCursorCache<EsHandler> {
    List<String> indexNameList;
    String lastIndex;
    String scrollId;
    String cursorIdUrl;

    public EsCursorCache(Page lastPage, Integer batchSize,
                         List<String> indexNameList, String lastIndex, String scrollId, String cursorIdUrl) {
        super(lastPage, batchSize);
        this.indexNameList = indexNameList;
        this.lastIndex = lastIndex;
        this.scrollId = scrollId;
        this.cursorIdUrl = cursorIdUrl;
    }

    @Override
    public void closeCursor() {

    }
}
