package com.meiya.whalex.db.entity.bigtable;

import com.meiya.whalex.db.entity.AbstractCursorCache;
import com.meiya.whalex.interior.db.search.in.Page;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import java.util.List;

/**
 * @author 黄河森
 * @date 2020/9/7
 * @project whalex-data-driver
 */
@Slf4j
@Data
public class HBaseCursorCache extends AbstractCursorCache {

    private ResultScanner scanner;

    private List<String> indexNameList;

    private String lastIndex;

    // ResultScanner没有判断是否有hasNext的方法，该变量用于存下一批次扫描的第一个result
    private Result firstResult;

    public HBaseCursorCache(Page lastPage, Integer batchSize, ResultScanner scanner,Result firstResult, List<String> indexNameList,String lastIndex) {
        super(lastPage, batchSize);
        this.scanner = scanner;
        this.firstResult = firstResult;
        this.indexNameList = indexNameList;
        this.lastIndex = lastIndex;
    }

    @Override
    public void closeCursor() {
        try {
            scanner.close();
        } catch (Exception e) {
            log.error("HBase Scanner对象关闭失败!", e);
        }
    }
}
