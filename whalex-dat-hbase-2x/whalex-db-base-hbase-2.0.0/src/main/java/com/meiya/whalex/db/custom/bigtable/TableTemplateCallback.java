/**
 * Copyright©2011 Xiamen Meiah Pico IT CO., Ltd. 
 * All rights reserved.
 */
package com.meiya.whalex.db.custom.bigtable;

import com.meiya.whalex.exception.BusinessException;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;

/**
 * 功能/模块： {@link org.apache.hadoop.hbase.client.HTable} 相关的回调类。<br/>
 * <b>请先考虑使用其他更具体的类，尽量避免使用该类，因其回调方法中提供了 HTable 实例，可能被误用。比如在回调方法中：</b><br/>
 * <pre>ResultScanner scanner = htable.getScanner(scan);</pre>
 * <b>但却没有调用 scanner.close() 进行关闭操作。</b>
 * 描述：<br/>
 * 修订历史：<br/>
 * 日期  作者  参考  描述：<br/>
 * @author yanxz
 * @version 1.0 Nov 22, 2011
 * @see HBaseTemplate#execute(String, TableTemplateCallback)
 * @param <T> 结果类型
 */
public interface TableTemplateCallback<T> {

    /**
     * Gets called by HbaseTemplate.execute with an active HTable for the given
     * tablename.
     * @param htable htable instance for the given tablename.
     * @return a result object, or null if none
     * @throws BusinessException
     * @throws IOException
     * @throws InterruptedException 
     */
    T doWithTable(Table htable) throws BusinessException, IOException, InterruptedException;


}
