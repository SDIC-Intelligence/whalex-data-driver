/**
 * Copyright©2011 Xiamen Meiah Pico IT CO., Ltd. 
 * All rights reserved.
 */
package com.meiya.whalex.db.custom.bigtable;

import com.meiya.whalex.exception.BusinessException;
import org.apache.hadoop.hbase.client.Result;

import java.io.IOException;

/**
 * 功能/模块： {@link Result} 相关的回调类。<br/>
 * 描述：<br/>
 * 修订历史：<br/>
 * 日期  作者  参考  描述：<br/>
 * @author yanxz
 * @version 1.0 Nov 22, 2011
 * @see HBaseTemplate#executeWithScan(String, org.apache.hadoop.hbase.client.Scan, ResultCallback)
 * @param <T> 结果类型
 */
public interface ResultCallback<T> {

    /**
     * Gets called by HBaseTemplate.executeWithScan with an active Scanner for
     * the given scan.
     * @param result Result instance for the given scan.
     * @return a result object, or null if none
     * @throws BusinessException
     * @throws IOException
     */
    T doInRow(Result result) throws BusinessException, IOException;
}
