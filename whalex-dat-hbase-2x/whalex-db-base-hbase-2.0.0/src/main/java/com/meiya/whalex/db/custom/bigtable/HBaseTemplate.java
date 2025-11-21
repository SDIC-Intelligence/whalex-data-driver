/**
 * Copyright©2011 Xiamen Meiah Pico IT CO., Ltd.
 * All rights reserved.
 */
package com.meiya.whalex.db.custom.bigtable;

import com.meiya.whalex.exception.BusinessException;
import com.meiya.whalex.exception.ExceptionCode;
import com.meiya.whalex.util.JsonUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.HashedBytes;
import org.apache.hadoop.hbase.util.RegionSplitter;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;


/**
 * 功能/模块：Spring Template 风格的 HBase 访问类。<br/>
 * 描述：<br/>
 * <p>为了提高效率，请根据实际情况设置：<pre>
 * {@link Get#addFamily(byte[])}
 * {@link Get#addColumn(byte[], byte[])}
 * {@link Scan#addFamily(byte[])}
 * {@link Scan#addColumn(byte[], byte[])}
 * ...
 * 修订历史：<br/>
 * 日期  作者  参考  描述：<br/>
 * @author yanxz
 * @version 1.0 Nov 22, 2011
 * @see
 */
@Slf4j
public class HBaseTemplate {

    // 不通过 Spring IoC 访问时使用
    // 为支持可以连接多个集群，不使用单例子
    //private static HBaseTemplate singleton;

    // 不通过 Spring IoC 访问时使用
    // 为支持可以连接多个集群，不使用单例子
    //private static HBaseAdmin adminSingleton;


    private Connection hTablePool;
    private String userName;//华为集群的用户名,做为hbase 命名空间

    /**
     * weic 无参构造函数，为了做AOP时提供给CGLIB生成子类代理用
     */
    public HBaseTemplate() {
    }

    public HBaseTemplate(Connection hTablePool, String userName) {
        this.hTablePool = hTablePool;
        this.userName = userName;
    }

//	public HBaseTemplate(Connection hTablePool) {
//		this.hTablePool = hTablePool;
//		//this.userName=huaweiUniformLogin.getUserName();
//	}

    public Connection getHTablePool() {
        return hTablePool;
    }

    public void setHTablePool(Connection tablePool) {
        hTablePool = tablePool;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    /**
     * 应用停止时的清除动作。<br/>
     * 配置文件：hbaseContext.xml, &lt;bean ... destroy-method=&quot;cleanup&quot;/&gt;
     */
    public void cleanup() {
        try {
            hTablePool.close();
        } catch (Throwable e) {
            log.error(e + e.getMessage(), e);
        }
    	/*
    	OR
    	I guess the main advantage would be that the interface
    	http://commons.apache.org/pool/apidocs/org/apache/commons/pool/ObjectPool.htmlis
    	simple and well-understood by a lot of people, and also
    	http://commons.apache.org/pool/apidocs/org/apache/commons/pool/impl/GenericObjectPool.htmlmakes
    	a lot of things configurable and allows a built-in way to optionally
    	verify that each HTable is working before handing it out.
    	*/
    }

    /**
     * 利用coprocessor统计表数据量
     * @param tableName 目标表名
     * @return 表数据量或者-1(异常情况)
     *//*
    public long count(String tableName){
    	try{
    		Scan scan = new Scan();
    		scan.setFilter(new FirstKeyOnlyFilter());
			Map<byte[], Long> results = this.coprocessorExec(tableName, CommonProtocol.class, "getScanCount", new Scan(scan));
			long count = 0;
			//整合不同RegionServer上的结果
			for(Long result : results.values()){
				count += result;
			}
			return count;
    	} catch (Exception e) {
			log.error(e);
			return -1;
		}
    }*/


    /**
     * DML 操作
     * 表否存在
     *
     * @param tableName
     * @return
     * @throws IOException
     */
    public boolean tableExists(String tableName) throws IOException {
        tableName = getNameSpaceTableName(tableName);
        return hTablePool.getAdmin().tableExists(TableName.valueOf(tableName));
    }

    /**
     * 列出表
     *
     * @param pattern 表的正则
     * @return
     * @throws IOException
     */
    public List<String> listTable(String pattern) throws IOException {
        TableName[] tableNames = hTablePool.getAdmin().listTableNamesByNamespace(pattern);
        List<String> tableNameList = new ArrayList<>();
        for (TableName tableName : tableNames) {
            tableNameList.add(tableName.getQualifierAsString());
        }
        return tableNameList;
    }

    /**
     * 列出表
     *
     * @return
     * @throws IOException
     */
    public List<String> listTable() throws IOException {
        TableName[] tableNames = hTablePool.getAdmin().listTableNames();
        List<String> tableNameList = new ArrayList<>();
        for (TableName tableName : tableNames) {
            tableNameList.add(tableName.getNameAsString());
        }
        return tableNameList;
    }

    /**
     * 删除表
     *
     * @param tableName 表名
     * @throws IOException
     */
    public void disableAndDropTable(String tableName) throws IOException {
        tableName = getNameSpaceTableName(tableName);
        Admin admin = hTablePool.getAdmin();
        TableName table = TableName.valueOf(tableName);
        if (!admin.isTableDisabled(table)) {
            admin.disableTable(table);
        }
        admin.deleteTable(table);
        log.info("delete " + table + " success!");
    }


    /**
     * 创建hbase 表
     *
     * @param tableName
     * @param tableDesMapList 对应hbase shell create table 的参数
     *                        [
     *                        {"NAME":"B","BLOOMFILTER":"ROW","VERSIONS":"1","COMPRESSION":"SNAPPY"},
     *                        {"NAME":"C","BLOOMFILTER":"ROW","VERSIONS":"1","COMPRESSION":"SNAPPY","BLOCKCACHE":"false"},
     *                        {"NUMREGIONS":"100","SPLITALGO":"HexStringSplit"}
     *                        ]
     * @throws IOException
     */
    public void createTable(String tableName, List<Map<String, String>> tableDesMapList, List<String> rowKeyPartition) throws IOException {
        tableName = getNameSpaceTableName(tableName);
        Admin admin = hTablePool.getAdmin();
        HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
        RegionSplitter.SplitAlgorithm splitAlgorithm = null;
        int regionCount = 10;
        for (Map<String, String> tableDesMap : tableDesMapList) {
            if (tableDesMap.get("NAME") != null) {
                HColumnDescriptor columnDescriptor = new HColumnDescriptor(Bytes.toBytes(tableDesMap.get("NAME")));
                for (Map.Entry<String, String> kv : tableDesMap.entrySet()) {
                    if (kv.getKey().equals("NAME")) continue;
                    columnDescriptor.setValue(kv.getKey(), kv.getValue());
                }
                tableDesc.addFamily(columnDescriptor);
            }
            if (tableDesMap.get("SPLITALGO") != null) {
                splitAlgorithm = RegionSplitter.newSplitAlgoInstance(hTablePool.getConfiguration(), tableDesMap.get("SPLITALGO"));
                regionCount = Integer.parseInt(tableDesMap.get("NUMREGIONS"));
            }
        }
        if(CollectionUtils.isNotEmpty(rowKeyPartition)) {
            byte[][] splitKeys = rowKeyPartition.stream()
                    .map(partition -> Bytes.toBytes(partition))
                    .collect(Collectors.toList())
                    .toArray(new byte[][]{});
            admin.createTable(tableDesc, splitKeys);
        }else if (splitAlgorithm != null) {
            admin.createTable(tableDesc, splitAlgorithm.split(regionCount));
        } else {
            admin.createTable(tableDesc);
        }
    }

    public void createTable(String tableName) throws IOException {
        createTable(tableName, 100);
    }

    public void createTable(String tableName, int regionCount) throws IOException {
        String defualtBcFamilyScheme = "[\n" +
                "{\"NAME\":\"B\",\"BLOOMFILTER\":\"ROW\",\"VERSIONS\":\"1\",\"COMPRESSION\":\"SNAPPY\"},\n" +
                "{\"NAME\":\"C\",\"BLOOMFILTER\":\"ROW\",\"VERSIONS\":\"1\",\"COMPRESSION\":\"SNAPPY\",\"BLOCKCACHE\":\"false\"},\n" +
                "{\"NUMREGIONS\":\"" + regionCount + "\",\"SPLITALGO\":\"HexStringSplit\"}\n" +
                "]";
        List<Map<String, String>> map = JsonUtil.jsonStrToObject(defualtBcFamilyScheme, List.class);
        createTable(tableName, map, null);
    }


    /**
     * 执行 HBase 表访问操作。<br/>
     * <b>请先考虑使用其他更具体的方法，尽量避免使用该方法，因其回调方法中提供了 HTable 实例，可能被误用。比如在回调方法中：</b><br/>
     * <pre>ResultScanner scanner = htable.getScanner(scan);</pre>
     * <b>但却没有调用 scanner.close() 进行关闭操作。</b>
     *
     * @param tableName 表名
     * @param action    回调类
     * @return 由回调类方法返回的对象
     * @throws BusinessException 异常
     */
    public <T> T execute(String tableName, TableTemplateCallback<T> action) throws BusinessException {
        Table htable = null;
        try {
            tableName = getNameSpaceTableName(tableName);
            htable = hTablePool.getTable(TableName.valueOf(tableName));
            T result = action.doWithTable(htable);
            return result;
        } catch (IOException e) {
            throw new BusinessException("Exception occured in execute method, tableName="
                    + tableName + "\n",
                    e);
        } catch (BusinessException e) {
            throw e;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new BusinessException("Exception occured in execute method, tableName="
                    + tableName + "\n",
                    e);
        } finally {
            if (htable != null) {
                try {
                    htable.close();
                } catch (IOException e) {
                    log.error("hTable close fail!", e);
                }
            }
        }
    }

    //如有没有命名空间，默认用用户自己的命名空间
    public String getNameSpaceTableName(String tableName) {
        if (this.userName != null && !tableName.contains(":")) {
            tableName = userName + ":" + tableName;
        }
        return tableName;
    }

    public Object[] batch(String tableName, final List<Row> actions) {
        return this.execute(tableName, new TableTemplateCallback<Object[]>() {
            @Override
            public Object[] doWithTable(Table htable)
                    throws BusinessException, IOException, InterruptedException {
                Object[] results = new Object[actions.size()];
                htable.batch(actions, results);
                return results;
            }
        });
    }

    /**
     * 锁定一行
     * @param tableName 目标表
     * @param row 被锁定的行
     * @return 行锁对象
     * @throws IOException 异常
     */
    /*public RowLock lockRow(String tableName, byte[] row) throws IOException {
        Table htable = hTablePool.getTable(tableName);
        try{
            return htable.lockRow(row);
        } finally {
            if (htable != null) {
                try {
                    htable.close();
                } catch (IOException e) {
                    log.error(e);
                }
            }
        }
    }*/

    /**
     * 解锁行
     * @param tableName 目标表
     * @param rowLock 行锁对象
     * @throws IOException 异常
     */
    /*public void unlockRow(String tableName, RowLock rowLock) throws IOException {
        Table htable = hTablePool.getTable(tableName);
        try{
            htable.unlockRow(rowLock);
        } finally {
            if (htable != null) {
                try {
                    htable.close();
                } catch (IOException e) {
                    log.error(e);
                }
            }
        }
    }*/

    /**
     * coprocessor执行类
     * @param tableName 处理的表名
     * @param coprocessorClass coprocessor类
     * @param coprocessorMethod coprocessor方法名称
     * @param methodArgs coprocessor方法的参数
     * @return Map<RegionName, 每个Region对应的结果集>,如果出异常,返回的将是null而不是emptyMap
     * @author weic
     */
    /*public <T, E extends CoprocessorProtocol> Map<byte[], T> coprocessorExec(String tableName, Class<E> coprocessorClass, String coprocessorMethod,  Object...methodArgs){
    	return this.coprocessorExec(tableName, null, null, coprocessorClass, coprocessorMethod, methodArgs);
    }*/

    /**
     * coprocessor执行类
     * @param tableName 处理的表名
     * @param startRow 注意，这里的startRow和endRow不是用来过滤结果集的，而是用来决定Endpoint执行的Region范围,
     * 从startRow到endRow间所有row所在的Region将被Endpoint处理执行
     * @param endRow 结束搜索endRow所在的Region
     * @param coprocessorClass coprocessor类
     * @param coprocessorMethod coprocessor方法名称
     * @param methodArgs coprocessor方法的参数
     * @return Map<RegionName, 每个Region对应的结果集>,如果出异常,返回的将是null而不是emptyMap
     * @author weic
     */
    /*public <T, E extends CoprocessorProtocol> Map<byte[], T> coprocessorExec(String tableName, byte[] startRow, byte[] endRow, Class<E> coprocessorClass, String coprocessorMethod, Object...methodArgs){
    	Table htable = hTablePool.getTable(tableName);
        try {
        	Batch.Call<E, T> call = Batch.forMethod(coprocessorClass, coprocessorMethod, methodArgs);
        	return htable.coprocessorExec(coprocessorClass, startRow, endRow, call);
        } catch(Throwable e){
        	log.error(e);
        	return null;
        } finally {
            if (htable != null) {
                try {
					htable.close();
				} catch (IOException e) {
					log.error(e);
				}
            }
        }
    }*/

    /**
     * 获得Scan的结果数量
     * @param tableName 表名
     * @param scan Scan
     * @return 统计结果数目
     */
    /*public long getScanCount(String tableName, Scan scan){
    	Map<byte[], Long> results = this.coprocessorExec(tableName, scan.getStartRow(), scan.getStopRow(), CommonProtocol.class, "getScanCount", scan);
		long count = 0;
		//整合不同RegionServer上的结果
		for(Long result : results.values()){
			count += result;
		}
		return count;
    }*/

    /**
     * 执行 HBase 表访问操作，根据传入的 Scan 对象。<br/>
     *
     * @param tableName 表名
     * @param scan      {@link Scan}
     * @param action    回调类
     * @return 由回调类方法返回的对象（最后一次调用 {@link ResultCallback#doInRow(Result)} 返回的结果）
     * @throws BusinessException 异常
     */
    public <T> T executeWithScan(final String tableName, final Scan scan, final ResultCallback<T> action) throws BusinessException {
        return this.execute(tableName, new TableTemplateCallback<T>() {

            @Override
            public T doWithTable(Table htable) throws BusinessException, IOException {
                ResultScanner scanner = null;
                T t = null;
                try {
                    scanner = htable.getScanner(scan);
                    Result result = scanner.next();
                    while (result != null) {
                        t = action.doInRow(result);
                        result = scanner.next();
                    }
                } catch (IOException e) {
                    throw new BusinessException("Exception occured in executeWithScan method, tableName="
                            + tableName + "\n scan=" + scan + "\n",
                            e);
                } catch (BusinessException e) {
                    throw e;
                } finally {
                    if (scanner != null) {
                        scanner.close();
                    }
                }
                return t;
            }
        });
    }

    /**
     * 执行 HBase 表访问操作，根据传入的 Scan 对象。<br/>
     *
     * @param tableName 表名
     * @param scan      {@link Scan}
     * @param action    回调类
     * @return 返回对象的集合
     * @throws BusinessException 异常
     */
    public <T> List<T> queryWithScan(final String tableName, final Scan scan, final ResultCallback<T> action) throws BusinessException {
        return this.queryWithScan(tableName, scan, false, action);
    }

    /**
     * 执行 HBase 表访问操作，根据传入的 Scan 对象。(可过滤null的结果)<br/>
     *
     * @param tableName  表名
     * @param scan       {@link Scan}
     * @param filterNull 空值是否被过滤出结果集
     * @param action     回调类
     * @return 返回对象的集合
     * @throws BusinessException 异常
     */
    public <T> List<T> queryWithScan(final String tableName, final Scan scan, final boolean filterNull, final ResultCallback<T> action) throws BusinessException {
        return this.execute(tableName, new TableTemplateCallback<List<T>>() {
            @Override
            public List<T> doWithTable(Table htable) throws BusinessException, IOException {
                ResultScanner scanner = null;
                List<T> t = new ArrayList<T>();
                try {
                    scanner = htable.getScanner(scan);
                    Result result = scanner.next();
                    while (result != null) {
                        T res = action.doInRow(result);
                        if (null != res || !filterNull) {
                            t.add(res);
                        }
                        result = scanner.next();
                    }
                } catch (IOException e) {
                    throw new BusinessException("Exception occured in executeWithScan method, tableName="
                            + tableName + "\n scan=" + scan + "\n",
                            e);
                } catch (BusinessException e) {
                    throw e;
                } finally {
                    if (scanner != null) {
                        scanner.close();
                    }
                }
                return t;
            }
        });
    }

    /**
     * 执行 HBase 表访问操作，根据传入的 Scan 对象。<br/>
     *
     * @param tableName 表名
     * @param scan      {@link Scan}
     * @return 异常
     */
    public List<Result> executeWithScan(final String tableName, final Scan scan) {
        return this.execute(tableName, new TableTemplateCallback<List<Result>>() {
            @Override
            public List<Result> doWithTable(Table htable) throws BusinessException, IOException {
                ResultScanner scanner = null;
                List<Result> results = new ArrayList<Result>();
                try {
                    scanner = htable.getScanner(scan);
                    Result result = scanner.next();
                    while (result != null) {
                        results.add(result);
                        result = scanner.next();
                    }
                } catch (BusinessException e) {
                    throw e;
                } finally {
                    if (scanner != null) {
                        scanner.close();
                    }
                }
                return results;
            }
        });
    }

    /**
     * 更新方法(对于多个列同时更新，不建议循环调用此方法。请使用{@link HBaseTemplate#batchPut(String, List)})
     *
     * @param tableName 表名
     * @param row       rowKey
     * @param family    列族
     * @param qualifier 列
     * @param timestamp 时间戳
     * @param value     值
     * @throws BusinessException 异常
     */
    public void put(String tableName, byte[] row, byte[] family, byte[] qualifier, long timestamp, byte[] value) throws BusinessException {
        final Put put = new Put(row);
        put.addColumn(family, qualifier, timestamp, value);
        put(tableName, put);
    }

    /**
     * 更新方法(对于多个列同时更新，不建议循环调用此方法。请使用{@link HBaseTemplate#batchPut(String, List)})
     *
     * @param tableName 表名
     * @param row       rowKey
     * @param family    列族
     * @param qualifier 列
     * @param value     值
     * @throws BusinessException 异常
     */
    public void put(String tableName, byte[] row, byte[] family, byte[] qualifier, byte[] value) throws BusinessException {
        final Put put = new Put(row);
        put.addColumn(family, qualifier, value);
        put(tableName, put);
    }

    /**
     * 更新方法(对于多个列同时更新，不建议循环调用此方法。请使用{@link HBaseTemplate#batchPut(String, List)})
     *
     * @param tableName 表名
     * @param put       {@link Put}
     * @throws BusinessException 异常
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void put(String tableName, final Put put) throws BusinessException {
        if (put.isEmpty()) {
            log.warn("put has no columns");
        }
        execute(tableName, new TableTemplateCallback() {
            @Override
            public Object doWithTable(Table htable) throws BusinessException, IOException {
                htable.put(put);
                return null;
            }
        });
    }

    /**
     * 原子性Put
     *
     * @param tableName  表名
     * @param row        用来检查的rowkey
     * @param family     用来检查的列族
     * @param qualifier  用来检查的列
     * @param checkValue 用来检查的值
     * @param put        Put
     * @throws BusinessException
     */
    public boolean checkAndPut(String tableName, final byte[] row, final byte[] family, final byte[] qualifier, final byte[] checkValue, final Put put) throws BusinessException {
        if (put.isEmpty()) {
            log.warn("put has no columns");
        }
        return execute(tableName, new TableTemplateCallback<Boolean>() {
            @Override
            public Boolean doWithTable(Table htable) throws BusinessException, IOException {
                return htable.checkAndPut(row, family, qualifier, checkValue, put);
            }
        });
    }


    /**
     * 原子性Delete
     *
     * @param tableName  表名
     * @param row        用来检查的rowkey
     * @param family     用来检查的列族
     * @param qualifier  用来检查的列
     * @param checkValue 用来检查的值
     * @param delete     delete
     * @throws BusinessException
     */
    public boolean checkAndDelete(String tableName, final byte[] row, final byte[] family, final byte[] qualifier, final byte[] checkValue, final Delete delete) throws BusinessException {
        if (delete.isEmpty()) {
            log.warn("put has no columns");
        }
        return execute(tableName, new TableTemplateCallback<Boolean>() {
            @Override
            public Boolean doWithTable(Table htable) throws BusinessException, IOException {
                return htable.checkAndDelete(row, family, qualifier, checkValue, delete);
            }
        });
    }

    /**
     * 批量更新方法
     *
     * @param tableName 表名
     * @param puts      Put集合
     * @throws BusinessException 异常
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void batchPut(String tableName, final List<Put> puts) throws BusinessException {
        this.execute(tableName, new TableTemplateCallback() {
            @Override
            public Object doWithTable(Table htable) throws BusinessException, IOException {
                htable.put(puts);
                return null;
            }
        });
    }

    /**
     * 删除表的单个列值。
     *
     * @param tableName 目标表名
     * @param row       rowkey
     * @param family    列族
     * @param qualifier 列
     * @throws BusinessException 异常
     */
    public void delete(String tableName, byte[] row, byte[] family, byte[] qualifier) throws BusinessException {
        Delete d = new Delete(row);
        d.addColumn(family, qualifier);
        delete(tableName, d);
    }

    /**
     * 删除方法(对于多个列同时删除，不建议循环调用此方法。请使用{@link HBaseTemplate#batchDelete(String, List)})
     *
     * @param tableName 表名
     * @param delete    {@link Delete}
     * @throws BusinessException 异常
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void delete(String tableName, final Delete delete) throws BusinessException {
        this.execute(tableName, new TableTemplateCallback() {
            @Override
            public Object doWithTable(Table htable) throws BusinessException, IOException {
                htable.delete(delete);
                return null;
            }
        });
    }

    /**
     * 批量删除
     *
     * @param tableName 表名
     * @param deletes   多个Delete
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void delete(String tableName, final List<Delete> deletes) {
        this.execute(tableName, new TableTemplateCallback() {
            @Override
            public Object doWithTable(Table htable) throws BusinessException, IOException {
                htable.delete(deletes);
                return null;
            }
        });
    }

    /**
     * 批量删除方法
     *
     * @param tableName 表名
     * @param deletes   see {@link Delete}
     * @throws BusinessException 异常
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void batchDelete(String tableName, final List<Delete> deletes) throws BusinessException {
        this.execute(tableName, new TableTemplateCallback() {
            @Override
            public Object doWithTable(Table htable) throws BusinessException, IOException {
                htable.delete(deletes);
                return null;
            }
        });
    }

    /**
     * 执行一个针对 row/family/qualifier 的 Get 操作。
     *
     * @param tableName 表名
     * @param row       rowkey
     * @param family    列族
     * @param qualifier 列
     * @return value of KeyValue
     * @throws BusinessException 异常
     */
    public byte[] get(String tableName, byte[] row, final byte[] family,
                      final byte[] qualifier) throws BusinessException {
        final Get get = new Get(row);
        get.addColumn(family, qualifier);

        return execute(tableName, new TableTemplateCallback<byte[]>() {
            @Override
            public byte[] doWithTable(Table htable) throws BusinessException,
                    IOException {
                Result result = htable.get(get);
                if (result.isEmpty()) {
                    return null;
                }

                return result.getValue(family, qualifier);
            }
        });
    }

    /**
     * 执行一个针对 row/family/多个qualifier 的 Get 操作。
     *
     * @param tableName  表名
     * @param row        rowkey
     * @param family     列族
     * @param qualifiers 列
     * @return value of KeyValue
     * @throws BusinessException 异常
     */
    public Map<HashedBytes, byte[]> gets(String tableName, byte[] row, final byte[] family,
                                         final byte[]... qualifiers) throws BusinessException {
        final Get get = new Get(row);
        for (byte[] qualifier : qualifiers) {
            get.addColumn(family, qualifier);
        }
        final Map<HashedBytes, byte[]> resultMap = new HashMap<HashedBytes, byte[]>();
        return execute(tableName, new TableTemplateCallback<Map<HashedBytes, byte[]>>() {
            @Override
            public Map<HashedBytes, byte[]> doWithTable(Table htable) throws BusinessException,
                    IOException {
                Result result = htable.get(get);
                if (result.isEmpty()) {
                    return null;
                }
                for (byte[] qualifier : qualifiers) {
                    resultMap.put(new HashedBytes(qualifier), result.getValue(family, qualifier));
                }
                return resultMap;
            }
        });
    }

    /**
     * 执行一个针对 row 的 Get 操作。
     *
     * @param tableName 表名
     * @param row       列
     * @param action    get的回调
     * @return get结果
     * @throws BusinessException 异常
     */
    public <T> T get(String tableName, byte[] row, final ResultCallback<T> action)
            throws BusinessException {
        return this.get(tableName, row, null, action);
    }

    /**
     * 执行一个针对 row 的 Get 操作。
     *
     * @param tableName 表名
     * @param row       列
     * @return get结果
     * @throws BusinessException 异常
     */
    public Result getResult(String tableName, final byte[] row) throws BusinessException {
        return this.getResult(tableName, new Get(row));
    }

    /**
     * 执行一个针对 row 的 Get 操作。
     *
     * @param tableName 表名
     * @param get       {@link Get}
     * @return get结果
     * @throws BusinessException 异常
     */
    public Result getResult(String tableName, final Get get) throws BusinessException {
        return execute(tableName, new TableTemplateCallback<Result>() {
            @Override
            public Result doWithTable(Table htable) throws BusinessException,
                    IOException {
                return htable.get(get);
            }
        });
    }

    /**
     * 执行一个针对 row 的 Get 操作。
     *
     * @param tableName 表名
     * @param getList   {@link Get}
     * @return get结果
     * @throws BusinessException 异常
     */
    public Result[] getResult(String tableName, final List<Get> getList) throws BusinessException {
        return execute(tableName, new TableTemplateCallback<Result[]>() {
            @Override
            public Result[] doWithTable(Table htable) throws BusinessException,
                    IOException {
                return htable.get(getList);
            }
        });
    }

    /**
     * 执行一个针对 row 的 Get 操作。
     *
     * @param tableName 表名
     * @param row       列
     * @param family    只取出这个列族的结果
     * @param action    get的回调
     * @return get结果
     * @throws BusinessException 异常
     */
    public <T> T get(String tableName, byte[] row, byte[] family, final ResultCallback<T> action)
            throws BusinessException {
        final Get get = new Get(row);
        if (family != null) {
            get.addFamily(family);
        }
        return execute(tableName, new TableTemplateCallback<T>() {
            @Override
            public T doWithTable(Table htable) throws BusinessException,
                    IOException {
                Result result = htable.get(get);
                if (!result.isEmpty()) {
                    return action.doInRow(result);
                }
                return null;
            }
        });
    }

    /**
     * 判断指定row是否存在。
     *
     * @param tableName 表名
     * @param rowkey    rowkey
     * @return 指定row是否存在
     * @throws BusinessException 异常
     */
    public boolean exists(String tableName, final byte[] rowkey) throws BusinessException {
        return execute(tableName, new TableTemplateCallback<Boolean>() {
            @Override
            public Boolean doWithTable(Table htable)
                    throws BusinessException, IOException {
                return htable.exists(new Get(rowkey));
            }
        });
    }

    /**
     * 判断指定的row是否存在。
     *
     * @param tableName 表名
     * @param get       {@link Get}
     * @return 指定row是否存在
     * @throws BusinessException 异常
     */
    public boolean exists(String tableName, final Get get) throws BusinessException {
        return execute(tableName, new TableTemplateCallback<Boolean>() {
            @Override
            public Boolean doWithTable(Table htable)
                    throws BusinessException, IOException {
                return htable.exists(get);
            }
        });
    }

    /**
     * 判断指定row是否存在指定列族的值
     *
     * @param tableName 表名
     * @param row       rowkey
     * @param family    列族
     * @return 是否存在
     * @throws BusinessException 异常
     */
    public boolean exists(String tableName, byte[] row, byte[] family)
            throws BusinessException {
        Get get = new Get(row);
        get.addFamily(family);
        return exists(tableName, get);
    }

    /**
     * 判断指定的列是否存在。
     *
     * @param tableName 表名
     * @param row       rowkey
     * @param family    列族
     * @param qualifier 列
     * @return 是否存在
     * @throws BusinessException 异常
     */
    public boolean exists(String tableName, byte[] row, byte[] family,
                          byte[] qualifier) throws BusinessException {
        Get get = new Get(row);
        get.addColumn(family, qualifier);
        return exists(tableName, get);
    }

    /**
     * 自动增加一个列的值
     *
     * @param tableName 表名
     * @param row       rowkey
     * @param family    列
     * @param qualifier 值欲增大的列
     * @param amount    增量
     * @return 增大后的值
     */
    public long incrementColumnValue(String tableName, byte[] row, byte[] family, byte[] qualifier, long amount) {
        return incrementColumnValue(tableName, row, family, qualifier, amount, Durability.SYNC_WAL);
    }

    /**
     * 自动增加一个列的值
     *
     * @param tableName  表名
     * @param row        rowkey
     * @param family     列
     * @param qualifier  值欲增大的列
     * @param amount     增量
     * @param durability 是否写write-ahead-log
     * @return 增大后的值
     */
    public long incrementColumnValue(String tableName, byte[] row, byte[] family, byte[] qualifier, long amount, Durability durability) {
        Table htable = null;
        try {
            htable = hTablePool.getTable(TableName.valueOf(tableName));
            //Durability.SYNC_WAL
            return htable.incrementColumnValue(row, family, qualifier, amount, durability);
        } catch (IOException e) {
            throw new BusinessException("incrementColumnValue异常.", e);
        } finally {
            if (htable != null) {
                try {
                    htable.close();
                } catch (IOException e) {
                    log.error("hTable close fail!", e);
                }
            }
        }
    }

    /**
     * 如果非空就执行put.add操作(String)
     *
     * @param put       Put
     * @param family    列族
     * @param qualifier 列
     * @param value     值
     */
    public static void addPutIfNotNull(Put put, byte[] family, byte[] qualifier, String value) {
        if (value != null) {
            put.addColumn(family, qualifier, Bytes.toBytes(value));
        }
    }

    /**
     * 执行put.add操作(boolean)
     *
     * @param put       Put
     * @param family    列族
     * @param qualifier 列
     * @param value     值
     */
    public static void addPutIfNotNull(Put put, byte[] family, byte[] qualifier, boolean value) {
        put.addColumn(family, qualifier, Bytes.toBytes(value));
    }

    /**
     * 如果非空就执行put.add操作(long)</br>
     * 如果存在本来就放0的状态请勿调用此方法
     *
     * @param put       Put
     * @param family    列族
     * @param qualifier 列
     * @param value     值
     */
    public static void addPutIfNotNull(Put put, byte[] family, byte[] qualifier, long value) {
        if (value != 0) {
            put.addColumn(family, qualifier, Bytes.toBytes(value));
        }
    }

    /**
     * 如果非空就执行put.add操作(long)</br>
     * 如果存在本来就放0的状态请勿调用此方法
     *
     * @param put       Put
     * @param family    列族
     * @param qualifier 列
     * @param value     值
     */
    public static void addPutIfNotNull(Put put, byte[] family, byte[] qualifier, Long value) {
        if (value != null) {
            put.addColumn(family, qualifier, Bytes.toBytes(value));
        }
    }

    /**
     * 如果非空就执行put.add操作(int)</br>
     * 如果存在本来就放0的状态请勿调用此方法
     *
     * @param put       Put
     * @param family    列族
     * @param qualifier 列
     * @param value     值
     */
    public static void addPutIfNotNull(Put put, byte[] family, byte[] qualifier, int value) {
        if (value != 0) {
            put.addColumn(family, qualifier, Bytes.toBytes(value));
        }
    }

    /**
     * 如果非空就执行put.add操作(byte[])
     *
     * @param put       Put
     * @param family    列族
     * @param qualifier 列
     * @param value     值
     */
    public static void addPutIfNotNull(Put put, byte[] family, byte[] qualifier, byte[] value) {
        if (value != null) {
            put.addColumn(family, qualifier, value);
        }
    }

    /**
     * 如果非空就执行put.add操作(short)</br>
     * 如果存在本来就放0的状态请勿调用此方法
     *
     * @param put       Put
     * @param family    列族
     * @param qualifier 列
     * @param value     值
     */
    public static void addPutIfNotNull(Put put, byte[] family, byte[] qualifier, short value) {
        if (value != 0) {
            put.addColumn(family, qualifier, Bytes.toBytes(value));
        }
    }

    /**
     * 如果非空就执行put.add操作(short)</br>
     * 如果存在本来就放0的状态请勿调用此方法
     *
     * @param put       Put
     * @param family    列族
     * @param qualifier 列
     * @param value     值
     */
    public static void addPutIfNotNull(Put put, byte[] family, byte[] qualifier, Short value) {
        if (value != null) {
            addPutIfNotNull(put, family, qualifier, Bytes.toBytes(value));
        }
    }

    public List<String> getNameSpace() throws IOException {
        List<String> list = new ArrayList<>();
        NamespaceDescriptor[] namespaceDescriptors = hTablePool.getAdmin().listNamespaceDescriptors();
        for (NamespaceDescriptor namespaceDescriptor : namespaceDescriptors) {
            list.add(namespaceDescriptor.getName());
        }
        return list;
    }

    public List<Map<String, Object>> getSchemaInfo(String tableName) throws IOException {

        List<Map<String, Object>> list = new ArrayList<>();
        Table table = null;
        try {
            String nameSpaceTableName = getNameSpaceTableName(tableName);
            if(!nameSpaceTableName.contains(":")) {
                nameSpaceTableName  = "default:" + tableName;
            }
            table = hTablePool.getTable(TableName.valueOf(nameSpaceTableName));
            TableDescriptor descriptor = table.getDescriptor();
            Set<byte[]> columnFamilyNames = descriptor.getColumnFamilyNames();
            for (byte[] columnFamilyName : columnFamilyNames) {

                Map<String, Object> map = new HashMap<>();

                ColumnFamilyDescriptor columnFamily = descriptor.getColumnFamily(columnFamilyName);
                String name = columnFamily.getNameAsString();

                map.put("columnFamily", name);
                map.put("ttl", columnFamily.getTimeToLive());
                map.put("versions", columnFamily.getMaxVersions());
                map.put("min_versions", columnFamily.getMinVersions());
                map.put("in_memory", columnFamily.isInMemory());
                map.put("blocksize", columnFamily.getBlocksize());
                map.put("blockcache", columnFamily.isBlockCacheEnabled());
                map.put("evict_blocks_on_close", columnFamily.isEvictBlocksOnClose());
                map.put("new_version_behavior", columnFamily.isNewVersionBehavior());
                KeepDeletedCells keepDeletedCells = columnFamily.getKeepDeletedCells();
                if(keepDeletedCells != null) {
                    map.put("keep_deleted_cells", keepDeletedCells.name());
                }
                map.put("cache_data_on_write", columnFamily.isCacheDataOnWrite());
                DataBlockEncoding dataBlockEncoding = columnFamily.getDataBlockEncoding();
                if(dataBlockEncoding != null) {
                    map.put("data_block_encoding", dataBlockEncoding.name());
                }
                map.put("replication_scope", columnFamily.getScope());
                BloomType bloomFilterType = columnFamily.getBloomFilterType();
                if(bloomFilterType != null) {
                    map.put("bloomfilter", bloomFilterType.name());
                }
                map.put("cache_index_on_write", columnFamily.isCacheIndexesOnWrite());
                map.put("cache_blooms_on_write", columnFamily.isCacheBloomsOnWrite());
                map.put("prefetch_blocks_on_open", columnFamily.isPrefetchBlocksOnOpen());
                Compression.Algorithm compressionType = columnFamily.getCompressionType();
                if(compressionType != null) {
                    map.put("compression", compressionType.getName());
                }

                Scan scan = new Scan();
                scan.addFamily(columnFamilyName);
                scan.setMaxResultSize(1);
                Filter filter1 = new RowFilter(CompareFilter.CompareOp.GREATER, new BinaryComparator(null));
                FilterList filterList = new FilterList(new PageFilter(1), filter1);
                scan.setFilter(filterList);
                ResultScanner scanner = null;

                try {
                    scanner =table.getScanner(scan);
                    Set<String> fieldList = new LinkedHashSet<>();
                    for (Result r = scanner.next(); r != null; r = scanner.next()) {
                        if (!StringUtils.isEmpty(Bytes.toString(r.getRow()))) {
                            Cell[] cells = r.rawCells();
                            for (Cell cell : cells) {
                                String column = Bytes.toString(CellUtil.cloneQualifier(cell));
                                fieldList.add(column);
                            }
                        }
                    }
                    map.put("fields", fieldList);
                }catch (Exception e) {
                    e.printStackTrace();
                    log.error("hbase获表结构失败：" + e.getMessage());
                    throw new BusinessException(ExceptionCode.DB_QUERY_EXCEPTION, e.getMessage());
                } finally {
                    if(scanner != null)  scanner.close();
                }

                list.add(map);

            }
        }catch (Exception e){
            e.printStackTrace();
            log.error("hbase获表结构失败：" + e.getMessage());
            throw new BusinessException(ExceptionCode.DB_QUERY_EXCEPTION, e.getMessage());
        }finally {
            if(table != null) table.close();
        }

        return list;
    }



}
