package com.meiya.whalex.db.module.file;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.DateTime;
import cn.hutool.core.date.DateUnit;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.map.MapUtil;
import cn.hutool.core.util.NumberUtil;
import cn.hutool.core.util.ReflectUtil;
import cn.hutool.core.util.StrUtil;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.meiya.whalex.annotation.Support;
import com.meiya.whalex.cache.DatCaffeine;
import com.meiya.whalex.db.constant.SupportPower;
import com.meiya.whalex.db.entity.AbstractCursorCache;
import com.meiya.whalex.db.entity.DatabaseSetting;
import com.meiya.whalex.db.entity.PageResult;
import com.meiya.whalex.db.entity.QueryCursorMethodResult;
import com.meiya.whalex.db.entity.QueryMethodResult;
import com.meiya.whalex.db.entity.file.*;
import com.meiya.whalex.db.stream.StreamIterator;
import com.meiya.whalex.db.util.param.impl.file.NanoTimeUtils;
import com.meiya.whalex.exception.BusinessException;
import com.meiya.whalex.exception.ExceptionCode;
import com.meiya.whalex.filesystem.entity.FileTreeNode;
import com.meiya.whalex.filesystem.entity.FilterFileCondition;
import com.meiya.whalex.filesystem.entity.QueryFileTreeNode;
import com.meiya.whalex.filesystem.module.AbstractDistributedFileSystemBaseService;
import com.meiya.whalex.interior.db.operation.in.Point;
import com.meiya.whalex.util.JsonUtil;
import com.meiya.whalex.util.date.JodaTimeUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.StatsProvidingRecordWriter;
import org.apache.hadoop.hive.ql.io.orc.*;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.orc.OrcProto;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.NanoTime;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.io.IOException;
import java.lang.reflect.Method;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Ftp 服务
 *
 * @author wangkm1
 * @date 2025/03/31
 * @project whale-cloud-platformX
 */
@Slf4j
@Support(value = {
        SupportPower.TEST_CONNECTION,
        SupportPower.CREATE_TABLE,
        SupportPower.DROP_TABLE,
        SupportPower.SHOW_TABLE_LIST,
        SupportPower.SEARCH,
        SupportPower.CREATE
})
public class BaseFtpServiceImpl<S extends FtpClient,
        Q extends FtpHandler,
        D extends FtpDatabaseInfo,
        T extends FtpTableInfo,
        C extends AbstractCursorCache> extends AbstractDistributedFileSystemBaseService<S, Q, D, T, C> {

    public static final String TABLE_META_DIC = "/.meta/schema.";

    public static final String TABLE_TEMP_DIC = "/.tmp";

    private static final Pattern PATTERN_PARTITION_REGEX = Pattern.compile("^.*=([0-9]{4}(-[0-9]{2}(-[0-9]{2})?)?)$");

    private ConcurrentHashMap<String, Cache<String, Map<String, ParquetWriter<Group>>>> parquetWriterCache = new ConcurrentHashMap<>();

    private ConcurrentHashMap<String, Cache<String, Map<String, OrcWriter>>> orcWriterCache = new ConcurrentHashMap<>();

    private ConcurrentHashMap<String, ReentrantLock> writeLockCache = new ConcurrentHashMap<>();

    /**
     * 合并小文件任务缓存
     */
    private ConcurrentHashMap<String, MergeDataTask<ParquetWriter<Group>>> mergeDataTaskParquetCache = new ConcurrentHashMap<>();

    public ConcurrentHashMap<String, Cache<String, Map<String, ParquetWriter<Group>>>> getParquetWriterCache() {
        return parquetWriterCache;
    }

    public ConcurrentHashMap<String, Cache<String, Map<String, OrcWriter>>> getOrcWriterCache() {
        return orcWriterCache;
    }

    public ConcurrentHashMap<String, ReentrantLock> getWriteLockCache() {
        return this.writeLockCache;
    }

    /**
     * 写过期处理
     *
     * @param tableName
     * @param writer
     */
    private void writeParquetExpire(String tableName, Map<String, ParquetWriter<Group>> writer) {
        Map.Entry<String, ParquetWriter<Group>> next = writer.entrySet().iterator().next();
        String[] split = StringUtils.split(next.getKey(), "$");
        String path = split[0];
        String cacheKey = split[1];
        try {
            log.info("HDfs ParquetWriter out of write time execute submit, path: [{}]", path);
            closeParquetWriter(cacheKey, path, next.getValue());
        } catch (Exception e) {
            log.error("HDfs ParquetWriter timeout close fail, table: [{}] filePath: [{}] ", tableName, path, e);
        }
    }

    /**
     * 写过期处理
     *
     * @param tableName
     * @param writer
     */
    private void writeOrcExpire(String tableName, Map<String, OrcWriter> writer) {
        Map.Entry<String, OrcWriter> next = writer.entrySet().iterator().next();
        String[] split = StringUtils.split(next.getKey(), "$");
        String path = split[0];
        String cacheKey = split[1];
        try {
            log.info("HDfs OrcWriter out of write time execute submit, path: [{}]", path);
            closeOrcWriter(cacheKey, path, next.getValue().getWriter());
        } catch (Exception e) {
            log.error("HDfs ParquetWriter timeout close fail, table: [{}] filePath: [{}] ", tableName, path, e);
        }
    }

    /**
     * 关闭写入对象
     *
     * @param cacheKey
     * @param file
     * @param parquetWriter
     * @throws Exception
     */
    public void closeParquetWriter(String cacheKey, String file, ParquetWriter<Group> parquetWriter) throws Exception {
        S connect = helper.getDbConnect(cacheKey);
        closeParquetWriter(connect, file, parquetWriter);
    }

    /**
     * 关闭写入对象
     *
     * @param connect
     * @param file
     * @param parquetWriter
     * @throws Exception
     */
    public void closeParquetWriter(S connect, String file, ParquetWriter<Group> parquetWriter) throws Exception {
        parquetWriter.close();
        mvTemFile(file, connect);
    }

    /**
     * 将临时目录文件移到正式目录
     *
     * @param file
     * @param connect
     * @throws IOException
     */
    private void mvTemFile(String file, S connect) throws IOException {
        String replace = StringUtils.replace(file, TABLE_TEMP_DIC, "");
        String parentPath = StringUtils.substringBeforeLast(replace, "/");
        log.info("HDfs file mv {} to {}", file, replace);
        if (!connect.exists(parentPath)) {
            connect.mkdir(parentPath);
        }
        connect.rename(file, replace);
    }

    /**
     * 关闭写入对象
     *
     * @param cacheKey
     * @param file
     * @param orcWriter
     * @throws Exception
     */
    public void closeOrcWriter(String cacheKey, String file, StatsProvidingRecordWriter orcWriter) throws Exception {
        S connect = helper.getDbConnect(cacheKey);
        closeOrcWriter(connect, file, orcWriter);
    }

    /**
     * 关闭写入对象
     *
     * @param connect
     * @param file
     * @param orcWriter
     * @throws Exception
     */
    public void closeOrcWriter(S connect, String file, StatsProvidingRecordWriter orcWriter) throws Exception {
        orcWriter.close(true);
        mvTemFile(file, connect);
    }

    /**
     * 写入 parquet
     *
     * @param connect
     * @param databaseConf
     * @param tableConf
     * @param schemaInfo
     * @param rowData
     * @param cacheKey
     * @param lock
     * @param commitNow
     * @throws Exception
     */
    private void writeParquet(S connect, D databaseConf, T tableConf, SchemaInfo<MessageType> schemaInfo, List<Map<String, Object>> rowData, String cacheKey, ReentrantLock lock, boolean commitNow) throws Exception {
        MessageType parquetSchema = schemaInfo.getSchema();
        String partitionFiled = schemaInfo.getPartitionFiled();
        String path = schemaInfo.getPath();
        String format = schemaInfo.getFormat();
        try {
            // 持有锁
            lock.lock();
            // 获取当前 HDFS 对应的写入对象缓存池
            Cache<String, Map<String, ParquetWriter<Group>>> writerMap = this.parquetWriterCache.get(cacheKey);
            // 第一次创建写入对象
            if (writerMap == null) {
                writerMap = DatCaffeine.<String, Map<String, ParquetWriter<Group>>>newBuilder().expireAfterAccess(5, TimeUnit.MINUTES).removalListener((k, v, removalCause) -> {
                    if (!RemovalCause.EXPLICIT.equals(removalCause)) {
                        writeParquetExpire(k, v);
                    }
                }).build();
                this.parquetWriterCache.put(cacheKey, writerMap);
            }
            SimpleGroupFactory simpleGroupFactory = new SimpleGroupFactory(parquetSchema);
            // 执行写入
            for (Map<String, Object> rowDatum : rowData) {
                Group group = simpleGroupFactory.newGroup();
                // 分区值
                String partitionValue = null;
                for (Map.Entry<String, Object> entry : rowDatum.entrySet()) {
                    String key = entry.getKey();
                    Object value = entry.getValue();
                    if (value != null) {
                        Type type = parquetSchema.getType(key);
                        if (type == null) {
                            throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "HDFs 写入 parquet 文件中当前未定义: " + key + " 字段!");
                        }
                        // 根据schema处理值
                        value = conversionParquetValue(value, type);
                        boolean needPartition = StringUtils.isNotBlank(partitionFiled) && StringUtils.equalsIgnoreCase(key, partitionFiled);
                        if (value instanceof String) {
                            group.add(key, (String) value);
                            if (needPartition) {
                                if (StringUtils.isNotBlank(format)) {
                                    Date time = DateUtils.parseDateStrictly((String) value, JodaTimeUtil.DEFAULT_YMD_FORMAT
                                            , JodaTimeUtil.DEFAULT_YMDHMS_FORMAT
                                            , JodaTimeUtil.SOLR_TDATE_FORMATE
                                            , JodaTimeUtil.SOLR_TDATE_RETURN_FORMATE
                                            , JodaTimeUtil.COMPACT_YMDHMS_FORMAT
                                            , JodaTimeUtil.COMPACT_YMD_FORMAT
                                            , JodaTimeUtil.COMPACT_YMDH_FORMAT);
                                    partitionValue = DateUtil.format(time, format);
                                } else {
                                    partitionValue = (String) value;
                                }
                            }
                        } else if (value instanceof Integer) {
                            group.add(key, (Integer) value);
                            if (needPartition) {
                                partitionValue = String.valueOf(value);
                            }
                        } else if (value instanceof Long) {
                            group.add(key, (Long) value);
                            if (needPartition) {
                                partitionValue = String.valueOf(value);
                            }
                        } else if (value instanceof Boolean) {
                            group.add(key, (Boolean) value);
                            if (needPartition) {
                                partitionValue = String.valueOf(value);
                            }
                        } else if (value instanceof Double) {
                            group.add(key, (Double) value);
                            if (needPartition) {
                                partitionValue = String.valueOf(value);
                            }
                        } else if (value instanceof Float) {
                            group.add(key, (Float) value);
                            if (needPartition) {
                                partitionValue = String.valueOf(value);
                            }
                        } else if (value instanceof Date || value instanceof Timestamp || value instanceof Time) {
                            Date jDate = (Date) value;
                            cn.hutool.core.date.DateTime date = DateUtil.date(jDate.getTime());
                            org.apache.hadoop.hive.common.type.Timestamp timestamp = org.apache.hadoop.hive.common.type.Timestamp.ofEpochMilli(date.getTime(), date.getTimeZone().getRawOffset());
                            NanoTime nanoTime = NanoTimeUtils.getNanoTime(timestamp, false);
                            group.add(key, nanoTime);
                            if (needPartition) {
                                if (StringUtils.isNotBlank(format)) {
                                    partitionValue = DateUtil.format((Date) value, format);
                                } else {
                                    partitionValue = DateUtil.format((Date) value, DatePattern.NORM_DATE_PATTERN);
                                }
                            }
                        } else if (value instanceof Point) {
                            Point point = (Point) value;
                            Double lon = point.getLon();
                            Double lat = point.getLat();
                            group.add(key, lat + "," + lon);
                        } else if (value instanceof List) {
                            String json = JsonUtil.objectToStr(value);
                            group.add(key, json);
                        } else if (value.getClass().isArray() && !(value instanceof byte[])) {
                            Object[] objects = (Object[]) value;
                            String json = JsonUtil.objectToStr(CollectionUtil.newArrayList(objects));
                            group.add(key, json);
                        } else if (value instanceof Map) {
                            String json = JsonUtil.objectToStr(value);
                            group.add(key, json);
                        } else if (value instanceof byte[]) {
                            byte[] bytes = (byte[]) value;
                            group.add(key, Binary.fromConstantByteArray(bytes));
                        } else {
                            group.add(key, value.toString());
                            if (needPartition) {
                                partitionValue = value.toString();
                            }
                        }
                    }
                }
                // 获取写入对象
                String writerKey = tableConf.getTableName();
                if (StringUtils.isNotBlank(partitionValue)) {
                    writerKey = writerKey + "$" + partitionValue;
                }
                String finalPartitionValue = partitionValue;
                Map<String, ParquetWriter<Group>> parquetWriterMap = writerMap.get(writerKey, new Function<String, Map<String, ParquetWriter<Group>>>() {
                    @Override
                    public Map<String, ParquetWriter<Group>> apply(String s) {
                        try {
                            // 临时写入文件路径
                            String file;
                            if (StringUtils.isNotBlank(finalPartitionValue)) {
                                file = databaseConf.getBashPath() + tableConf.getPath() + TABLE_TEMP_DIC + "/" + path + "=" + finalPartitionValue;
                            } else {
                                file = databaseConf.getBashPath() + tableConf.getPath() + TABLE_TEMP_DIC;
                            }
                            file = file + "/" + UUID.randomUUID().toString().replaceAll("-", "") + "." + FtpTableInfo.PARQUET_TYPE;
                            ParquetWriter<Group> parquetWriter = getParquetWriter(connect.getConf(), file, parquetSchema, tableConf.getCompressionCode());
                            return MapUtil.builder(file + "$" + cacheKey, parquetWriter).build();
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                });
                Map.Entry<String, ParquetWriter<Group>> next = parquetWriterMap.entrySet().iterator().next();
                ParquetWriter<Group> parquetWriter = next.getValue();
                parquetWriter.write(group);
            }
            // 判断文件大小
            List<String> invalidateKey = new ArrayList<>();
            ConcurrentMap<String, Map<String, ParquetWriter<Group>>> parquetWriterMap = writerMap.asMap();
            for (Map.Entry<String, Map<String, ParquetWriter<Group>>> entry : parquetWriterMap.entrySet()) {
                String key = entry.getKey();
                // 判断 key 是否为当前操作的表
                if (StringUtils.startsWithIgnoreCase(key, tableConf.getTableName())) {
                    Map<String, ParquetWriter<Group>> value = entry.getValue();
                    Map.Entry<String, ParquetWriter<Group>> next = value.entrySet().iterator().next();
                    String nextKey = next.getKey();
                    ParquetWriter<Group> parquetWriter = next.getValue();
                    if (commitNow || (parquetWriter.getDataSize() >= tableConf.getBlockSize())) {
                        invalidateKey.add(key);
                        String[] split = StringUtils.split(nextKey, "$");
                        closeParquetWriter(split[1], split[0], parquetWriter);
                    }
                }
            }
            if (invalidateKey.size() > 0) {
                writerMap.invalidateAll(invalidateKey);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * 转换值
     *
     * @param value
     * @param type
     * @return
     * @throws Exception
     */
    private Object conversionParquetValue(Object value, Type type) throws Exception {
        PrimitiveType.PrimitiveTypeName primitiveTypeName = type.asPrimitiveType().getPrimitiveTypeName();
        String key = type.getName();
        if (value instanceof String) {
            if (PrimitiveType.PrimitiveTypeName.INT96.equals(primitiveTypeName)) {
                Date date = DateUtils.parseDateStrictly((String) value, JodaTimeUtil.DEFAULT_YMD_FORMAT
                        , JodaTimeUtil.DEFAULT_YMDHMS_FORMAT
                        , JodaTimeUtil.SOLR_TDATE_FORMATE
                        , JodaTimeUtil.SOLR_TDATE_RETURN_FORMATE
                        , JodaTimeUtil.COMPACT_YMDHMS_FORMAT
                        , JodaTimeUtil.COMPACT_YMD_FORMAT
                        , JodaTimeUtil.COMPACT_YMDH_FORMAT);
                return date;
            } else if (PrimitiveType.PrimitiveTypeName.BOOLEAN.equals(primitiveTypeName)) {
                if (!StrUtil.equalsAnyIgnoreCase((String) value, "true", "false")) {
                    throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "HDFs 写入 parquet 文件中当前定义: " + key + " 字段为 BOOLEAN 类型, 请确认当前传入值是否正确!");
                }
                return Boolean.parseBoolean((String) value);
            } else if (PrimitiveType.PrimitiveTypeName.DOUBLE.equals(primitiveTypeName)) {
                try {
                    Double aDouble = Double.valueOf((String) value);
                    return aDouble;
                } catch (NumberFormatException e) {
                    throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "HDFs 写入 parquet 文件中当前定义: " + key + " 字段为 DOUBLE 类型, 请确认当前传入值是否正确!");
                }
            } else if (PrimitiveType.PrimitiveTypeName.FLOAT.equals(primitiveTypeName)) {
                Float aFloat;
                try {
                    aFloat = Float.valueOf((String) value);
                    return aFloat;
                } catch (NumberFormatException e) {
                    throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "HDFs 写入 parquet 文件中当前定义: " + key + " 字段为 FLOAT 类型, 请确认当前传入值是否正确!");
                }
            } else if (PrimitiveType.PrimitiveTypeName.INT32.equals(primitiveTypeName)) {
                Integer aInt;
                try {
                    aInt = Integer.valueOf((String) value);
                    return aInt;
                } catch (NumberFormatException e) {
                    throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "HDFs 写入 parquet 文件中当前定义: " + key + " 字段为 INTEGER 类型, 请确认当前传入值是否正确!");
                }
            } else if (PrimitiveType.PrimitiveTypeName.INT64.equals(primitiveTypeName)) {
                Long aLong;
                try {
                    aLong = Long.valueOf((String) value);
                    return aLong;
                } catch (NumberFormatException e) {
                    throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "HDFs 写入 parquet 文件中当前定义: " + key + " 字段为 LONG 类型, 请确认当前传入值是否正确!");
                }
            } else if (PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY.equals(primitiveTypeName)) {
                try {
                    Binary binary = Binary.fromString((String) value);
                    return binary.getBytes();
                } catch (NumberFormatException e) {
                    throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "HDFs 写入 parquet 文件中当前定义: " + key + " 字段为 BYTES 类型, 请确认当前传入值是否正确!");
                }
            } else {
                return value;
            }
        } else if (value instanceof Long) {
            if (PrimitiveType.PrimitiveTypeName.INT96.equals(primitiveTypeName)) {
                // 如果为 INT96 则表示为时间 Date 或 Time 类型，这时候该值为时间戳
                Date date = DateUtil.date((Long) value);
                return date;
            } else {
                return value;
            }
        } else if (value instanceof Double) {
            if (PrimitiveType.PrimitiveTypeName.FLOAT.equals(primitiveTypeName)) {
                return Float.valueOf(String.valueOf(value));
            } else {
                return value;
            }
        } else if (value instanceof Float) {
            if (PrimitiveType.PrimitiveTypeName.DOUBLE.equals(primitiveTypeName)) {
                return Double.valueOf(String.valueOf(value));
            } else {
                return value;
            }
        } else if (value instanceof Integer) {
            if (PrimitiveType.PrimitiveTypeName.INT64.equals(primitiveTypeName)) {
                return Long.valueOf(String.valueOf(value));
            } else {
                return value;
            }
        } else {
            return value;
        }
    }

    /**
     * 写入 orc
     *
     * @param connect
     * @param databaseConf
     * @param tableConf
     * @param rowData
     * @param orcSchema
     * @param cacheKey
     * @param lock
     * @param commitNow
     * @throws Exception
     */
    private void writeOrc(S connect, D databaseConf, T tableConf, SchemaInfo<StandardStructObjectInspector> orcSchema, List<Map<String, Object>> rowData, String cacheKey, ReentrantLock lock, boolean commitNow) throws Exception {
        StandardStructObjectInspector inspector = orcSchema.getSchema();
        String partitionFiled = orcSchema.getPartitionFiled();
        String format = orcSchema.getFormat();
        String path = orcSchema.getPath();
        try {
            // 持有锁
            lock.lock();
            // 获取当前 HDFS 对应的写入对象缓存池
            Cache<String, Map<String, OrcWriter>> writerMap = this.orcWriterCache.get(cacheKey);
            // 第一次创建写入对象
            if (writerMap == null) {
                writerMap = DatCaffeine.<String, Map<String, OrcWriter>>newBuilder().expireAfterAccess(5, TimeUnit.MINUTES).removalListener((k, v, removalCause) -> {
                    if (!RemovalCause.EXPLICIT.equals(removalCause)) {
                        writeOrcExpire(k, v);
                    }
                }).build();
                this.orcWriterCache.put(cacheKey, writerMap);
            }
            // 执行写入
            for (Map<String, Object> rowDatum : rowData) {
                OrcSerde orcSerde = new OrcSerde();
                List<String> originalColumnNames = inspector.getOriginalColumnNames();
                List<Object> objectList = new ArrayList<>(originalColumnNames.size());
                // 分区值
                String partitionValue = null;
                for (String originalColumnName : originalColumnNames) {
                    boolean needPartition = StringUtils.isNotBlank(partitionFiled) && StringUtils.equalsIgnoreCase(originalColumnName, partitionFiled);
                    Object value = rowDatum.get(originalColumnName);
                    if (value != null) {
                        StructField structFieldRef = inspector.getStructFieldRef(originalColumnName);
                        value = conversionOrcValue(value, structFieldRef);
                        if (value instanceof String) {
                            objectList.add(value);
                            if (needPartition) {
                                if (StringUtils.isNotBlank(format)) {
                                    Date time = DateUtils.parseDateStrictly((String) value, JodaTimeUtil.DEFAULT_YMD_FORMAT
                                            , JodaTimeUtil.DEFAULT_YMDHMS_FORMAT
                                            , JodaTimeUtil.SOLR_TDATE_FORMATE
                                            , JodaTimeUtil.SOLR_TDATE_RETURN_FORMATE
                                            , JodaTimeUtil.COMPACT_YMDHMS_FORMAT
                                            , JodaTimeUtil.COMPACT_YMD_FORMAT
                                            , JodaTimeUtil.COMPACT_YMDH_FORMAT);
                                    partitionValue = DateUtil.format(time, format);
                                } else {
                                    partitionValue = (String) value;
                                }
                            }
                        } else if (value instanceof Integer) {
                            objectList.add((Integer) value);
                            if (needPartition) {
                                partitionValue = String.valueOf(value);
                            }
                        } else if (value instanceof Long) {
                            objectList.add((Long) value);
                            if (needPartition) {
                                partitionValue = String.valueOf(value);
                            }
                        } else if (value instanceof Double) {
                            objectList.add((Double) value);
                            if (needPartition) {
                                partitionValue = String.valueOf(value);
                            }
                        } else if (value instanceof Float) {
                            objectList.add((Float) value);
                            if (needPartition) {
                                partitionValue = String.valueOf(value);
                            }
                        } else if (value instanceof Date || value instanceof Timestamp || value instanceof Time) {
                            Date jDate = (Date) value;
                            cn.hutool.core.date.DateTime date = DateUtil.date(jDate.getTime());
                            org.apache.hadoop.hive.common.type.Timestamp timestamp = org.apache.hadoop.hive.common.type.Timestamp.ofEpochMilli(date.getTime(), date.getTimeZone().getRawOffset());
                            TimestampWritableV2 timestampWritableV2 = new TimestampWritableV2(timestamp);
                            objectList.add(timestampWritableV2);
                            if (needPartition) {
                                if (StringUtils.isNotBlank(format)) {
                                    partitionValue = DateUtil.format((Date) value, format);
                                } else {
                                    partitionValue = DateUtil.format((Date) value, DatePattern.NORM_DATE_PATTERN);
                                }
                            }
                        } else if (value instanceof Boolean) {
                            objectList.add(value);
                            if (needPartition) {
                                partitionValue = String.valueOf(value);
                            }
                        } else if (value instanceof Point) {
                            Point point = (Point) value;
                            Double lon = point.getLon();
                            Double lat = point.getLat();
                            String pointStr = lat + "," + lon;
                            objectList.add(pointStr);
                        } else if (value instanceof List) {
                            String str = JsonUtil.objectToStr(value);
                            objectList.add(str);
                        } else if (value.getClass().isArray() && !(value instanceof byte[])) {
                            Object[] objects = (Object[]) value;
                            String str = JsonUtil.objectToStr(CollectionUtil.newArrayList(objects));
                            objectList.add(str);
                        } else if (value instanceof Map) {
                            String str = JsonUtil.objectToStr(value);
                            objectList.add(str);
                        } else if (value instanceof byte[]) {
                            byte[] bytes = (byte[]) value;
                            Binary binary = Binary.fromConstantByteArray(bytes);
                            objectList.add(binary);
                        } else {
                            objectList.add(value);
                        }
                    } else {
                        objectList.add(null);
                    }
                }
                // 获取写入对象
                String writerKey = tableConf.getTableName();
                if (StringUtils.isNotBlank(partitionValue)) {
                    writerKey = writerKey + "$" + partitionValue;
                }
                String finalPartitionValue = partitionValue;
                // 获取写入对象
                Map<String, OrcWriter> orcWriterMap = writerMap.get(writerKey, new Function<String, Map<String, OrcWriter>>() {
                    @Override
                    public Map<String, OrcWriter> apply(String s) {
                        try {
                            // 临时写入文件路径
                            // 临时写入文件路径
                            String file;
                            if (StringUtils.isNotBlank(finalPartitionValue)) {
                                file = databaseConf.getBashPath() + tableConf.getPath() + TABLE_TEMP_DIC + "/" + path + "=" + finalPartitionValue;
                            } else {
                                file = databaseConf.getBashPath() + tableConf.getPath() + TABLE_TEMP_DIC;
                            }
                            file = file + "/" + UUID.randomUUID().toString().replaceAll("-", "") + "." + FtpTableInfo.ORC_TYPE;
                            StatsProvidingRecordWriter orcWriter = getOrcWriter(connect, file, tableConf.getCompressionCode(), tableConf.getBlockSize());
                            return MapUtil.builder(file + "$" + cacheKey, new OrcWriter(orcWriter)).build();
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                });
                Map.Entry<String, OrcWriter> next = orcWriterMap.entrySet().iterator().next();
                String nextKey = next.getKey();
                OrcWriter orcWriter = next.getValue();
                orcWriter.getWriter().write(orcSerde.serialize(objectList, inspector));
                WriterImpl writer = (WriterImpl) ReflectUtil.getFieldValue(orcWriter.getWriter(), "writer");
                List<OrcProto.StripeInformation> stripes = (List<OrcProto.StripeInformation>) ReflectUtil.getFieldValue(writer, "stripes");
                int index = orcWriter.getStatisticIndex();
                if (stripes.size() > index) {
                    OrcProto.StripeInformation information = stripes.get(index);
                    long dataLength = information.getDataLength();
                    long dataSize = orcWriter.getDataSize() + dataLength;
                    orcWriter.setDataSize(dataSize);
                    orcWriter.increment();
                }
            }
            // 判断文件大小
            List<String> invalidateKey = new ArrayList<>();
            ConcurrentMap<String, Map<String, OrcWriter>> orcWriterMap = writerMap.asMap();
            for (Map.Entry<String, Map<String, OrcWriter>> entry : orcWriterMap.entrySet()) {
                String key = entry.getKey();
                if (StringUtils.startsWithIgnoreCase(key, tableConf.getTableName())) {
                    Map<String, OrcWriter> value = entry.getValue();
                    Map.Entry<String, OrcWriter> next = value.entrySet().iterator().next();
                    String nextKey = next.getKey();
                    OrcWriter orcWriter = next.getValue();
                    if (commitNow || (orcWriter.getDataSize() >= tableConf.getBlockSize())) {
                        invalidateKey.add(key);
                        String[] split = StringUtils.split(nextKey, "$");
                        closeOrcWriter(split[1], split[0], orcWriter.getWriter());
                    }
                }
            }
            if (invalidateKey.size() > 0) {
                writerMap.invalidateAll(invalidateKey);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * 转换值
     *
     * @param value
     * @param type
     * @return
     * @throws Exception
     */
    private Object conversionOrcValue(Object value, StructField type) throws Exception {
        String key = type.getFieldName();
        ObjectInspector fieldObjectInspector = type.getFieldObjectInspector();
        String typeName = fieldObjectInspector.getTypeName();
        if (value instanceof String) {
            if (StringUtils.endsWithIgnoreCase(typeName, "timestamp")) {
                Date date = DateUtils.parseDateStrictly((String) value, JodaTimeUtil.DEFAULT_YMD_FORMAT
                        , JodaTimeUtil.DEFAULT_YMDHMS_FORMAT
                        , JodaTimeUtil.SOLR_TDATE_FORMATE
                        , JodaTimeUtil.SOLR_TDATE_RETURN_FORMATE
                        , JodaTimeUtil.COMPACT_YMDHMS_FORMAT
                        , JodaTimeUtil.COMPACT_YMD_FORMAT
                        , JodaTimeUtil.COMPACT_YMDH_FORMAT);
                return date;
            } else if (StringUtils.endsWithIgnoreCase(typeName, "boolean")) {
                if (!StrUtil.equalsAnyIgnoreCase((String) value, "true", "false")) {
                    throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "HDFs 写入 parquet 文件中当前定义: " + key + " 字段为 BOOLEAN 类型, 请确认当前传入值是否正确!");
                }
                return Boolean.parseBoolean((String) value);
            } else if (StringUtils.endsWithIgnoreCase(typeName, "double")) {
                try {
                    Double aDouble = Double.valueOf((String) value);
                    return aDouble;
                } catch (NumberFormatException e) {
                    throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "HDFs 写入 parquet 文件中当前定义: " + key + " 字段为 DOUBLE 类型, 请确认当前传入值是否正确!");
                }
            } else if (StringUtils.endsWithIgnoreCase(typeName, "float")) {
                Float aFloat;
                try {
                    aFloat = Float.valueOf((String) value);
                    return aFloat;
                } catch (NumberFormatException e) {
                    throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "HDFs 写入 parquet 文件中当前定义: " + key + " 字段为 FLOAT 类型, 请确认当前传入值是否正确!");
                }
            } else if (StringUtils.endsWithIgnoreCase(typeName, "int")) {
                Integer aInt;
                try {
                    aInt = Integer.valueOf((String) value);
                    return aInt;
                } catch (NumberFormatException e) {
                    throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "HDFs 写入 parquet 文件中当前定义: " + key + " 字段为 INTEGER 类型, 请确认当前传入值是否正确!");
                }
            } else if (StringUtils.endsWithIgnoreCase(typeName, "bigint")) {
                Long aLong;
                try {
                    aLong = Long.valueOf((String) value);
                    return aLong;
                } catch (NumberFormatException e) {
                    throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "HDFs 写入 parquet 文件中当前定义: " + key + " 字段为 LONG 类型, 请确认当前传入值是否正确!");
                }
            } else if (StringUtils.endsWithIgnoreCase(typeName, "binary")) {
                try {
                    Binary binary = Binary.fromString((String) value);
                    return binary.getBytes();
                } catch (NumberFormatException e) {
                    throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "HDFs 写入 parquet 文件中当前定义: " + key + " 字段为 BYTES 类型, 请确认当前传入值是否正确!");
                }
            } else {
                return value;
            }
        } else if (value instanceof Long) {
            if (StringUtils.endsWithIgnoreCase(typeName, "timestamp")) {
                // 如果为 INT96 则表示为时间 Date 或 Time 类型，这时候该值为时间戳
                Date date = DateUtil.date((Long) value);
                return date;
            } else {
                return value;
            }
        } else if (value instanceof Double) {
            if (StringUtils.endsWithIgnoreCase(typeName, "float")) {
                return Float.valueOf(String.valueOf(value));
            } else {
                return value;
            }
        } else if (value instanceof Float) {
            if (StringUtils.endsWithIgnoreCase(typeName, "double")) {
                return Double.valueOf(String.valueOf(value));
            } else {
                return value;
            }
        } else if (value instanceof Integer) {
            if (StringUtils.endsWithIgnoreCase(typeName, "bigint")) {
                return Long.valueOf(String.valueOf(value));
            } else {
                return value;
            }
        } else {
            return value;
        }
    }

    /**
     * 获取写锁
     *
     * @param flagKey
     * @return
     */
    private ReentrantLock getWriteLock(String flagKey) {
        ReentrantLock lock = this.writeLockCache.get(flagKey);
        if (lock == null) {
            synchronized (this.writeLockCache) {
                lock = this.writeLockCache.get(flagKey);
                if (lock == null) {
                    lock = new ReentrantLock();
                    this.writeLockCache.put(flagKey, lock);
                }
            }
        }
        return lock;
    }

    /**
     * 获取 orc 写对象
     *
     * @param connect
     * @param filePath
     * @param compressionCode
     * @param blockSize
     * @return
     * @throws Exception
     */
    private StatsProvidingRecordWriter getOrcWriter(S connect, String filePath, String compressionCode, Long blockSize) throws Exception {
        OrcOutputFormat outFormat = new OrcOutputFormat();
        Properties tableProperties = new Properties();
        if (StringUtils.isNotBlank(compressionCode)) {
            switch (compressionCode.toUpperCase()) {
                case FtpTableInfo.COMPRESSION_SNAPPY:
                    tableProperties.put("orc.compress", CompressionKind.SNAPPY.name());
                    break;
                case FtpTableInfo.COMPRESSION_LZO:
                    tableProperties.put("orc.compress", CompressionKind.LZO.name());
                    break;
                case FtpTableInfo.COMPRESSION_ZLIB:
                    tableProperties.put("orc.compress", CompressionKind.ZLIB.name());
                    break;
                case FtpTableInfo.COMPRESSION_NONE:
                    tableProperties.put("orc.compress", CompressionKind.NONE.name());
                    break;
                default:
                    throw new UnsupportedOperationException("ORC 格式不支持 " + compressionCode.toUpperCase() + " 类型压缩!");
            }
        } else {
            tableProperties.put("orc.compress", CompressionKind.SNAPPY.name());
        }
        JobConf jobConf = new JobConf(connect.getConf());
        long defaultBlockSize = connect.getFileSystem().getDefaultBlockSize();
        jobConf.setLong("hive.exec.orc.default.block.size", defaultBlockSize);
        jobConf.setLong("hive.exec.orc.default.stripe.size", blockSize);
        jobConf.setFloat("hive.exec.orc.memory.pool", (float) 0.3);
        StatsProvidingRecordWriter recordWriter = outFormat.getHiveRecordWriter(jobConf, new Path(filePath), null, true, tableProperties, Reporter.NULL);
        return recordWriter;
    }

    /**
     * 获取 parquet 写对象
     *
     * @param configuration
     * @param filePath
     * @param schema
     * @param compressionCode
     * @return
     * @throws Exception
     */
    private ParquetWriter<Group> getParquetWriter(Configuration configuration, String filePath, MessageType schema, String compressionCode) throws Exception {
        Path path = new Path(filePath);
        ExampleParquetWriter.Builder builder = ExampleParquetWriter.builder(path)
                .withConf(configuration)
                .withWriteMode(ParquetFileWriter.Mode.CREATE)
                .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_1_0)
                .withType(schema);

        if (StringUtils.isNotBlank(compressionCode)) {
            switch (compressionCode.toUpperCase()) {
                case FtpTableInfo.COMPRESSION_SNAPPY:
                    builder.withCompressionCodec(CompressionCodecName.SNAPPY);
                    break;
                case FtpTableInfo.COMPRESSION_BROTLI:
                    builder.withCompressionCodec(CompressionCodecName.BROTLI);
                    break;
                case FtpTableInfo.COMPRESSION_GZIP:
                    builder.withCompressionCodec(CompressionCodecName.GZIP);
                    break;
                case FtpTableInfo.COMPRESSION_LZ4:
                    builder.withCompressionCodec(CompressionCodecName.LZ4);
                    break;
                case FtpTableInfo.COMPRESSION_LZO:
                    builder.withCompressionCodec(CompressionCodecName.LZO);
                    break;
                case FtpTableInfo.COMPRESSION_ZSTD:
                    builder.withCompressionCodec(CompressionCodecName.ZSTD);
                    break;
                case FtpTableInfo.COMPRESSION_NONE:
                    builder.withCompressionCodec(CompressionCodecName.UNCOMPRESSED);
                    break;
            }
        } else {
            builder.withCompressionCodec(CompressionCodecName.SNAPPY);
        }
        return builder.build();
    }

    @Override
    protected QueryMethodResult queryMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        String fileType = tableConf.getFileType();
        if (StringUtils.isBlank(fileType)) {
            throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "HDFS 查询必须指定 fileType 类型!");
        } else {
            if (!StrUtil.equalsAnyIgnoreCase(fileType, FtpTableInfo.ORC_TYPE, FtpTableInfo.PARQUET_TYPE)) {
                throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "HDFS 查询仅支持 fileType 为 ORC 或者 PARQUET!");
            }
        }
        List<FileStatus> ls = connect.ll(databaseConf.getBashPath() + tableConf.getPath());
        long count = ls.parallelStream().filter(dic -> StrUtil.equals(dic.getPath().getName(), ".meta"))
                .count();
        if (count < 1) {
            throw new BusinessException(ExceptionCode.DB_QUERY_EXCEPTION, "HDFs 查询仅支持由 [DAT] 入库的文件!");
        }

        return new QueryMethodResult();
    }

    /**
     * 读取 Orc 文件
     *
     * @param path
     * @param connect
     * @param limit
     * @param objectInspector
     * @return
     */
    private List<Map<String, Object>> readOrc(Path path, S connect, int limit, StandardStructObjectInspector objectInspector) {
        RecordReader rows = null;
        List<Map<String, Object>> schema = new ArrayList<>();
        try {
            Reader reader = OrcFile.createReader(connect.getFileSystem(), path);
            List<? extends StructField> allStructFieldRefs = objectInspector.getAllStructFieldRefs();
            rows = reader.rows();
            while (rows.hasNext()) {
                OrcStruct next = (OrcStruct) rows.next(null);
                Map<String, Object> resultMap = new HashMap<>(allStructFieldRefs.size());
                Method getFieldValue = OrcStruct.class.getDeclaredMethod("getFieldValue", int.class);
                getFieldValue.setAccessible(true);
                for (int i = 0; i < allStructFieldRefs.size(); i++) {
                    StructField structField = allStructFieldRefs.get(i);
                    WritableComparable invoke = (WritableComparable) getFieldValue.invoke(next, structField.getFieldID());
                    if (invoke instanceof TimestampWritableV2) {
                        Timestamp timestamp = ((TimestampWritableV2) invoke).getTimestamp().toSqlTimestamp();
                        resultMap.put(structField.getFieldName(), timestamp.toLocalDateTime().toString());
                    } else {
                        if (invoke == null) {
                            resultMap.put(structField.getFieldName(), "");
                        } else {
                            resultMap.put(structField.getFieldName(), invoke.toString());
                        }
                    }
                }
                schema.add(resultMap);
                if (schema.size() >= limit) {
                    return schema;
                }
            }
        } catch (Exception e) {
            throw new BusinessException("HDFS 获取 Orc schema 异常!", e);
        } finally {
            if (rows != null) {
                try {
                    rows.close();
                } catch (IOException ioException) {
                    log.error("HDFS 关闭 Orc 读取流对象异常!", ioException);
                }
            }
        }
        return schema;
    }

    /**
     * 读取 Parquet 文件
     *
     * @param path
     * @param connect
     * @param limit
     * @return
     */
    private List<Map<String, Object>> readParquet(Path path, S connect, int limit) throws Exception {
        List<Map<String, Object>> dataList = new ArrayList<>();
        readParquet(path, connect, new Function<Group, Boolean>() {
            @Override
            public Boolean apply(Group group) {
                GroupType groupType = group.getType();
                Map<String, Object> resultMap = new HashMap<>(groupType.getFieldCount());
                for (int i = 0; i < groupType.getFieldCount(); i++) {
                    String fieldName = groupType.getFieldName(i);
                    String value = null;
                    try {
                        value = group.getValueToString(i, 0);
                    } catch (RuntimeException e) {
                        if (StringUtils.startsWithIgnoreCase(e.getMessage(), "not found " + i +"(" + fieldName + ") element number 0 in group")) {
                            value = "";
                        } else {
                            throw e;
                        }
                    }
                    if (StringUtils.contains(value, "Int96Value")) {
                        Binary int96 = group.getInt96(i, 0);
                        NanoTime nanoTime = NanoTime.fromBinary(int96);
                        org.apache.hadoop.hive.common.type.Timestamp timestamp = NanoTimeUtils.getTimestamp(nanoTime, false);
                        Timestamp sqlTimestamp = timestamp.toSqlTimestamp();
                        value = sqlTimestamp.toLocalDateTime().toString();
                    }
                    resultMap.put(fieldName, value);
                }

                dataList.add(resultMap);
                if (dataList.size() >= limit) {
                    return true;
                }
                return false;
            }
        });
        return dataList;
    }

    private void readParquet(Path path, S connect, Function<Group, Boolean> function) throws Exception {
        ParquetReader<Group> reader = null;

        try {
            reader = ParquetReader.builder(new GroupReadSupport(), path).withConf(connect.getConf()).build();
            SimpleGroup group = null;
            // 读取数据
            while (true) {
                try {
                    if (!((group = (SimpleGroup) reader.read()) != null)) break;
                } catch (IOException e) {
                    throw new BusinessException("HDFS 获取 Parquet schema 异常!", e);
                }
                boolean apply = function.apply(group);
                if (apply) {
                    break;
                }
            }
        } catch (Exception e) {
            throw new BusinessException("HDFS 获取 Parquet 数据异常!", e);
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    log.error("HDFS 关闭 Parquet 读取流对象异常!", e);
                }
            }
        }
    }

    @Override
    protected QueryMethodResult countMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "HadoopServiceImpl.countMethod");
    }

    @Override
    protected QueryMethodResult testConnectMethod(S connect, D databaseConf) throws Exception {
        connect.ls("/");
        return new QueryMethodResult();
    }

    @Override
    protected QueryMethodResult showTablesMethod(S connect, Q queryEntity, D databaseConf) throws Exception {
        String bashPath = databaseConf.getBashPath();
        if (StringUtils.isBlank(bashPath)) {
            bashPath = "/";
        }
        List<FileStatus> fileStatusList = connect.ll(bashPath);
        if (CollectionUtil.isEmpty(fileStatusList)) {
            return new QueryMethodResult(0, Collections.emptyList());
        }
        List<Map<String, Object>> result = fileStatusList.stream().flatMap((fileStatus) -> {
            Map<String, Object> map = new HashMap<>(1);
            map.put("tableName", StringUtils.replace(fileStatus.getPath().getName(), "hdfs:/", ""));
            map.put("type", fileStatus.isDirectory() ? "directory" : "file");
            map.put("size", fileStatus.getLen());
            map.put("lastModified", fileStatus.getModificationTime());
            return Stream.of(map);
        }).collect(Collectors.toList());
        return new QueryMethodResult(result.size(), result);
    }

    @Override
    protected QueryMethodResult getIndexesMethod(S connect, D databaseConf, T tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "HadoopServiceImpl.getIndexesMethod");
    }

    @Override
    protected QueryMethodResult createTableMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        if (connect.exists(databaseConf.getBashPath() + tableConf.getPath())) {
            throw new BusinessException(ExceptionCode.CREATE_TABLE_EXISTS_EXCEPTION);
        }
        connect.mkdir(databaseConf.getBashPath() + tableConf.getPath());
        FtpHandler.CreateTable createTable = queryEntity.getCreateTable();
        if (createTable != null) {
            String fileType = tableConf.getFileType();
            if (StrUtil.equalsAnyIgnoreCase(fileType, FtpTableInfo.ORC_TYPE)) {
                // 获取数据
                List<Writable> orcData = createTable.getOrcData();
                StatsProvidingRecordWriter orcWriter = null;
                try {
                    if (connect.exists(databaseConf.getBashPath() + tableConf.getPath() + TABLE_META_DIC + FtpTableInfo.ORC_TYPE)) {
                        throw new BusinessException(ExceptionCode.CREATE_TABLE_EXISTS_EXCEPTION);
                    }
                    // 获取写对象
                    orcWriter = getOrcWriter(connect, databaseConf.getBashPath() + tableConf.getPath() + TABLE_META_DIC + FtpTableInfo.ORC_TYPE, tableConf.getCompressionCode(), tableConf.getBlockSize());
                    for (Writable orcDatum : orcData) {
                        orcWriter.write(orcDatum);
                    }
                } finally {
                    if (orcWriter != null) {
                        orcWriter.close(true);
                    }
                }
            } else {
                MessageType messageType = createTable.getParquetSchema();
                List<Group> parquetSchemaData = createTable.getParquetData();
                ParquetWriter<Group> parquetWriter = null;
                try {
                    if (connect.exists(databaseConf.getBashPath() + tableConf.getPath() + TABLE_META_DIC + FtpTableInfo.PARQUET_TYPE)) {
                        throw new BusinessException(ExceptionCode.CREATE_TABLE_EXISTS_EXCEPTION);
                    }
                    if(CollectionUtils.isNotEmpty(parquetSchemaData)) {
                        parquetWriter = getParquetWriter(connect.getConf(), databaseConf.getBashPath() + tableConf.getPath() + TABLE_META_DIC + FtpTableInfo.PARQUET_TYPE, messageType, tableConf.getCompressionCode());
                        for (Group parquetSchemaDatum : parquetSchemaData) {
                            parquetWriter.write(parquetSchemaDatum);
                        }
                    }
                } finally {
                    if (parquetWriter != null) {
                        parquetWriter.close();
                    }
                }
            }

        }
        return new QueryMethodResult();
    }

    private String getPath(String basePath, String subPath) {
        String path = basePath;

        if(StringUtils.isBlank(path)) {
            path = "/";
        }

        if(!path.startsWith("/")) {
            path = "/" + path;
        }

        if(path.endsWith("/")) {
            path = path.substring(0, path.length() - 1);
        }

        if(StringUtils.isNotBlank(subPath)) {
            if(subPath.startsWith("/")) {
                path += subPath;
            }else {
                path += "/" + subPath;
            }
        }

        if(path.endsWith("/")) {
            path = path.substring(0, path.length() - 1);
        }

        return path;
    }

    @Override
    protected QueryMethodResult dropTableMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        FtpHandler.DropTable dropTable = queryEntity.getDropTable();
        Date startTime = dropTable.getStartTime();
        Date stopTime = dropTable.getStopTime();

        String path = databaseConf.getBashPath() + tableConf.getPath();

        if(StringUtils.isBlank(path) || StringUtils.equalsIgnoreCase(path, "/")) {
            throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "路径为/， 不允许进行删除操作");
        }



        return new QueryMethodResult();
    }



    @Override
    protected PageResult uploadFile(S connect, Q queryEntity, D database, T table) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + "uploadFile");
    }

    @Override
    protected FileTreeNode ll(S connect, Q queryEntity, D database, T table) throws Exception {
        QueryFileTreeNode queryFileTreeNode = queryEntity.getQueryFileTreeNode();
        // 层级
        Integer hierarchy = queryFileTreeNode.getHierarchy();
        String bashPath = database.getBashPath();
        String path = table.getPath();
        // 根路径
        String rootPath =  bashPath + path;
        if(StringUtils.isBlank(rootPath)) {
            rootPath = "/";
        }
        /**
         * 根路径层级下的文件和目录
         */
        if (!connect.exists(rootPath)) {
            throw new BusinessException(ExceptionCode.DB_QUERY_EXCEPTION, "当前路径不存在: " + rootPath);
        }

        // 如果是文件则直接组装不需要递归
        if (connect.isFile(rootPath)) {
            List<FileStatus> fileStatusList = connect.ll(new Path(rootPath));
            if (fileStatusList.size() == 1) {
                FileStatus fileStatus = fileStatusList.get(0);
                FileTreeNode fileTreeNode = FileTreeNode.builder().name(rootPath).isFile(true).isDir(false)
                        .hitTotal(0L).total(0L)
                        .modificationTime(fileStatus.getModificationTime())
                        .length(fileStatus.getLen())
                        .build();
                return fileTreeNode;
            }
        }

        List<FileStatus> fileStatusList = connect.ll(new Path(rootPath));
        return getFileTreeNode(connect, queryFileTreeNode.getFilter(), rootPath, fileStatusList, hierarchy);
    }

    @Override
    protected StreamIterator<byte[]> downFile(S connect, Q queryEntity, D database, T table) throws Exception {
        FSDataInputStream inputStream = connect.getFileSystem().open(new Path(table.getPath()));
        return new HadoopStreamIterator(inputStream);
    }

    /**
     * 获取文件树
     *
     * @param connect
     * @param filter
     * @param rootPath
     * @param fileStatusList
     * @param hierarchy
     * @return
     * @throws Exception
     */
    private static FileTreeNode getFileTreeNode(FtpClient connect, FilterFileCondition filter, String rootPath, List<FileStatus> fileStatusList, Integer hierarchy) throws Exception {
        // 文件大小过滤
        FilterFileCondition.FileLengthCondition fileLengthCondition = null;
        if (filter != null) {
            fileLengthCondition = filter.getFileLength();
        }
        FileTreeNode.FileTreeNodeBuilder fileTreeNodeBuilder = FileTreeNode.builder().name(rootPath).isFile(false).isDir(true);
        Long total = 0L;
        Long hitTotal = 0L;
        for (FileStatus fileStatus : fileStatusList) {
            long len = fileStatus.getLen();
            boolean directory = fileStatus.isDirectory();

            if (!directory) {
                total++;
            }

            // 文件大小过滤
            if (!directory && fileLengthCondition != null) {
                Long minLength = fileLengthCondition.getMinLength();
                Long maxLength = fileLengthCondition.getMaxLength();
                if (minLength != null && len < minLength) {
                    continue;
                }
                if (maxLength != null && len > maxLength) {
                    continue;
                }
            }

            if (!directory) {
                hitTotal++;
            }

            String name = fileStatus.getPath().getName();
            long modificationTime = fileStatus.getModificationTime();
            FileTreeNode.FileTreeNodeBuilder childrenBuilder = FileTreeNode.builder()
                    .isFile(!directory)
                    .isDir(directory)
                    .name(name)
                    .modificationTime(modificationTime)
                    .length(len);

            // 递归层级
            if (directory && hierarchy != null && (hierarchy > 1 || hierarchy == -1)) {
                List<FileStatus> childrenFileStatusList = connect.ll(fileStatus.getPath());
                FileTreeNode fileTreeNode = getFileTreeNode(connect, filter, fileStatus.getPath().getName(), childrenFileStatusList, hierarchy == -1 ? hierarchy : hierarchy--);
                List<FileTreeNode> children = fileTreeNode.getChildren();
                if (children != null) {
                    childrenBuilder.children(children);
                }
                if (fileTreeNode.getTotal() != null) {
                    childrenBuilder.total(fileTreeNode.getTotal());
                }
                if (fileTreeNode.getHitTotal() != null) {
                    childrenBuilder.hitTotal(fileTreeNode.getHitTotal());
                }
            }
            fileTreeNodeBuilder.children(
                    childrenBuilder.build()
            );
        }
        FileTreeNode fileTreeNode = fileTreeNodeBuilder.total(total).hitTotal(hitTotal).build();
        return fileTreeNode;
    }

    @Override
    public boolean mkdirs(DatabaseSetting databaseSetting, String targetPath) throws Exception {
        return false;
    }

    @Override
    public boolean exists(DatabaseSetting databaseSetting, String targetPath) throws Exception {
        return false;
    }

    @Override
    public boolean isFile(DatabaseSetting databaseSetting, String targetPath) throws Exception {
        return false;
    }

    @Override
    public boolean isDirectory(DatabaseSetting databaseSetting, String targetPath) throws Exception {
        return false;
    }

    @Override
    public void copyFromLocalFile(DatabaseSetting databaseSetting, String src, String dst, boolean delSrc, boolean overwrite) throws Exception {

    }

    @Override
    public void moveFromLocalFile(DatabaseSetting databaseSetting, String[] srcList, String dst, boolean delSrc, boolean overwrite) throws Exception {

    }

    @Override
    public void moveFromLocalFile(DatabaseSetting databaseSetting, String src, String dst, boolean delSrc, boolean overwrite) throws Exception {

    }

    @Override
    public void copyToLocalFile(DatabaseSetting databaseSetting, String src, String dst, boolean delSrc, boolean useRawLocalFileSystem) throws Exception {

    }

    /**
     * 删除回调
     */
    interface DeletePathCallBack {
        void delete() throws Exception;
    }

    @Override
    protected QueryMethodResult deleteIndexMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "HadoopServiceImpl.deleteIndexMethod");
    }

    @Override
    protected QueryMethodResult createIndexMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "HadoopServiceImpl.createIndexMethod");
    }

    @Override
    protected QueryMethodResult insertMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        List<Map<String, Object>> rowData = queryEntity.getInsert().getRowData();
        Boolean commitNow = queryEntity.getInsert().getCommitNow();
        if (commitNow == null) {
            commitNow = false;
        }
        String fileType = tableConf.getFileType();
        String cacheKey = helper.getCacheKey(databaseConf, tableConf);
        String flagKey = tableConf.getTableName() + "$" + cacheKey;
        // 获取锁
        ReentrantLock lock = getWriteLock(flagKey);

        return new QueryMethodResult(rowData.size(), null);
    }

    @Override
    protected QueryMethodResult updateMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "HadoopServiceImpl.updateMethod");
    }

    @Override
    protected QueryMethodResult delMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "HadoopServiceImpl.delMethod");
    }

    @Override
    protected QueryMethodResult monitorStatusMethod(S connect, D databaseConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "HadoopServiceImpl.monitorStatusMethod");

    }

    @Override
    protected QueryMethodResult querySchemaMethod(S connect, D databaseConf, T tableConf) throws Exception {
        return null;
    }

    @Override
    protected QueryMethodResult queryDatabaseSchemaMethod(S connect, D databaseConf, T tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + ".queryDatabaseSchemaMethod");
    }

    @Override
    protected QueryCursorMethodResult queryCursorMethod(S connect, Q queryEntity, D databaseConf, T tableConf, C cursorCache, Consumer<Map<String, Object>> consumer, Boolean rollAllData) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "HadoopServiceImpl.queryCursorMethod");
    }

    @Override
    protected QueryMethodResult saveOrUpdateMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "HadoopServiceImpl.saveOrUpdateMethod");
    }

    @Override
    protected QueryMethodResult saveOrUpdateBatchMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "HadoopServiceImpl.saveOrUpdateBatchMethod");
    }

    @Override
    protected QueryMethodResult alterTableMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        String path = databaseConf.getBashPath() + tableConf.getPath();
        FtpHandler.AlterTable alterTable = queryEntity.getAlterTable();
        String newTableName = alterTable.getNewTableName();
        // 若存在则报错
        if (connect.exists(newTableName) && connect.isFile(newTableName)) {
            throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "HDFS 移动或重命名路径文件: " + newTableName + "已存在!");
        }

        if(!connect.exists(path)) {
            throw new BusinessException(ExceptionCode.HDFS_NOT_EXISTS_EXCEPTION, path);
        }


        boolean result = connect.getFileSystem().rename(new Path(path), new Path(newTableName));
        if(result) {
            return new QueryMethodResult();
        }
        throw new BusinessException("重命名或移动文件失败!");
    }

    @Override
    protected QueryMethodResult tableExistsMethod(S connect, D databaseConf, T tableConf) throws Exception {
        boolean exists = connect.exists(databaseConf.getBashPath() + tableConf.getPath());
        Map<String, Object> existsMap = new HashMap<>(1);
        existsMap.put(tableConf.getTableName(), exists);
        return new QueryMethodResult(1, CollectionUtil.newArrayList(existsMap));
    }

    @Override
    protected QueryMethodResult queryTableInformationMethod(S connect, D databaseConf, T tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "hadoopServiceImpl.queryTableInformationMethod");
    }

    @Override
    protected QueryMethodResult queryListDatabaseMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + ".queryListDatabaseMethod");
    }

    @Override
    protected QueryMethodResult queryDatabaseInformationMethod(S connect, Q queryEntity, D databaseConf, T tableConf) {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + ".queryDatabaseInformationMethod");
    }

    @Override
    protected QueryMethodResult mergeDataMethod(S connect, Q queryEntity, D databaseConf, T tableConf) throws Exception {

        if (!StrUtil.equalsIgnoreCase(tableConf.getFileType(), FtpTableInfo.PARQUET_TYPE)) {
            throw new BusinessException(ExceptionCode.HDFS_MERGE_FILE_TYPE_EXCEPTION, FtpTableInfo.PARQUET_TYPE);
        }

        FtpHandler.MergeData mergeData = queryEntity.getMergeData();
        String src = mergeData.getSrc();
        Long blockSize = mergeData.getBlockSize();
        // 占比
        Double mergeScale = mergeData.getMergeScale();

        if (blockSize == null) {
            blockSize = tableConf.getBlockSize();
        }

        // 需要合并的小文件大小
        long maxMergeSize = mergeData.getMaxMergeSize();

        // 需要扫描小文件的路径
        String scanMergePath = databaseConf.getBashPath() + tableConf.getPath();

        /**
         * 排除的文件或者目录
         */
        List<String> excludeFiles = mergeData.getExcludeFiles();
        List<String> excludePaths = mergeData.getExcludePaths();
        // 分区目录存储时，可配置当前扫描的分区范围
        Integer exePeriodValue = mergeData.getExePeriodValue();

        // 如果参数传入特定的路径，则使用指定的路径进行扫描
        if (StringUtils.isNotBlank(src)) {
            scanMergePath = src;
        }

        // 存储需要合并的小文件和路径
        Map<String, List<FileStatus>> mergeFiles = new HashMap<>();

        // 扫描需要合并的小文件
        scanMergeFile(connect, maxMergeSize, scanMergePath, excludeFiles, excludePaths, exePeriodValue, mergeScale, mergeFiles, tableConf.getFileType());

        List<Map<String, Object>> mergeDataResultList = new ArrayList<>();
        Map<String, Exception> errorMsg = new HashMap<>();
        // 根据目录合并小文件
        if (CollectionUtil.isNotEmpty(mergeFiles)) {
            for (Map.Entry<String, List<FileStatus>> entry : mergeFiles.entrySet()) {
                String path = entry.getKey();
                List<FileStatus> fileStatusList = entry.getValue();
                if (fileStatusList.size() <= 1) {
                    if (log.isDebugEnabled()) {
                        log.debug("HDfs path: {} 小文件数只有一个，暂时不进行合并!", path);
                    }
                    continue;
                }
                try {
                    MergeDataResult mergeDataResult = mergeDataTask(connect, path, fileStatusList, blockSize, tableConf.getFileType(), tableConf.getCompressionCode());
                    mergeDataResultList.add(JsonUtil.entityToMap(mergeDataResult));
                } catch (Exception e) {
                    errorMsg.put(path, e);
                    log.error("HDFs path: {} 小文件合并任务失败!", path, e);
                }
            }
            // 判断是否存在错误
            if (errorMsg.size() > 0) {
                // 存在合并错误
                StringBuilder sb = new StringBuilder();
                for (Map.Entry<String, Exception> entry : errorMsg.entrySet()) {
                    String path = entry.getKey();
                    Exception value = entry.getValue();
                    sb.append("path: [").append(path).append("] error: [").append(value.getMessage()).append("]\n");
                }
                throw new BusinessException(ExceptionCode.HDFS_MERGE_EXCEPTION, sb.toString());
            }
        }

        return new QueryMethodResult(mergeDataResultList.size(), mergeDataResultList);
    }

    /**
     * 文件合并任务
     *
     * @param connect
     * @param path
     * @param fileStatusList
     * @param blockSize
     * @param fileType
     * @param compressionCode
     * @throws Exception
     */
    protected MergeDataResult mergeDataTask(S connect, String path, List<FileStatus> fileStatusList, long blockSize, String fileType, String compressionCode) throws Exception {
        String mergePath = path + "/.merge_tmp";
        if (!connect.exists(mergePath)) {
            connect.mkdir(mergePath);
        }

        // 任务ID
        String taskId = UUID.randomUUID().toString().replaceAll("-", "") + "_" + DateUtil.current();

        mergePath = mergePath + "/" + taskId;

        if (!connect.exists(mergePath)) {
            connect.mkdir(mergePath);
        }

        try {
            // 获取 schema
            HadoopInputFile hadoopInputFile = HadoopInputFile.fromPath(fileStatusList.get(0).getPath(), connect.getConf());
            ParquetFileReader parquetFileReader = ParquetFileReader.open(hadoopInputFile);
            ParquetMetadata metadata = parquetFileReader.getFooter();
            MessageType parquetSchema = metadata.getFileMetaData().getSchema();

            // 遍历合并小文件
            for (FileStatus fileStatus : fileStatusList) {
                Path filePath = fileStatus.getPath();
                // parquet 文件
                if (StrUtil.equalsIgnoreCase(fileType, FtpTableInfo.PARQUET_TYPE)) {
                    String finalMergePath = mergePath;
                    readParquet(filePath, connect, new Function<Group, Boolean>() {
                        @Override
                        public Boolean apply(Group group) {
                            // 需要做个回调，从回调中获取 写对象，并判断大小
                            try {
                                mergeDataTask(taskId, connect.getConf(), filePath.toUri().getPath(), finalMergePath, parquetSchema, compressionCode, blockSize, group);
                                return false;
                            } catch (Exception e) {
                                throw new BusinessException("读取小文件数据合并到新文件执行异常! msg: " + e.getMessage(), e);
                            }
                        }
                    });
                }
            }
        } catch (Throwable e) {
            log.warn("HDFs path: {} 小文件合并任务异常, 将关闭正在合并的写入流以及删除对应的临时合并目录!", path);

            // 处理异常
            MergeDataTask<ParquetWriter<Group>> mergeDataTask = mergeDataTaskParquetCache.remove(taskId);
            ParquetWriter<Group> write = mergeDataTask.getWrite();
            if (write != null) {
                write.close();
            }
            // 删除临时合并文件目录
            connect.rm(mergePath);

            throw e;
        }

        // 小文件合并完成，迁移合并文件并删除小文件
        MergeDataTask<ParquetWriter<Group>> mergeDataTask = mergeDataTaskParquetCache.remove(taskId);
        ParquetWriter<Group> write = mergeDataTask.getWrite();
        if (write != null) {
            write.close();
        }
        // 合并生成的文件
        Set<String> mergeFiles = mergeDataTask.getMergeFiles();
        // 已经被合并的小文件
        Set<String> targetFiles = mergeDataTask.getTargetFiles();

        // 被合并的目标小文件
        List<String> mergeTargetFiles = new ArrayList<>();

        // 合并生成的文件
        List<String> mergeGenerateFiles = new ArrayList<>();

        // 迁移合并文件到正式目录
        for (String mergeFile : mergeFiles) {
            String replace = StringUtils.replace(mergeFile, "/.merge_tmp/" + taskId, "");
            String parentPath = StringUtils.substringBeforeLast(replace, "/");
            log.info("HDfs merge file mv {} to {}", mergeFile, replace);
            if (!connect.exists(parentPath)) {
                connect.mkdir(parentPath);
            }
            connect.rename(mergeFile, replace);
            // 记录合并生成文件
            mergeGenerateFiles.add(StringUtils.substringAfterLast(replace, "/"));
        }

        /**
         * 迁移已被合并的文件到备份目录
         */
        for (String targetFile : targetFiles) {
            String parentPath = StringUtils.substringBeforeLast(targetFile, "/");
            String fileName = StringUtils.substringAfterLast(targetFile, "/");
            String mergeTargetBackupPath = parentPath + "/.merge_backup";
            String backUpsPath = mergeTargetBackupPath + "/" + fileName;
            log.info("HDfs backup file mv {} to {}", targetFile, backUpsPath);
            if (!connect.exists(mergeTargetBackupPath)) {
                connect.mkdir(mergeTargetBackupPath);
            }
            connect.rename(targetFile, backUpsPath);
            // 记录被合并的小文件
            mergeTargetFiles.add(fileName);
        }

        // 删除临时合并文件目录
        connect.rm(mergePath);

        return MergeDataResult.builder().mergeTargetFiles(mergeTargetFiles).mergeGenerateFiles(mergeGenerateFiles).parentPath(path).build();
    }

    /**
     * 合并文件任务
     *
     * @param taskId
     * @param configuration
     * @param targetFile
     * @param mergePath
     * @param schema
     * @param compressionCode
     * @param blockSize
     * @param group
     * @throws Exception
     */
    protected void mergeDataTask(String taskId, Configuration configuration, String targetFile, String mergePath, MessageType schema, String compressionCode, long blockSize, Group group) throws Exception {
        MergeDataTask<ParquetWriter<Group>> mergeDataTask = mergeDataTaskParquetCache.get(taskId);
        ParquetWriter<Group> parquetWriter;

        if (mergeDataTask == null) {
            // 第一次开启任务
            mergeDataTask = MergeDataTask.<ParquetWriter<Group>>builder().taskId(taskId)
                            .targetFiles(new LinkedHashSet<>())
                                    .mergeFiles(new LinkedHashSet<>())
                                            .build();
            mergeDataTaskParquetCache.put(taskId, mergeDataTask);
        }

        parquetWriter = mergeDataTask.getWrite();

        if (parquetWriter == null) {
            // 第一次开启任务或者上个写对象已经满足最大块
            String mergeFile = mergePath + "/" + UUID.randomUUID().toString().replaceAll("-", "") + "." + FtpTableInfo.PARQUET_TYPE;
            parquetWriter = getParquetWriter(configuration, mergeFile, schema, compressionCode);
            mergeDataTask.setWrite(parquetWriter);
            mergeDataTask.getMergeFiles().add(mergeFile);
        }

        parquetWriter.write(group);

        // 记录当前读取的小文件
        mergeDataTask.getTargetFiles().add(targetFile);

        // 满足最大块条件
        if (parquetWriter.getDataSize() >= blockSize) {
            // 关闭 parquet 写对象
            parquetWriter.close();
            mergeDataTask.setWrite(null);
        }
    }

    /**
     * 扫描需要合并的文件
     *
     * @param connect
     * @param maxMergeSize
     * @param scanMergePath
     * @param excludeFiles
     * @param excludePaths
     * @param mergeFiles
     * @param fileType
     * @param exePeriodValue
     * @param mergeScale
     * @throws Exception
     */
    protected void scanMergeFile(S connect, long maxMergeSize, String scanMergePath, List<String> excludeFiles, List<String> excludePaths, Integer exePeriodValue, Double mergeScale, Map<String, List<FileStatus>> mergeFiles, String fileType) throws Exception {
        // 符合条件的文件数
        Double total = 0D;
        // 符合小文件的条件的文件树
        Double hitTotal = 0D;

        List<FileStatus> ls = connect.ll(scanMergePath);
        // 先过滤一下目录
        List<FileStatus> scanFiles = ls.parallelStream().filter(dic ->
        {
            boolean a = !StrUtil.equalsAnyIgnoreCase(dic.getPath().getName(), ".meta", ".tmp", ".merge_tmp", ".merge_backup");
            if (!a) {
                return false;
            }

            // 分区过滤
            if (exePeriodValue != null && dic.isDirectory()) {
                String name = dic.getPath().getName();
                Matcher matcher = PATTERN_PARTITION_REGEX.matcher(name);
                if (matcher.matches()) {
                    // 匹配提取日期
                    String group = matcher.group(1);
                    if (StrUtil.length(group) == 4) {
                        // 年
                        int yyyy = Integer.valueOf(group);
                        int year = DateUtil.year(DateUtil.date());
                        if (yyyy > year && yyyy < (year - exePeriodValue)) {
                            return false;
                        }
                    } else if (StrUtil.length(group) == 7) {
                        // 年月
                        DateTime parse = DateUtil.parse(group, "yyyy-MM");
                        long month = DateUtil.betweenMonth(parse, DateUtil.date(), false);
                        if (month > exePeriodValue) {
                            return false;
                        }
                    } else if (StrUtil.length(group) == 10) {
                        // 年月日
                        DateTime parse = DateUtil.parse(group, "yyyy-MM-dd");
                        long day = DateUtil.betweenDay(parse, DateUtil.date(), false);
                        if (day > exePeriodValue) {
                            return false;
                        }
                    }
                } else {
                    // 不匹配则排除
                    return false;
                }
            }
            // 排除目录
            if (CollectionUtil.isNotEmpty(excludePaths)) {
                boolean b = !StrUtil.equalsAnyIgnoreCase(dic.getPath().getName()
                        , excludePaths.toArray(new String[excludePaths.size()]));
                return b;
            }
            return true;
        }).collect(Collectors.toList());

        if (CollectionUtil.isEmpty(scanFiles)) {
            log.info("path: {} 路径下暂无任何文件或目录，不需要执行合并!", scanMergePath);
        }

        for (FileStatus scanFile : scanFiles) {
            if (scanFile.isDirectory()) {
                // 子目录
                scanMergeFile(connect, maxMergeSize, scanFile.getPath().toUri().getPath(), excludeFiles, excludePaths, exePeriodValue, mergeScale, mergeFiles, fileType);
            } else {
                // 文件
                if (!StrUtil.endWithIgnoreCase(scanFile.getPath().getName(), fileType)) {
                    continue;
                }
                // 先过滤一下不进行合并的文件
                if (CollectionUtil.isNotEmpty(excludeFiles)) {
                    boolean flag = false;
                    for (String excludeFile : excludeFiles) {
                        if (StrUtil.startWith(excludeFile, "*.")) {
                            // 匹配文件后缀
                            if (StrUtil.endWithIgnoreCase(scanFile.getPath().getName(), StrUtil.subAfter(excludeFile, "*.", true))) {
                                flag = true;
                                break;
                            }
                        } else {
                            if (StrUtil.equalsIgnoreCase(scanFile.getPath().getName(), excludeFile)) {
                                // 匹配排除的文件名称
                                flag = true;
                                break;
                            }
                        }
                    }
                    if (flag) {
                        if (log.isDebugEnabled()) {
                            log.debug("HDFS File: {} 命中排除文件，跳过合并!", scanFile.getPath().getName());
                        }
                        continue;
                    }
                }
                total++;
                // 文件大小
                long len = scanFile.getLen();
                if (len < maxMergeSize) {
                    // 小于合并文件大小则记录下来
                    List<FileStatus> fileStatuses = mergeFiles.get(scanMergePath);
                    if (fileStatuses == null) {
                        fileStatuses = new ArrayList<>();
                        mergeFiles.put(scanMergePath, fileStatuses);
                    }
                    fileStatuses.add(scanFile);
                    hitTotal++;
                }
            }
        }
        List<FileStatus> fileStatusList = mergeFiles.get(scanMergePath);
        if (mergeScale != null && CollectionUtil.isNotEmpty(fileStatusList)) {
            // 计算百分比，超过设置的值才需要进行合并
            // 计算百分比
            double div = NumberUtil.div(hitTotal, total);
            if (div < mergeScale) {
                mergeFiles.remove(scanMergePath);
                if (log.isDebugEnabled()) {
                    log.debug("HDFs path: {} 小文件占比为: {} 未达到设置的合并阈值: {}, 不执行小文件合并!", scanMergePath, div, mergeScale);
                }
            }
        }

    }
}
