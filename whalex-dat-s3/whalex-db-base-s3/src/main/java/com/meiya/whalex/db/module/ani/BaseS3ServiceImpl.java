package com.meiya.whalex.db.module.ani;

import cn.hutool.core.map.MapUtil;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.util.IOUtils;
import com.meiya.whalex.annotation.Support;
import com.meiya.whalex.db.constant.SupportPower;
import com.meiya.whalex.db.entity.*;
import com.meiya.whalex.db.entity.ani.S3BucketInfo;
import com.meiya.whalex.db.entity.ani.S3DatabaseInfo;
import com.meiya.whalex.db.entity.ani.S3Handle;
import com.meiya.whalex.db.entity.ani.S3RecordInfo;
import com.meiya.whalex.db.entity.ani.S3StreamIterator;
import com.meiya.whalex.db.stream.StreamIterator;
import com.meiya.whalex.exception.BusinessException;
import com.meiya.whalex.exception.ExceptionCode;
import com.meiya.whalex.filesystem.entity.FileTreeNode;
import com.meiya.whalex.filesystem.module.AbstractDistributedFileSystemBaseService;
import com.meiya.whalex.interior.db.search.in.QueryParamCondition;
import com.meiya.whalex.interior.db.search.in.Where;
import com.meiya.whalex.util.JsonUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * @author 黄河森
 * @date 2019/12/28
 * @project whale-cloud-platformX
 */
@Support(value = {
        SupportPower.TEST_CONNECTION,
        SupportPower.CREATE,
        SupportPower.DELETE,
        SupportPower.SEARCH,
        SupportPower.SHOW_TABLE_LIST
})
@Slf4j
public class BaseS3ServiceImpl extends AbstractDistributedFileSystemBaseService<AmazonS3, S3Handle, S3DatabaseInfo, S3BucketInfo, AbstractCursorCache> {

    @Override
    protected QueryMethodResult queryMethod(AmazonS3 connect, S3Handle queryEntity, S3DatabaseInfo databaseConf, S3BucketInfo tableConf) {
        List<String> keys = queryEntity.getS3QueryOrDel().getKeys();

        List<Map<String, Object>> rows = new ArrayList<>();
        AtomicBoolean success = new AtomicBoolean(true);
        keys.parallelStream().forEach(key -> {
            try {
                S3Object o = connect.getObject(tableConf.getBucketName(), key);
                Map<String, Object> dataMap = new HashMap<>();
                dataMap.put("key", key);
                dataMap.put("success", true);

                if (o != null) {
                    S3ObjectInputStream s3is = o.getObjectContent();
                    ObjectMetadata metadata = o.getObjectMetadata();
                    byte[] bytes = IOUtils.toByteArray(s3is);
                    dataMap.put("data", bytes); //会自动转成base64

                    //对用户元数据进行解码
                    Map<String, String> userMetadata = metadata.getUserMetadata();
                    dataMap.put("metadata",  userMetadata);
                    if (!MapUtil.isEmpty(userMetadata)) {
                        dataMap.put("metadata", JsonUtil.jsonStrToMap(URLDecoder.decode(userMetadata.get("metadata"), "utf-8")));
                    }
                } else {
                    dataMap.put("data", null);
                }
                rows.add(dataMap);
            } catch (AmazonS3Exception e) {
                e.printStackTrace();
                success.set(false);
                Map<String, Object> dataMap = new HashMap<>();
                dataMap.put("key", key);
                dataMap.put("success", false);
                if (e.getMessage().contains("The specified bucket does not exist")) {
                    dataMap.put("data", "bucket桶不存在");
                } else if (e.getMessage().contains("The specified key does not exist")) {
                    dataMap.put("data", "文件不存在");
                } else {
                    dataMap.put("data", e.getMessage());
                }
                rows.add(dataMap);
            } catch (IOException e) {
                e.printStackTrace();
                success.set(false);
                Map<String, Object> dataMap = new HashMap<>();
                dataMap.put("key", key);
                dataMap.put("success", false);
                dataMap.put("data", "流转byte数组异常, " + e.getMessage());
                rows.add(dataMap);
            }
        });
        return new QueryMethodResult(success.get(), rows.size(), rows);
    }

    @Override
    protected QueryMethodResult countMethod(AmazonS3 connect, S3Handle queryEntity, S3DatabaseInfo databaseConf, S3BucketInfo tableConf) throws Exception {
//        S3Handle.AniQuery aniQuery = queryEntity.getAniQuery();
//        String sqlCount = aniQuery.getSqlCount();
//        Object[] paramArray = aniQuery.getParamArray();
//        sqlCount = String.format(sqlCount, jointTable(databaseConf.getBucket(), tableConf.getBucketName()));
//        queryEntity.setQueryStr(transitionQueryStr(sqlCount, paramArray));
//        Long total = 0L;
////        Map<String, Object> totalMap = connect.query(sqlCount, new MapHandler(), paramArray);
////        total = Long.parseLong(totalMap.get("count").toString());
//        return new QueryMethodResult(total, null);
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "s3 实现");
    }

    @Override
    protected QueryMethodResult testConnectMethod(AmazonS3 connect, S3DatabaseInfo databaseConf) throws Exception {
        List<Bucket> buckets = connect.listBuckets();
        return new QueryMethodResult();
    }

    @Override
    protected QueryMethodResult showTablesMethod(AmazonS3 connect, S3Handle queryEntity, S3DatabaseInfo databaseConf) throws Exception {
        List<Bucket> buckets = connect.listBuckets();
        List<Map<String, Object>> query = new ArrayList<>();
        for (Bucket bucket : buckets) {
            Map<String, Object> map = new HashMap<>();
            map.put("tableName", bucket.getName());
            query.add(map);
        }

        return new QueryMethodResult(query.size(), query);
    }

    @Override
    protected QueryMethodResult getIndexesMethod(AmazonS3 connect, S3DatabaseInfo databaseConf, S3BucketInfo tableConf) throws Exception {
        QueryMethodResult queryMethodResult = new QueryMethodResult();
//        List<Map<String, Object>> dataList = new ArrayList<>();
//        queryMethodResult.setRows(dataList);
//        String showIndexSql = String.format(SHOW_INDEX_SQL, tableConf.getBucketName(), tableConf.getBucketName().toLowerCase(), databaseConf.getBucket());
//        List<Map<String, Object>> queryList = Collections.emptyList();
////        List<Map<String, Object>> queryList = connect.query(showIndexSql, new MapListHandler());
//        if (CollectionUtil.isNotEmpty(queryList)) {
//            for (int i = 0; i < queryList.size(); i++) {
//                Map<String, Object> map = queryList.get(i);
//                Map<String, Object> resultMap = new HashMap<>(1);
//                dataList.add(resultMap);
//                Object columnName = map.get("indexdef");
//                if (columnName != null) {
//                    String columnNameStr = String.valueOf(columnName);
//                    columnNameStr = StringUtils.substringBetween(columnNameStr, "(", ")");
//                    columnNameStr = StringUtils.replace(columnNameStr, ", ", ",");
//                    resultMap.put("column", columnNameStr);
//                }
//            }
//        }
        return queryMethodResult;
    }

    @Override
    protected QueryMethodResult createTableMethod(AmazonS3 connect, S3Handle queryEntity, S3DatabaseInfo databaseConf, S3BucketInfo tableConf) throws Exception {
        String bucketName = tableConf.getBucketName();
        connect.createBucket(bucketName);
        return new QueryMethodResult();
    }

    @Override
    protected QueryMethodResult dropTableMethod(AmazonS3 connect, S3Handle queryEntity, S3DatabaseInfo databaseConf, S3BucketInfo tableConf) throws Exception {
//        String dropTableSQL = String.format(DROP_TABLE_SQL, jointTable(databaseConf.getBucket(), tableConf.getBucketName()));
        int update = 0;
//        int update = connect.update(dropTableSQL);
        connect.deleteBucket(tableConf.getBucketName());
        return new QueryMethodResult(update, null);
    }

    @Override
    protected QueryMethodResult deleteIndexMethod(AmazonS3 connect, S3Handle queryEntity, S3DatabaseInfo databaseConf, S3BucketInfo tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "PostGreServiceImpl.deleteIndexMethod");
    }

    @Override
    protected QueryMethodResult createIndexMethod(AmazonS3 connect, S3Handle queryEntity, S3DatabaseInfo databaseConf, S3BucketInfo tableConf) throws Exception {
//        String sql = queryEntity.getAniCreateIndex().getSql();
//        sql = StringUtils.replace(sql, "${tableName}", jointTable(databaseConf.getBucket(), tableConf.getBucketName()));
////        connect.update(sql);
//        return new QueryMethodResult();
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "s3 实现");
    }

    @Override
    protected QueryMethodResult insertMethod(AmazonS3 connect, S3Handle queryEntity, S3DatabaseInfo databaseConf, S3BucketInfo tableConf) throws Exception {
        List<Map<String, Object>> rows = new ArrayList<>();
        List<S3RecordInfo> records = queryEntity.getS3Insert().getRecords();
        ObjectMetadata objectMetadata = new ObjectMetadata();
        AtomicBoolean success = new AtomicBoolean(true);
        records.parallelStream().forEach(s3RecordInfo -> {
            try {
                byte[] decode = Base64.getDecoder().decode(s3RecordInfo.getData());

                //设置文件大小
                objectMetadata.setContentLength(decode.length);
                //由于元数据中包含中文时会报异常,故在新增时进行转码处理
                //注意,在查询时同时需要解码处理
                if (!MapUtil.isEmpty(s3RecordInfo.getMetadata())) {
                    HashMap<String, String> userMetadata = new HashMap<>();
                    userMetadata.put("metadata", URLEncoder.encode(JsonUtil.objectToStr(s3RecordInfo.getMetadata()), "UTF-8"));
                    objectMetadata.setUserMetadata(userMetadata);
                }

                //插入对象
                connect.putObject(tableConf.getBucketName(), s3RecordInfo.getKey(), new ByteArrayInputStream(decode), objectMetadata);

                //上传成功
                Map<String, Object> map = new HashMap<>();
                map.put("key", s3RecordInfo.getKey());
                map.put("success", true);
                rows.add(map);
            } catch (AmazonServiceException e) {
                e.printStackTrace();
                success.set(false);
                //上传失败
                Map<String, Object> map = new HashMap<>();
                map.put("key", s3RecordInfo.getKey());
                map.put("success", false);
                map.put("data", e.getMessage());
                rows.add(map);
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
                success.set(false);
                //上传失败
                Map<String, Object> map = new HashMap<>();
                map.put("key", s3RecordInfo.getKey());
                map.put("success", false);
                map.put("data", "对象元数据转码失败");
                rows.add(map);
            }
        });

        return new QueryMethodResult(success.get(), rows.size(), rows);
    }

    @Override
    protected QueryMethodResult updateMethod(AmazonS3 connect, S3Handle queryEntity, S3DatabaseInfo
            databaseConf, S3BucketInfo tableConf) throws Exception {
//        String sql = queryEntity.getAniUpdate().getSql();
//        sql = StringUtils.replace(sql, "${tableName}", jointTable(databaseConf.getBucket(), tableConf.getBucketName()));
////        int update = connect.update(sql);
//        return new QueryMethodResult(0, null);
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "s3 实现");
    }

    @Override
    protected QueryMethodResult delMethod(AmazonS3 connect, S3Handle queryEntity, S3DatabaseInfo
            databaseConf, S3BucketInfo tableConf) throws Exception {
        List<String> keys = queryEntity.getS3QueryOrDel().getKeys();

        List<Map<String, Object>> rows = new ArrayList<>();
        keys.parallelStream().forEach(key -> {
            try {
                connect.deleteObject(tableConf.getBucketName(), key);
                Map<String, Object> dataMap = new HashMap<>();
                dataMap.put("key", key);
                dataMap.put("success", true);
                rows.add(dataMap);
            } catch (Exception e) {
                Map<String, Object> dataMap = new HashMap<>();
                dataMap.put("key", key);
                dataMap.put("success", false);

                if (e.getMessage().contains("The specified bucket does not exist")) {
                    dataMap.put("data", "bucket桶不存在");
                } else {
                    dataMap.put("data", "出现异常: " + e.getMessage());
                }
                rows.add(dataMap);
            }
        });
        return new QueryMethodResult(rows.size(), rows);
    }

    @Override
    protected QueryMethodResult monitorStatusMethod(AmazonS3 connect, S3DatabaseInfo databaseConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "PostGreServiceImpl.monitorStatusMethod");
    }

    @Override
    protected QueryMethodResult querySchemaMethod(AmazonS3 connect, S3DatabaseInfo databaseConf, S3BucketInfo
            tableConf) throws BusinessException {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + ".querySchemaMethod");
    }

    @Override
    protected QueryMethodResult queryDatabaseSchemaMethod(AmazonS3 connect, S3DatabaseInfo databaseConf, S3BucketInfo tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + ".queryDatabaseSchemaMethod");
    }

    @Override
    protected QueryCursorMethodResult queryCursorMethod(AmazonS3 connect, S3Handle queryEntity, S3DatabaseInfo databaseConf, S3BucketInfo tableConf, AbstractCursorCache cursorCache, Consumer<Map<String, Object>> consumer, Boolean rollAllData) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "S3ServiceImpl.queryCursorMethod");
    }

    @Override
    protected QueryMethodResult saveOrUpdateMethod(AmazonS3 connect, S3Handle queryEntity, S3DatabaseInfo databaseConf, S3BucketInfo tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "S3ServiceImpl.saveOrUpdateMethod");
    }

    @Override
    protected QueryMethodResult saveOrUpdateBatchMethod(AmazonS3 connect, S3Handle queryEntity, S3DatabaseInfo databaseConf, S3BucketInfo tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "S3ServiceImpl.saveOrUpdateBatchMethod");
    }

    @Override
    protected QueryMethodResult alterTableMethod(AmazonS3 connect, S3Handle queryEntity, S3DatabaseInfo databaseConf, S3BucketInfo tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "S3ServiceImpl.alterTableMethod");
    }

    @Override
    protected QueryMethodResult tableExistsMethod(AmazonS3 connect, S3DatabaseInfo databaseConf, S3BucketInfo tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "S3ServiceImpl.tableExistsMethod");
    }

    @Override
    protected QueryMethodResult queryTableInformationMethod(AmazonS3 connect, S3DatabaseInfo databaseConf, S3BucketInfo tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + ".queryTableInformationMethod");
    }

    @Override
    protected QueryMethodResult queryListDatabaseMethod(AmazonS3 connect, S3Handle queryEntity, S3DatabaseInfo databaseConf, S3BucketInfo tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + ".queryListDatabaseMethod");
    }

    @Override
    protected QueryMethodResult queryDatabaseInformationMethod(AmazonS3 connect, S3Handle queryEntity, S3DatabaseInfo databaseConf, S3BucketInfo tableConf) {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + ".queryDatabaseInformationMethod");
    }

    @Override
    protected S3Handle transitionQueryParam(QueryParamCondition queryParamCondition, S3DatabaseInfo databaseInfo, S3BucketInfo bucketInfo) throws Exception {
        List<Where> wheres = queryParamCondition.getWhere();
        if (CollectionUtils.isEmpty(wheres)) {
            throw new BusinessException("未提供下载条件");
        }
        S3Handle s3Handle = new S3Handle();
        S3Handle.S3QueryOrDel s3QueryOrDel = new S3Handle.S3QueryOrDel();
        s3Handle.setS3QueryOrDel(s3QueryOrDel);
        s3QueryOrDel.setKeys(getS3Handle(wheres));
        return s3Handle;
    }

    private List<String> getS3Handle(List<Where> wheres) {
        if (CollectionUtils.isEmpty(wheres)) {
            return Collections.emptyList();
        }
        List<String> keys = new ArrayList<>();
        wheres.forEach(where1 -> {
            if (StringUtils.isNotBlank(where1.getField()) &&
                    StringUtils.equals("key", where1.getField()) &&
                    where1.getParam() instanceof String &&
                    StringUtils.isNotBlank((String) where1.getParam())) {
                keys.add((String) where1.getParam());
            }
            keys.addAll(getS3Handle(where1.getParams()));

        });
        return keys;
    }

    @Override
    protected S3Handle transitionAddParam(AddParamCondition addParamCondition, S3DatabaseInfo databaseInfo, S3BucketInfo bucketInfo) throws Exception {
        List<Map<String, Object>> fieldValueList = addParamCondition.getFieldValueList();
        if (CollectionUtils.isEmpty(fieldValueList)) {
            throw new BusinessException("未传入上传数据");
        }

        S3Handle.S3Insert s3Insert = new S3Handle.S3Insert();
        s3Insert.setRecords(new ArrayList<>());
        fieldValueList.forEach(fieldValue -> {
            s3Insert.getRecords().add(JsonUtil.jsonStrToObject(JsonUtil.objectToStr(fieldValue), S3RecordInfo.class));
        });

        S3Handle s3Handle = new S3Handle();
        s3Handle.setS3Insert(s3Insert);
        return s3Handle;
    }

    @Override
    protected S3Handle transitionUpdateParam(UpdateParamCondition updateParamCondition, S3DatabaseInfo databaseInfo, S3BucketInfo bucketInfo) throws Exception {
        return super.transitionUpdateParam(updateParamCondition, databaseInfo, bucketInfo);
    }

    @Override
    protected S3Handle transitionDelParam(DelParamCondition delParamCondition, S3DatabaseInfo databaseInfo, S3BucketInfo bucketInfo) throws Exception {
        List<Where> wheres = delParamCondition.getWhere();
        if (CollectionUtils.isEmpty(wheres)) {
            throw new BusinessException("未提供删除条件");
        }
        S3Handle s3Handle = new S3Handle();
        S3Handle.S3QueryOrDel s3QueryOrDel = new S3Handle.S3QueryOrDel();
        s3Handle.setS3QueryOrDel(s3QueryOrDel);
        s3QueryOrDel.setKeys(getS3Handle(wheres));
        return s3Handle;
    }

    @Override
    protected PageResult uploadFile(AmazonS3 connect, S3Handle queryEntity, S3DatabaseInfo database, S3BucketInfo table) throws Exception {
        S3Handle.UploadFile uploadFile = queryEntity.getUploadFile();
        InputStream inputStream = uploadFile.getInputStream();
        String key = uploadFile.getKey();
        if(inputStream == null) {
            throw new RuntimeException("数据源不能为空");
        }
        PageResult pageResult = new PageResult();
        List<Map<String, Object>> rows = new ArrayList<>();
        pageResult.setRows(rows);
        try {
            String bucketName = table.getBucketName();
            InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(bucketName, key);
            InitiateMultipartUploadResult initResponse = connect.initiateMultipartUpload(initRequest);

            byte[] buffer = new byte[5 * 1024 * 1024];
            int partNumber = 1;
            List<PartETag> partETags = new ArrayList<>();
            int bytesRead;

            while ((bytesRead = inputStream.read(buffer)) != - 1) {
                if(bytesRead == 0) continue;
                ByteArrayInputStream partInputStream = new ByteArrayInputStream(buffer, 0, bytesRead);
                //上传分块
                UploadPartRequest uploadPartRequest = new UploadPartRequest()
                        .withBucketName(bucketName)
                        .withKey(key)
                        .withUploadId(initResponse.getUploadId())
                        .withPartNumber(partNumber)
                        .withInputStream(partInputStream)
                        .withPartSize(bytesRead);
                PartETag partETag = connect.uploadPart(uploadPartRequest).getPartETag();
                partETags.add(partETag);
                partNumber++;
            }
            CompleteMultipartUploadRequest completeRequest = new CompleteMultipartUploadRequest(bucketName, key, initResponse.getUploadId(), partETags);
            connect.completeMultipartUpload(completeRequest);
            //上传成功
            Map<String, Object> map = new HashMap<>();
            map.put("key", key);
            map.put("success", true);
            pageResult.setTotal(1);
        } catch (AmazonServiceException e) {
            e.printStackTrace();
            pageResult.setSuccess(false);
            pageResult.setMessage(e.getMessage());
            //上传失败
            Map<String, Object> map = new HashMap<>();
            map.put("key", key);
            map.put("success", false);
            map.put("data", e.getMessage());
            rows.add(map);
            pageResult.setTotal(1);
        }

        return pageResult;
    }

    @Override
    protected FileTreeNode ll(AmazonS3 connect, S3Handle queryEntity, S3DatabaseInfo database, S3BucketInfo table) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + ".ll");
    }

    @Override
    protected StreamIterator<byte[]> downFile(AmazonS3 connect, S3Handle queryEntity, S3DatabaseInfo database, S3BucketInfo table) throws Exception {
        S3Handle.DownFile downFile = queryEntity.getDownFile();
        String key = downFile.getKey();

        S3Object o = connect.getObject(table.getBucketName(), key);
        if (o != null) {
            S3ObjectInputStream s3is = o.getObjectContent();
            return new S3StreamIterator(s3is);
        } else {
            throw new RuntimeException("文件不存在[" + key + "]");
        }
    }

    @Override
    public boolean mkdirs(DatabaseSetting databaseSetting, String targetPath) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + ".mkdirs");
    }

    @Override
    public boolean exists(DatabaseSetting databaseSetting, String targetPath) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + ".exists");
    }

    @Override
    public boolean isFile(DatabaseSetting databaseSetting, String targetPath) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + ".isFile");
    }

    @Override
    public boolean isDirectory(DatabaseSetting databaseSetting, String targetPath) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + ".isDirectory");
    }

    @Override
    public void copyFromLocalFile(DatabaseSetting databaseSetting, String src, String dst, boolean delSrc, boolean overwrite) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + ".copyFromLocalFile");
    }

    @Override
    public void moveFromLocalFile(DatabaseSetting databaseSetting, String[] srcList, String dst, boolean delSrc, boolean overwrite) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + ".moveFromLocalFile");
    }

    @Override
    public void moveFromLocalFile(DatabaseSetting databaseSetting, String src, String dst, boolean delSrc, boolean overwrite) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + ".moveFromLocalFile");
    }

    @Override
    public void copyToLocalFile(DatabaseSetting databaseSetting, String src, String dst, boolean delSrc, boolean useRawLocalFileSystem) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + ".copyToLocalFile");
    }
}
