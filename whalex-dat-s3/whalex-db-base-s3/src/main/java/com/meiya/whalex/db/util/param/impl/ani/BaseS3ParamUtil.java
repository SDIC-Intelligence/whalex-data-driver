package com.meiya.whalex.db.util.param.impl.ani;

import com.meiya.whalex.annotation.DbParamUtil;
import com.meiya.whalex.db.entity.ani.S3BucketInfo;
import com.meiya.whalex.db.entity.ani.S3DatabaseInfo;
import com.meiya.whalex.db.entity.ani.S3RecordInfo;
import com.meiya.whalex.exception.BusinessException;
import com.meiya.whalex.exception.ExceptionCode;
import com.meiya.whalex.filesystem.entity.QueryFileTreeNode;
import com.meiya.whalex.filesystem.entity.UploadFileParam;
import com.meiya.whalex.filesystem.util.param.AbstractFileSystemParserUtil;
import com.meiya.whalex.interior.db.constant.CloudVendorsEnum;
import com.meiya.whalex.interior.db.constant.DbResourceEnum;
import com.meiya.whalex.db.entity.*;
import com.meiya.whalex.db.entity.ani.S3Handle;
import com.meiya.whalex.db.util.param.AbstractDbModuleParamUtil;
import com.meiya.whalex.interior.db.constant.DbVersionEnum;
import com.meiya.whalex.interior.db.operation.in.AlterTableParamCondition;
import com.meiya.whalex.interior.db.operation.in.CreateTableParamCondition;
import com.meiya.whalex.interior.db.operation.in.QueryDatabasesCondition;
import com.meiya.whalex.interior.db.operation.in.QueryTablesCondition;
import com.meiya.whalex.interior.db.search.in.QueryParamCondition;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * S3 参数转换工具类
 *
 * @author 黄河森
 * @date 2019/12/23
 * @project whale-cloud-platformX
 *
 */
@DbParamUtil(dbType = DbResourceEnum.s3, version = DbVersionEnum.S3_1_11_415, cloudVendors = CloudVendorsEnum.OPEN)
@Slf4j
public class BaseS3ParamUtil extends AbstractFileSystemParserUtil<S3Handle, S3DatabaseInfo, S3BucketInfo> {
    @Override
    protected S3Handle transitionListTableParam(QueryTablesCondition queryTablesCondition, S3DatabaseInfo databaseInfo) throws Exception {
        return new S3Handle();
    }

    @Override
    protected S3Handle transitionListDatabaseParam(QueryDatabasesCondition queryDatabasesCondition, S3DatabaseInfo databaseInfo) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + ".queryListDatabaseMethod");
    }

    @Override
    protected S3Handle transitionCreateTableParam(CreateTableParamCondition createTableParamCondition, S3DatabaseInfo databaseInfo, S3BucketInfo bucketInfo) {
        S3Handle s3Handle = new S3Handle();
//        s3Handle.setAniCreateTable(S3ParserUtil.parserCreateTableSql(createTableParamCondition));
        return s3Handle;
    }

    @Override
    protected S3Handle transitionAlterTableParam(AlterTableParamCondition alterTableParamCondition, S3DatabaseInfo databaseInfo, S3BucketInfo bucketInfo) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "S3ParamUtil.transitionAlterTableParam");
    }

    @Override
    protected S3Handle transitionCreateIndexParam(IndexParamCondition indexParamCondition, S3DatabaseInfo databaseInfo, S3BucketInfo bucketInfo) {
        S3Handle s3Handle = new S3Handle();
        return s3Handle;
    }

    @Override
    protected S3Handle transitionDropIndexParam(IndexParamCondition indexParamCondition, S3DatabaseInfo databaseInfo, S3BucketInfo bucketInfo) {
        S3Handle s3Handle = new S3Handle();
        return s3Handle;
    }

    @Override
    protected S3Handle transitionQueryParam(QueryParamCondition queryParamCondition, S3DatabaseInfo databaseInfo, S3BucketInfo bucketInfo) {
        S3Handle s3Handle = new S3Handle();
//        S3Handle.AniQuery aniQuery;
//        // 如果存在自定义SQL，则不需要转换
//        String sql = queryParamCondition.getSql();
//        if (StringUtils.isNotBlank(sql)) {
//            aniQuery = new S3Handle.AniQuery();
//            aniQuery.setSql(sql);
//        } else {
//            aniQuery = S3ParserUtil.parserQuerySql(queryParamCondition);
//            aniQuery.setCount(queryParamCondition.isCountFlag());
//        }
//        s3Handle.setAniQuery(aniQuery);
        return s3Handle;
    }

    @Override
    protected S3Handle transitionUpdateParam(UpdateParamCondition updateParamCondition, S3DatabaseInfo databaseInfo, S3BucketInfo bucketInfo) {
        S3Handle s3Handle = new S3Handle();
//        S3Handle.AniUpdate aniUpdate = S3ParserUtil.parserUpdateSql(updateParamCondition);
//        s3Handle.setAniUpdate(aniUpdate);
        return s3Handle;
    }

    @Override
    protected S3Handle transitionInsertParam(AddParamCondition addParamCondition, S3DatabaseInfo databaseInfo, S3BucketInfo bucketInfo) {
        S3Handle s3Handle = new S3Handle();
        List<Map<String, Object>> fieldValueList = addParamCondition.getFieldValueList();
        if(CollectionUtils.isEmpty(fieldValueList)) {
            throw new RuntimeException("上传数据不能为空");
        }
        S3Handle.S3Insert s3Insert = new S3Handle.S3Insert();
        List<S3RecordInfo> records = new ArrayList<>();
        s3Insert.setRecords(records);
        for (Map<String, Object> map : fieldValueList) {
            String key = (String) map.get(S3RecordInfo.KEY);
            String data = (String) map.get(S3RecordInfo.DATA);
            Map<String, Object> metadata = (Map<String, Object>) map.get(S3RecordInfo.METADATA);
            if(StringUtils.isBlank(key)) {
                throw new RuntimeException("key不能为空");
            }
            S3RecordInfo s3RecordInfo = new S3RecordInfo();
            s3RecordInfo.setData(data);
            s3RecordInfo.setKey(key);
            s3RecordInfo.setMetadata(metadata);
            records.add(s3RecordInfo);
        }
        s3Handle.setS3Insert(s3Insert);
        return s3Handle;
    }

    @Override
    protected S3Handle transitionDeleteParam(DelParamCondition delParamCondition, S3DatabaseInfo databaseInfo, S3BucketInfo bucketInfo) {
        S3Handle s3Handle = new S3Handle();
//        s3Handle.setAniDel(S3ParserUtil.parserDelSql(delParamCondition));
        return s3Handle;
    }

    @Override
    protected S3Handle transitionDropTableParam(DropTableParamCondition dropTableParamCondition, S3DatabaseInfo databaseInfo, S3BucketInfo bucketInfo) throws Exception {
        return new S3Handle();
    }

    @Override
    public S3Handle transitionUpsertParam(UpsertParamCondition paramCondition, S3DatabaseInfo databaseInfo, S3BucketInfo bucketInfo) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "S3ParamUtil.transitionUpsertParam");
    }

    @Override
    public S3Handle transitionUpsertParamBatch(UpsertParamBatchCondition paramCondition, S3DatabaseInfo databaseInfo, S3BucketInfo bucketInfo) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + "transitionUpsertParamBatch");
    }

    @Override
    public S3Handle queryFileTreeNodeParser(QueryFileTreeNode queryFileTreeNode, S3DatabaseInfo databaseConf, S3BucketInfo tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + "queryFileTreeNodeParser");
    }

    @Override
    public S3Handle uploadFileParser(UploadFileParam uploadFileParam, S3DatabaseInfo database, S3BucketInfo table) {
        S3Handle s3Handle = new S3Handle();
        S3Handle.UploadFile uploadFile = new S3Handle.UploadFile();
        uploadFile.setInputStream(uploadFileParam.getInputStream());
        uploadFile.setKey(uploadFileParam.getPath());
        s3Handle.setUploadFile(uploadFile);
        return s3Handle;
    }

    @Override
    public S3Handle downFileParser(String path, S3DatabaseInfo database, S3BucketInfo table) {
        S3Handle s3Handle = new S3Handle();
        S3Handle.DownFile downFile = new S3Handle.DownFile();
        downFile.setKey(path);
        s3Handle.setDownFile(downFile);
        return s3Handle;
    }
}
