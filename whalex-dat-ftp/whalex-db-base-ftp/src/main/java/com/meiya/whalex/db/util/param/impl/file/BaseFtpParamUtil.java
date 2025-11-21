package com.meiya.whalex.db.util.param.impl.file;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.util.StrUtil;
import com.meiya.whalex.db.entity.*;
import com.meiya.whalex.db.entity.file.FtpDatabaseInfo;
import com.meiya.whalex.db.entity.file.FtpHandler;
import com.meiya.whalex.db.entity.file.FtpTableInfo;
import com.meiya.whalex.exception.BusinessException;
import com.meiya.whalex.exception.ExceptionCode;
import com.meiya.whalex.filesystem.entity.QueryFileTreeNode;
import com.meiya.whalex.filesystem.entity.UploadFileParam;
import com.meiya.whalex.filesystem.util.param.AbstractFileSystemParserUtil;
import com.meiya.whalex.interior.db.constant.PartitionType;
import com.meiya.whalex.interior.db.operation.in.*;
import com.meiya.whalex.interior.db.search.in.QueryParamCondition;
import com.meiya.whalex.util.date.DateUtil;
import com.meiya.whalex.util.date.JodaTimeUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.io.Writable;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.schema.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 参数转换实体
 *
 * @author 黄河森
 * @date 2019/12/30
 * @project whale-cloud-platformX
 */
@Slf4j
public class BaseFtpParamUtil<Q extends FtpHandler, D extends FtpDatabaseInfo,
        T extends FtpTableInfo> extends AbstractFileSystemParserUtil<Q, D, T> {
    @Override
    protected Q transitionListTableParam(QueryTablesCondition queryTablesCondition, D databaseConf) throws Exception {
        return (Q) new FtpHandler();
    }

    @Override
    protected Q transitionListDatabaseParam(QueryDatabasesCondition queryDatabasesCondition, D databaseConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + ".transitionListDatabaseParam");
    }

    @Override
    protected Q transitionMergeDataParam(MergeDataParamCondition mergeDataParamCondition, D databaseConf, T tableConf) throws Exception {

        if(StringUtils.isBlank(tableConf.getPath()) && StringUtils.isBlank(mergeDataParamCondition.getSrc())) {
            throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "src 不能为空");
        }

        if(mergeDataParamCondition.getMaxMergeSize() <= 0) {
            throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "最大合并大小必须大于0");
        }

        FtpHandler.MergeData mergeData = new FtpHandler.MergeData();
        mergeData.setSrc(mergeDataParamCondition.getSrc());
        mergeData.setMaxMergeSize(mergeDataParamCondition.getMaxMergeSize());
        mergeData.setBlockSize(mergeDataParamCondition.getBlockSize());
        mergeData.setExcludeFiles(mergeDataParamCondition.getExcludeFiles());
        mergeData.setExcludePaths(mergeDataParamCondition.getExcludePaths());
        mergeData.setExePeriodValue(mergeDataParamCondition.getExePeriodValue());
        mergeData.setMergeScale(mergeDataParamCondition.getMergeScale());

        FtpHandler ftpHandler = new FtpHandler();
        ftpHandler.setMergeData(mergeData);
        return (Q) ftpHandler;
    }

    @Override
    protected Q transitionCreateTableParam(CreateTableParamCondition createTableParamCondition, D databaseConf, T tableConf) throws Exception {
        FtpHandler ftpHandler = new FtpHandler();
        String fileType = tableConf.getFileType();
        if (StrUtil.equalsAnyIgnoreCase(fileType, FtpTableInfo.ORC_TYPE)) {
            StandardStructObjectInspector inspector = ObjectInspectorFactory.getStandardStructObjectInspector(
                    CollectionUtil.newArrayList("name", "type", "length", "isPartition", "partitionFormat", "path"),
                    CollectionUtil.newArrayList(
                            ObjectInspectorFactory.getReflectionObjectInspector(String.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA),
                            ObjectInspectorFactory.getReflectionObjectInspector(String.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA),
                            ObjectInspectorFactory.getReflectionObjectInspector(Integer.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA),
                            ObjectInspectorFactory.getReflectionObjectInspector(Boolean.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA),
                            ObjectInspectorFactory.getReflectionObjectInspector(String.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA),
                            ObjectInspectorFactory.getReflectionObjectInspector(String.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA)
                    )
            );
            List<CreateTableParamCondition.CreateTableFieldParam> createTableFieldParamList = createTableParamCondition.getCreateTableFieldParamList();
            List<Writable> writableList = new ArrayList<>(createTableFieldParamList.size());
            for (CreateTableParamCondition.CreateTableFieldParam createTableFieldParam : createTableFieldParamList) {
                OrcSerde orcSerde = new OrcSerde();
                String fieldName = createTableFieldParam.getFieldName();
                String fieldType = createTableFieldParam.getFieldType();
                Integer fieldLength = createTableFieldParam.getFieldLength();
                boolean partition = createTableFieldParam.isPartition();
                List<Object> recordList = CollectionUtil.newArrayList(fieldName, fieldType, fieldLength, partition);
                PartitionInfo partitionInfo = createTableFieldParam.getPartitionInfo();
                if (partitionInfo != null) {
                    String partitionFormat = partitionInfo.getPartitionFormat();
                    if (StringUtils.isBlank(partitionFormat)) {
                        partitionFormat = "";
                    }
                    recordList.add(partitionFormat);
                    PartitionType type = partitionInfo.getType();
                    List<PartitionInfo.PartitionEq> eq = partitionInfo.getEq();
                    if (PartitionType.EQ != null && PartitionType.EQ != type) {
                        throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "HDFs orc/parquet 分区仅支持 EQ 类型分区!");
                    }
                    if (CollectionUtil.isNotEmpty(eq)) {
                        PartitionInfo.PartitionEq partitionEq = eq.get(0);
                        String path = partitionEq.getPath();
                        if (StringUtils.isBlank(path)) {
                            throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "HDFs orc/parquet 设置分区必须指定分区文件夹前缀!");
                        } else {
                            if (!StringUtils.startsWithIgnoreCase(path, "/")) {
                                path = "/" + path;
                            }
                            if (StringUtils.endsWithIgnoreCase(path, "/")) {
                                path = StringUtils.substringBeforeLast(path, "/");
                            }
                        }
                        recordList.add(path);
                    }
                } else {
                    recordList.add("");
                    recordList.add("");
                }
                Writable serialize = orcSerde.serialize(recordList, inspector);
                writableList.add(serialize);
            }
            FtpHandler.CreateTable createTable = FtpHandler.CreateTable.builder().orcData(writableList).orcSchema(inspector).build();
            ftpHandler.setCreateTable(createTable);
        } else {
            List<Group> rows = new ArrayList<>();
            // 否则均以 parquet 格式作为元数据文件读取
            MessageType metadataSchema = Types.buildMessage()
                    .optional(PrimitiveType.PrimitiveTypeName.BINARY)
                    .as(OriginalType.UTF8)
                    .named("name")
                    .optional(PrimitiveType.PrimitiveTypeName.BINARY)
                    .as(OriginalType.UTF8)
                    .named("type")
                    .optional(PrimitiveType.PrimitiveTypeName.INT32)
                    .as(OriginalType.INT_32)
                    .named("length")
                    .optional(PrimitiveType.PrimitiveTypeName.BOOLEAN)
                    .named("isPartition")
                    .optional(PrimitiveType.PrimitiveTypeName.BINARY)
                    .as(OriginalType.UTF8)
                    .named("partitionFormat")
                    .optional(PrimitiveType.PrimitiveTypeName.BINARY)
                    .as(OriginalType.UTF8)
                    .named("path")
                    .named(tableConf.getTableName());
            SimpleGroupFactory simpleGroupFactory = new SimpleGroupFactory(metadataSchema);

            List<CreateTableParamCondition.CreateTableFieldParam> createTableFieldParamList = createTableParamCondition.getCreateTableFieldParamList();
            for (CreateTableParamCondition.CreateTableFieldParam createTableFieldParam : createTableFieldParamList) {
                Group group = simpleGroupFactory.newGroup();
                String fieldName = createTableFieldParam.getFieldName();
                String fieldType = createTableFieldParam.getFieldType();
                Integer fieldLength = createTableFieldParam.getFieldLength();
                boolean partition = createTableFieldParam.isPartition();
                if (fieldLength == null) {
                    fieldLength = 0;
                }
                group.append("name", fieldName);
                group.append("type", fieldType);
                group.append("length", fieldLength);
                group.append("isPartition", partition);
                PartitionInfo partitionInfo = createTableFieldParam.getPartitionInfo();
                if (partitionInfo != null) {
                    String partitionFormat = partitionInfo.getPartitionFormat();
                    if (StringUtils.isBlank(partitionFormat)) {
                        partitionFormat = "";
                    }
                    group.append("partitionFormat", partitionFormat);
                    PartitionType type = partitionInfo.getType();
                    List<PartitionInfo.PartitionEq> eq = partitionInfo.getEq();
                    if (PartitionType.EQ != null && PartitionType.EQ != type) {
                        throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "HDFs orc/parquet 分区仅支持 EQ 类型分区!");
                    }
                    if (CollectionUtil.isNotEmpty(eq)) {
                        PartitionInfo.PartitionEq partitionEq = eq.get(0);
                        String path = partitionEq.getPath();
                        if (StringUtils.isBlank(path)) {
                            throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "HDFs orc/parquet 设置分区必须指定分区文件夹前缀!");
                        } else {
                            if (!StringUtils.startsWithIgnoreCase(path, "/")) {
                                path = "/" + path;
                            }
                            if (StringUtils.endsWithIgnoreCase(path, "/")) {
                                path = StringUtils.substringBeforeLast(path, "/");
                            }
                        }
                        group.append("path", path);
                    }
                } else {
                    group.append("partitionFormat", "");
                    group.append("path", "");
                }
                rows.add(group);
            }

            FtpHandler.CreateTable createTable = FtpHandler.CreateTable.builder().parquetData(rows).parquetSchema(metadataSchema).build();
            ftpHandler.setCreateTable(createTable);
        }
        return (Q) ftpHandler;
    }

    @Override
    protected Q transitionAlterTableParam(AlterTableParamCondition alterTableParamCondition, D databaseConf, T tableConf) throws Exception {
        FtpHandler ftpHandler = new FtpHandler();
        String newTableName = alterTableParamCondition.getNewTableName();
        if(StringUtils.isBlank(newTableName)) {
            throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "newTableName不能为空");
        } else {
            if (!StringUtils.startsWithIgnoreCase(newTableName, "/")) {
                newTableName = "/" + newTableName;
            }
        }
        FtpHandler.AlterTable alterTable = new FtpHandler.AlterTable();
        alterTable.setNewTableName(newTableName);
        ftpHandler.setAlterTable(alterTable);
        return (Q) ftpHandler;
    }

    @Override
    protected Q transitionCreateIndexParam(IndexParamCondition indexParamCondition, D databaseConf, T tableConf) throws Exception {
        return (Q) new FtpHandler();
    }

    @Override
    protected Q transitionDropIndexParam(IndexParamCondition indexParamCondition, D databaseConf, T tableConf) throws Exception {
        return (Q) new FtpHandler();
    }

    @Override
    protected Q transitionQueryParam(QueryParamCondition queryParamCondition, D databaseConf, T tableConf) throws Exception {
        FtpHandler.Query query = FtpHandler.Query.builder().limit(10).build();
        FtpHandler ftpHandler = new FtpHandler();
        ftpHandler.setQuery(query);
        return (Q) ftpHandler;
    }

    @Override
    protected Q transitionUpdateParam(UpdateParamCondition updateParamCondition, D databaseConf, T tableConf) throws Exception {
        return (Q) new FtpHandler();
    }

    @Override
    protected Q transitionInsertParam(AddParamCondition addParamCondition, D databaseConf, T tableConf) throws Exception {
        List<Map<String, Object>> fieldValueList = addParamCondition.getFieldValueList();
        FtpHandler.Insert insert = FtpHandler.Insert.builder().rowData(fieldValueList).commitNow(addParamCondition.getCommitNow()).build();
        FtpHandler ftpHandler = new FtpHandler();
        ftpHandler.setInsert(insert);
        return (Q) ftpHandler;
    }

    @Override
    protected Q transitionDeleteParam(DelParamCondition delParamCondition, D databaseConf, T tableConf) throws Exception {
        return (Q) new FtpHandler();
    }

    @Override
    protected Q transitionDropTableParam(DropTableParamCondition dropTableParamCondition, D databaseConf, T tableConf) throws Exception {
        FtpHandler.DropTable dropTable = new FtpHandler.DropTable();
        FtpHandler ftpHandler = new FtpHandler();
        ftpHandler.setDropTable(dropTable);
        if (dropTableParamCondition != null) {
            if (StringUtils.isNotBlank(dropTableParamCondition.getStartTime())) {
                dropTable.setStartTime(DateUtil.convertToDate(dropTableParamCondition.getStartTime(), JodaTimeUtil.DEFAULT_YMD_FORMAT));
            }
            if (StringUtils.isNotBlank(dropTableParamCondition.getEndTime())) {
                dropTable.setStopTime(DateUtil.convertToDate(dropTableParamCondition.getEndTime(), JodaTimeUtil.DEFAULT_YMD_FORMAT));
            }
        }
        return (Q) ftpHandler;
    }

    @Override
    public Q transitionUpsertParam(UpsertParamCondition paramCondition, D databaseConf, T tableConf) throws Exception {
        return (Q) new FtpHandler();
    }

    @Override
    public Q transitionUpsertParamBatch(UpsertParamBatchCondition paramCondition, D databaseConf, T tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + "transitionUpsertParamBatch");
    }

    @Override
    public Q queryFileTreeNodeParser(QueryFileTreeNode queryFileTreeNode, D databaseConf, T tableConf) throws Exception {
        FtpHandler ftpHandler = new FtpHandler();
        ftpHandler.setQueryFileTreeNode(queryFileTreeNode);
        return (Q) ftpHandler;
    }

    @Override
    public Q uploadFileParser(UploadFileParam uploadFileParam, D database, T table) {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, this.getClass().getSimpleName() + "uploadFileParser");
    }

    @Override
    public Q downFileParser(String path, D database, T table) {
        if(StringUtils.isBlank(path)) {
            table.setPath(path);
        }
        return null;
    }

}
