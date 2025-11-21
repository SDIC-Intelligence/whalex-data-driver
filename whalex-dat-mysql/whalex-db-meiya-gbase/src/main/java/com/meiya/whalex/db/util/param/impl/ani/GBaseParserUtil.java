package com.meiya.whalex.db.util.param.impl.ani;

import com.meiya.whalex.db.constant.IndexType;
import com.meiya.whalex.db.entity.FieldTypeAdapter;
import com.meiya.whalex.db.entity.IndexParamCondition;
import com.meiya.whalex.db.entity.ani.AniHandler;
import com.meiya.whalex.db.entity.ani.BaseMySqlDatabaseInfo;
import com.meiya.whalex.db.entity.ani.BaseMySqlFieldTypeEnum;
import com.meiya.whalex.db.entity.ani.BaseMySqlTableInfo;
import com.meiya.whalex.interior.db.operation.in.CreateTableParamCondition;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 关系型数据库通用，解析查询实体转换为SQL
 *
 * @author 蔡荣桂
 * @date 2022/05/23
 * @project whale-cloud-platformX
 */
@Slf4j
public class GBaseParserUtil extends BaseMySqlParserUtil {

    /**
     * 建表模板
     */
    protected final static String CREATE_TABLE_TEMPLATE = "CREATE TABLE `%s` ( %s ) COMMENT='%s' DEFAULT CHARSET=utf8";

    @Override
    public AniHandler.AniCreateTable parserCreateTableSql(CreateTableParamCondition createTableParamCondition, BaseMySqlDatabaseInfo baseMySqlDatabaseInfo, BaseMySqlTableInfo baseMySqlTableInfo) {
        AniHandler.AniCreateTable createTable = new AniHandler.AniCreateTable();
        List<CreateTableParamCondition.CreateTableFieldParam> createTableFieldParamList = createTableParamCondition.getCreateTableFieldParamList();
        StringBuilder createSqlBuilder = new StringBuilder();
        // 存放主键
        List<String> primaryKeyList = new LinkedList<>();
        createTableFieldParamList.forEach(createTableFieldParam -> {
            String fieldType = createTableFieldParam.getFieldType();
            String fieldComment = createTableFieldParam.getFieldComment();
            Integer fieldLength = createTableFieldParam.getFieldLength();
            String fieldName = createTableFieldParam.getFieldName();
            Integer fieldDecimalPoint = createTableFieldParam.getFieldDecimalPoint();
            FieldTypeAdapter adapter = getAdapter(fieldType);
            createSqlBuilder.append(DOUBLE_QUOTATION_MARKS)
                    .append(fieldName)
                    .append(DOUBLE_QUOTATION_MARKS + " ")
                    .append(buildSqlDataType(adapter, fieldLength, fieldDecimalPoint));

            if (adapter.isUnsigned()) {
                if (createTableFieldParam.getUnsigned() != null && createTableFieldParam.getUnsigned()) {
                    createSqlBuilder.append(" UNSIGNED");
                }
            }

            if (createTableFieldParam.isNotNull()) {
                createSqlBuilder.append(" NOT NULL");
            }
            if (StringUtils.isNotBlank(fieldComment)) {
                createSqlBuilder.append(" COMMENT ")
                        .append("'")
                        .append(fieldComment)
                        .append("'");
            }
            createSqlBuilder.append(",");
            if (createTableFieldParam.isPrimaryKey()) {
                primaryKeyList.add(fieldName);
            }
        });
        createSqlBuilder.deleteCharAt(createSqlBuilder.length() - 1);
        if (CollectionUtils.isNotEmpty(primaryKeyList)) {
            createSqlBuilder.append(",PRIMARY KEY (");
            primaryKeyList.forEach(primaryKey -> {
                createSqlBuilder.append(DOUBLE_QUOTATION_MARKS)
                        .append(primaryKey)
                        .append(DOUBLE_QUOTATION_MARKS)
                        .append(",");
            });
            createSqlBuilder.deleteCharAt(createSqlBuilder.length() - 1);
            createSqlBuilder.append(")");
        }
        String tableComment = createTableParamCondition.getTableComment();
        createTable.setSql(String.format(CREATE_TABLE_TEMPLATE, baseMySqlTableInfo.getTableName(), createSqlBuilder.toString(), StringUtils.isNotBlank(tableComment) ? tableComment : ""));
        return createTable;
    }

    @Override
    public AniHandler.AniCreateIndex parserCreateIndexSql(IndexParamCondition indexParamCondition, BaseMySqlDatabaseInfo baseMySqlDatabaseInfo, BaseMySqlTableInfo baseMySqlTableInfo) {
        if (indexParamCondition.getIndexType() != null && indexParamCondition.getIndexType().equals(IndexType.UNIQUE)) {
            log.warn("GBase 不支持唯一索引, 将忽略该参数!");
            indexParamCondition.setIndexType(null);
        }

        AniHandler.AniCreateIndex aniCreateIndex = super.parserCreateIndexSql(indexParamCondition, baseMySqlDatabaseInfo, baseMySqlTableInfo);
        aniCreateIndex.setSql(aniCreateIndex.getSql());
        return aniCreateIndex;
    }


}
