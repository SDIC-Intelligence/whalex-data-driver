package com.meiya.whalex.db.util.param;

import com.meiya.whalex.db.entity.*;
import com.meiya.whalex.exception.BusinessException;
import com.meiya.whalex.exception.ExceptionCode;
import com.meiya.whalex.interior.db.operation.in.AlterTableParamCondition;
import com.meiya.whalex.interior.db.operation.in.CreateDatabaseParamCondition;
import com.meiya.whalex.interior.db.operation.in.CreateTableParamCondition;
import com.meiya.whalex.interior.db.operation.in.DropDatabaseParamCondition;
import com.meiya.whalex.interior.db.operation.in.MergeDataParamCondition;
import com.meiya.whalex.interior.db.operation.in.PublishMessage;
import com.meiya.whalex.interior.db.operation.in.QueryDatabasesCondition;
import com.meiya.whalex.interior.db.operation.in.QueryTablesCondition;
import com.meiya.whalex.interior.db.operation.in.SubscribeMessage;
import com.meiya.whalex.interior.db.operation.in.UpdateDatabaseParamCondition;
import com.meiya.whalex.interior.db.search.condition.Rel;
import com.meiya.whalex.interior.db.search.condition.Sort;
import com.meiya.whalex.interior.db.search.in.Order;
import com.meiya.whalex.interior.db.search.in.Page;
import com.meiya.whalex.interior.db.search.in.QueryParamCondition;
import com.meiya.whalex.interior.db.search.in.Where;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 各组件参数转换接口定义
 * <p>
 * Q 泛型: 转换指定组件的参数类型
 * D 泛型: 数据库配置信息实体
 * T 泛型: 数据库表配置实体
 *
 * @author 黄河森
 * @date 2019/9/13
 * @project whale-cloud-platformX
 */
public abstract class AbstractDbModuleParamUtil<Q extends AbstractDbHandler, D extends AbstractDatabaseInfo,
        T extends AbstractDbTableInfo> {

    /**
     * 正则匹配
     */
    private static Pattern LIKE_PATTERN = Pattern.compile("\\s+like\\s+");
    private static Pattern AND_PATTERN = Pattern.compile("\\s+and\\s+");
    private static List<String> sqlKey = Arrays.asList("from", "where", "group", "having", "order", "limit", "offset");

    /**
     * 获取指定组件查询实体
     *
     * @param queryParamCondition
     * @return
     */
    public Q getQueryParam(QueryParamCondition queryParamCondition, D databaseConf, T tableConf) throws Exception {
        analysisSql(queryParamCondition);
        return transitionQueryParam(queryParamCondition, databaseConf, tableConf);
    }

    /**
     * 获取指定组件插入实体
     *
     * @param addParamCondition
     * @return
     */
    public Q getAddParam(AddParamCondition addParamCondition, D databaseConf, T tableConf) throws Exception {
        return transitionInsertParam(addParamCondition, databaseConf, tableConf);
    }

    /**
     * 获取指定组件更新实体
     *
     * @param updateParamCondition
     * @return
     */
    public Q getUpdateParam(UpdateParamCondition updateParamCondition, D databaseConf, T tableConf) throws Exception {
        return transitionUpdateParam(updateParamCondition, databaseConf, tableConf);
    }

    /**
     * 获取指定组件删除实体
     *
     * @param delParamCondition
     * @return
     */
    public Q getDelParam(DelParamCondition delParamCondition, D databaseConf, T tableConf) throws Exception {
        return transitionDeleteParam(delParamCondition, databaseConf, tableConf);
    }

    /**
     * 获取指定组件表创建实体
     *
     * @param createTableParamCondition
     * @return
     */
    public Q getCreateTableParam(CreateTableParamCondition createTableParamCondition, D databaseConf, T tableConf) throws Exception {
        return transitionCreateTableParam(createTableParamCondition, databaseConf, tableConf);
    }

    /**
     * 获取指定组件表修改实体
     *
     * @param alterTableParamCondition
     * @return
     */
    public Q getAlterTableParam(AlterTableParamCondition alterTableParamCondition, D databaseConf, T tableConf) throws Exception {
        return transitionAlterTableParam(alterTableParamCondition, databaseConf, tableConf);
    }

    /**
     * 获取指定组件表索引实体
     *
     * @param indexParamCondition
     * @return
     */
    public Q getCreateIndexParam(IndexParamCondition indexParamCondition, D databaseConf, T tableConf) throws Exception {
        return transitionCreateIndexParam(indexParamCondition, databaseConf, tableConf);
    }

    /**
     * 获取指定组件表索引实体
     *
     * @param indexParamCondition
     * @return
     */
    public Q getDropIndexParam(IndexParamCondition indexParamCondition, D databaseConf, T tableConf) throws Exception {
        return transitionDropIndexParam(indexParamCondition, databaseConf, tableConf);
    }

    /**
     * 获取指定组件表删除表实体
     *
     * @param dropTableParamCondition
     * @return
     */
    public Q getDropTableParam(DropTableParamCondition dropTableParamCondition, D databaseConf, T tableConf) throws Exception {
        return transitionDropTableParam(dropTableParamCondition, databaseConf, tableConf);
    }

    /**
     * 获取指定组件表清空表实体
     *
     * @param emptyTableParamCondition
     * @return
     */
    public Q getEmptyTableParam(EmptyTableParamCondition emptyTableParamCondition, D databaseConf, T tableConf) throws Exception {
        return transitionEmptyTableParam(emptyTableParamCondition, databaseConf, tableConf);
    }

    /**
     * 查询库表参数转换
     *
     * @param queryTablesCondition
     * @return
     * @throws Exception
     */
    public Q getListTableParam(QueryTablesCondition queryTablesCondition, D databaseConf) throws Exception {
        return transitionListTableParam(queryTablesCondition, databaseConf);
    }

    /**
     * 查询库表参数转换
     *
     * @param queryTablesCondition
     * @return
     */
    protected abstract Q transitionListTableParam(QueryTablesCondition queryTablesCondition, D databaseConf) throws Exception;

    /**
     * 查询库参数转换
     *
     * @param queryDatabasesCondition
     * @return
     */
    protected abstract Q transitionListDatabaseParam(QueryDatabasesCondition queryDatabasesCondition, D databaseConf) throws Exception;

    /**
     * 建表参数转换
     *
     * @param createTableParamCondition
     * @return
     * @throws Exception
     */
    protected abstract Q transitionCreateTableParam(CreateTableParamCondition createTableParamCondition, D databaseConf, T tableConf) throws Exception;

    /**
     * 修改表参数转换
     *
     * @param alterTableParamCondition
     * @return
     * @throws Exception
     */
    protected abstract Q transitionAlterTableParam(AlterTableParamCondition alterTableParamCondition, D databaseConf, T tableConf) throws Exception;

    /**
     * 建索引参数转换
     *
     * @param indexParamCondition
     * @param databaseConf
     * @param tableConf
     * @return
     * @throws Exception
     */
    protected abstract Q transitionCreateIndexParam(IndexParamCondition indexParamCondition, D databaseConf, T tableConf) throws Exception;

    /**
     * 删除索引参数转换
     *
     * @param indexParamCondition
     * @param databaseConf
     * @param tableConf
     * @return
     * @throws Exception
     */
    protected abstract Q transitionDropIndexParam(IndexParamCondition indexParamCondition, D databaseConf, T tableConf) throws Exception;

    /**
     * 查询参数转换
     *
     * @param queryParamCondition
     * @return
     * @throws Exception
     */
    protected abstract Q transitionQueryParam(QueryParamCondition queryParamCondition, D databaseConf, T tableConf) throws Exception;

    /**
     * 更新参数转换
     *
     * @param updateParamCondition
     * @return
     * @throws Exception
     */
    protected abstract Q transitionUpdateParam(UpdateParamCondition updateParamCondition, D databaseConf, T tableConf) throws Exception;

    /**
     * 新增参数转换
     *
     * @param addParamCondition
     * @return
     * @throws Exception
     */
    protected abstract Q transitionInsertParam(AddParamCondition addParamCondition, D databaseConf, T tableConf) throws Exception;

    /**
     * 删除参数转换
     *
     * @param delParamCondition
     * @return
     * @throws Exception
     */
    protected abstract Q transitionDeleteParam(DelParamCondition delParamCondition, D databaseConf, T tableConf) throws Exception;

    /**
     * 删表参数转换
     *
     * @param dropTableParamCondition
     * @return
     * @throws Exception
     */
    protected abstract Q transitionDropTableParam(DropTableParamCondition dropTableParamCondition, D databaseConf, T tableConf) throws Exception;

    /**
     * 清空表参数转换
     *
     * @param emptyTableParamCondition
     * @return
     * @throws Exception
     */
    protected Q transitionEmptyTableParam(EmptyTableParamCondition emptyTableParamCondition, D databaseConf, T tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "AbstractDbModuleParamUtil.transitionEmptyTableParam");

    }

    /**
     * 新增或者更新参数转换
     *
     * @param paramCondition
     * @return
     * @throws Exception
     */
    public abstract Q transitionUpsertParam(UpsertParamCondition paramCondition, D databaseConf, T tableConf) throws Exception;

    /**
     * 批量新增或者更新参数转换
     * @param paramCondition
     * @return
     */
    public abstract Q transitionUpsertParamBatch(UpsertParamBatchCondition paramCondition, D databaseConf, T tableConf) throws Exception;

    /**
     * 合并数据
     *
     * @param mergeDataParamCondition
     * @return
     */
    protected Q transitionMergeDataParam(MergeDataParamCondition mergeDataParamCondition, D databaseConf, T tableConf) throws Exception {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "AbstractDbModuleParamUtil.transitionMergeDataParam");
    }

    protected Q transitionCreateDatabaseParam(CreateDatabaseParamCondition paramCondition, D dataConf) {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "AbstractDbModuleParamUtil.transitionCreateDatabaseParam");
    }

    protected Q transitionUpdateDatabaseParam(UpdateDatabaseParamCondition paramCondition, D dataConf) {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "AbstractDbModuleParamUtil.transitionUpdateDatabaseParam");
    }

    protected Q transitionDropDatabaseParam(DropDatabaseParamCondition paramCondition, D dataConf) {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "AbstractDbModuleParamUtil.transitionDropDatabaseParam");
    }

    protected Q transitionPublishMessage(PublishMessage paramCondition, D dataConf) {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "AbstractDbModuleParamUtil.transitionPublishMessage");
    }

    protected Q transitionSubscribeMessage(SubscribeMessage paramCondition, D dataConf) {
        throw new BusinessException(ExceptionCode.METHOD_NOT_IMPLEMENTED_EXCEPTION, "AbstractDbModuleParamUtil.transitionSubscribeMessage");
    }

    /**
     * SQL 报文转换为 包装参数
     *
     * @param paramCondition
     */
    private static void analysisSql(QueryParamCondition paramCondition) {
        // 解析 sql
        if (StringUtils.isNotBlank(paramCondition.getSql())) {
            String sql = paramCondition.getSql().trim();
            transitionSqlForSelect(paramCondition, sql);
            transitionSqlForWhere(paramCondition, sql);
            transitionSqlForGroup(paramCondition, sql);
            transitionSqlForOrder(paramCondition, sql);
            transitionSqlForLimit(paramCondition, sql);
        }
    }

    private static void transitionSqlForGroup(QueryParamCondition paramCondition, String sql) {
        String orderByPatternString = "\\s+group\\s+by\\s+";
        Pattern pattern = Pattern.compile(orderByPatternString);
        Matcher matcher = pattern.matcher(sql);
        boolean isContainsGroupBy = matcher.find();
        if (isContainsGroupBy) {
            int endIndex = getEndIndex(sql.toLowerCase(), matcher.end());
            String groupByString = sql.substring(matcher.end(), endIndex);
            String[] split = groupByString.split(",");
            List<String> groups = new ArrayList<>(split.length);
            for (String item : split) {
                groups.add(StringUtils.trim(item));
            }
            paramCondition.setGroup(groups);
        }
    }

    private static int getEndIndex(String sql, int regionEnd) {
        for (String key : sqlKey) {
            int i = sql.indexOf(key, regionEnd);
            if (i > 0) {
                return i;
            }
        }
        return sql.length();
    }

    /**
     * 转换SQL查询LIMIT字段部分
     *
     * @param paramCondition
     */
    private static void transitionSqlForLimit(QueryParamCondition paramCondition, String sql) {
        if (sql.toLowerCase().contains("limit")) {
            String limit = StringUtils.trim(StringUtils.substringAfter(sql.toLowerCase(), "limit"));
            if (StringUtils.isNotBlank(limit)) {
                String[] split = limit.split(",");
                if (split.length == 1) {
                    split = limit.split("offset");
                    if (split.length == 1) {
                        paramCondition.setPage(new Page(0, Integer.valueOf(split[0])));
                        return;
                    }
                }
                paramCondition.setPage(new Page(Integer.valueOf(StringUtils.trim(split[1])), Integer.valueOf(StringUtils.trim(split[0]))));
            } else {
                paramCondition.setPage(new Page(0, 15));
            }
        } else {
            paramCondition.setPage(new Page(0, 15));
        }
    }

    /**
     * 转换SQL查询排序字段部分
     *
     * @param paramCondition
     */
    private static void transitionSqlForOrder(QueryParamCondition paramCondition, String sql) {
        String orderByPatternString = "\\s+order\\s+by\\s+";
        Pattern pattern = Pattern.compile(orderByPatternString);
        Matcher matcher = pattern.matcher(sql);
        boolean isContainsOrderBy = matcher.find();
        if (isContainsOrderBy) {
            int endIndex = getEndIndex(sql.toLowerCase(), matcher.end());
            String orderByString = sql.substring(matcher.end(), endIndex);
            String[] split = orderByString.toLowerCase().split(",");
            List<Order> orders = new ArrayList<>(split.length);
            for (String item : split) {
                Sort sort = StringUtils.trim(item).contains(Sort.DESC.getName()) ? Sort.DESC : Sort.ASC;
                String field = StringUtils.replaceEach(StringUtils.trim(item), new String[]{sort.getName(), "`", "'", "\""}, new String[]{"", "", "", ""});
                Order order = Order.create(StringUtils.trim(field), sort);
                orders.add(order);
            }
            paramCondition.setOrder(orders);
        }
    }

    /**
     * 转换SQL查询条件字段部分
     *
     * @param paramCondition
     */
    private static void transitionSqlForWhere(QueryParamCondition paramCondition, String sql) {
        String whereStr = disposeWhereSql(sql);
        if (StringUtils.isNotBlank(whereStr)) {
            whereStr = StringUtils.trim(whereStr);
            String[] whereArray;
            if (AND_PATTERN.matcher(whereStr.toLowerCase()).find()) {
                if (whereStr.contains(" and ")) {
                    whereArray = whereStr.split(" and ");
                } else {
                    whereArray = whereStr.split(" AND ");
                }
            } else {
                whereArray = new String[]{whereStr};
            }
            List<Where> whereList = new ArrayList<>();
            paramCondition.setWhere(whereList);
            for (String where : whereArray) {
                where = StringUtils.trim(where);
                if (where.contains("<")) {
                    String[] split = StringUtils.split(where, "<");
                    Where whereParam = Where.create(StringUtils.trim(split[0]), StringUtils.replaceEach(StringUtils.trim(split[1]), new String[]{"'", "\""}, new String[]{"", ""}), Rel.LT);
                    whereList.add(whereParam);
                } else if (where.contains("<=")) {
                    String[] split = StringUtils.split(where, "<=");
                    Where whereParam = Where.create(StringUtils.trim(split[0]), StringUtils.replaceEach(StringUtils.trim(split[1]), new String[]{"'", "\""}, new String[]{"", ""}), Rel.LTE);
                    whereList.add(whereParam);
                } else if (where.contains(">")) {
                    String[] split = StringUtils.split(where, ">");
                    Where whereParam = Where.create(StringUtils.trim(split[0]), StringUtils.replaceEach(StringUtils.trim(split[1]), new String[]{"'", "\""}, new String[]{"", ""}), Rel.GT);
                    whereList.add(whereParam);
                } else if (where.contains(">=")) {
                    String[] split = StringUtils.split(where, ">=");
                    Where whereParam = Where.create(StringUtils.trim(split[0]), StringUtils.replaceEach(StringUtils.trim(split[1]), new String[]{"'", "\""}, new String[]{"", ""}), Rel.GTE);
                    whereList.add(whereParam);
                } else if (where.toLowerCase().contains("between")) {
                    String[] split;
                    if (where.contains("between")) {
                        split = StringUtils.split(where, "between");
                    } else {
                        split = StringUtils.split(where, "BETWEEN");
                    }

                    String[] ands;
                    if (split[1].contains("and")) {
                        ands = StringUtils.split(split[1], "and");
                    } else {
                        ands = StringUtils.split(split[1], "AND");
                    }
                    Where range = Where.createRange(StringUtils.trim(split[0]), StringUtils.trim(ands[0]), StringUtils.replaceEach(StringUtils.trim(ands[1]), new String[]{"'", "\""}, new String[]{"", ""}));
                    whereList.add(range);
                } else if (LIKE_PATTERN.matcher(where.toLowerCase()).find()) {
                    String[] split = new String[2];
                    split[0] = where.substring(0, where.toLowerCase().indexOf(" like "));
                    split[1] = where.substring(where.toLowerCase().indexOf(" like ") + 6, where.length());
                    String field = StringUtils.trim(split[0]);
                    String paramValue = StringUtils.replaceEach(StringUtils.trim(split[1]), new String[]{"`", "'", "\""}, new String[]{"", "", ""});
                    int firstIndex = paramValue.indexOf("%");
                    int lastIndex = paramValue.lastIndexOf("%");
                    Rel rel = null;
                    if (lastIndex == paramValue.length() - 1 && firstIndex == 0) {
                        /* field like %value%*/
                        rel = Rel.MIDDLE_LIKE;
                    } else if (firstIndex == 0 && firstIndex == lastIndex) {
                        /* field like %value */
                        rel = Rel.FRONT_LIKE;
                    } else if (firstIndex != paramValue.length() - 1 && firstIndex == lastIndex) {
                        /* field like value%value */
                        rel = Rel.MIDDLE_LIKE;
                    } else {
                        /* field like value% */
                        rel = Rel.TAIL_LIKE;
                    }
                    paramValue = Rel.MIDDLE_LIKE == rel ? paramValue : StringUtils.replaceEach(paramValue, new String[]{"%"}, new String[]{""});
                    Where whereParam = Where.create(field, paramValue, rel);
                    whereList.add(whereParam);
                } else {
                    String[] split = StringUtils.split(where, "=");
                    Where whereParam = Where.create(StringUtils.trim(split[0]), StringUtils.replaceEach(StringUtils.trim(split[1]), new String[]{"'", "\""}, new String[]{"", ""}), Rel.EQ);
                    whereList.add(whereParam);
                }
            }
        }
    }

    /**
     * 处理 where SQL
     *
     * @param sql
     */
    private static String disposeWhereSql(String sql) {
        String whereStr = null;
        if (sql.toLowerCase().contains("where")) {
            int whereIndex = StringUtils.indexOf(sql.toLowerCase(), "where") + 5;
            int endIndex = getEndIndex(sql.toLowerCase(), whereIndex);
            whereStr = StringUtils.substring(sql, whereIndex, endIndex);
        }
        return whereStr;
    }

    /**
     * 转换SQL查询结果字段部分
     *
     * @param paramCondition
     */
    private static void transitionSqlForSelect(QueryParamCondition paramCondition, String sql) {
        int selectIndex = StringUtils.indexOf(sql.toLowerCase(), "select") + 6;
        int fromIndex = getEndIndex(sql.toLowerCase(), selectIndex);
        String selectStr = StringUtils.substring(sql, selectIndex, fromIndex);
        selectStr = StringUtils.trim(selectStr);
        if (!"*".equals(selectStr)) {
            String[] fields = StringUtils.split(selectStr, ",");
            List<String> selectList = new ArrayList<>();
            for (String field : fields) {
                field = StringUtils.trim(field);
                if (StringUtils.containsIgnoreCase(field, "as")) {
                    int asIndex = field.toLowerCase().indexOf("as");
                    selectList.add(String.format("as(%s,%s)", StringUtils.trim(field.substring(0, asIndex)), StringUtils.trim(field.substring(asIndex + 2))));
                } else if (StringUtils.contains(field, " ")) {
                    int blankIndex = field.toLowerCase().indexOf(" ");
                    selectList.add(String.format("as(%s,%s)", StringUtils.trim(field.substring(0, blankIndex)), StringUtils.trim(field.substring(blankIndex + 1))));
                } else {
                    selectList.add(field);
                }
            }
            paramCondition.setSelect(selectList);
        }
    }

    protected String checkAlias(String select, String marks) {
        if (StringUtils.isBlank(select)) {
            return null;
        }
        boolean flag = StringUtils.startsWithIgnoreCase(select, "as(");
        if (flag) {
            select = select.substring(3, select.lastIndexOf(")"));
            String[] split = select.split(",");
            String field;
            String alias;
            if (split.length == 1) {
                field = alias = StringUtils.trim(split[0]);
            } else {
                field = StringUtils.trim(split[0]);
                alias = StringUtils.trim(split[1]);
            }
            StringBuffer stringBuffer = new StringBuffer();
            stringBuffer.append(marks)
                    .append(field)
                    .append(marks)
                    .append(" as ")
                    .append(marks)
                    .append(alias)
                    .append(marks);
            return stringBuffer.toString();
        } else {
            return null;
        }
    }

    public Q getListDatabaseParam(QueryDatabasesCondition queryDatabasesCondition, D databaseConf) throws Exception {
        return transitionListDatabaseParam(queryDatabasesCondition, databaseConf);
    }

    public Q getMergeDataParam(MergeDataParamCondition paramCondition, D dataConf, T tableConf) throws Exception {
        return transitionMergeDataParam(paramCondition, dataConf, tableConf);
    }

    public Q getCreateDatabaseParam(CreateDatabaseParamCondition paramCondition, D dataConf) {
        return transitionCreateDatabaseParam(paramCondition, dataConf);
    }

    public Q getUpdateDatabaseParam(UpdateDatabaseParamCondition paramCondition, D dataConf) {
        return transitionUpdateDatabaseParam(paramCondition, dataConf);
    }

    public Q getDropDatabaseParam(DropDatabaseParamCondition paramCondition, D dataConf) {
        return transitionDropDatabaseParam(paramCondition, dataConf);
    }

    public Q getPublishMessage(PublishMessage paramCondition, D dataConf) {
        return transitionPublishMessage(paramCondition, dataConf);
    }

    public Q getSubscribeMessage(SubscribeMessage paramCondition, D dataConf) {
        return transitionSubscribeMessage(paramCondition, dataConf);
    }

}
