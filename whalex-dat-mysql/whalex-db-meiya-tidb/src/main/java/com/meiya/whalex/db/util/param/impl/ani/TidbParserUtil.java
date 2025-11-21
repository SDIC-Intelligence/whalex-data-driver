package com.meiya.whalex.db.util.param.impl.ani;

import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.DateUtil;
import com.meiya.whalex.exception.BusinessException;
import com.meiya.whalex.interior.db.operation.in.Point;
import com.meiya.whalex.interior.db.search.in.Where;
import com.meiya.whalex.util.JsonUtil;
import lombok.extern.slf4j.Slf4j;

import java.sql.Timestamp;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * 关系型数据库通用，解析查询实体转换为SQL
 *
 * @author 黄河森
 * @date 2019/12/23
 * @project whale-cloud-platformX
 */
@Slf4j
public class TidbParserUtil extends BaseMySqlParserUtil {
    @Override
    protected Object translateValue(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Date) {
            return DateUtil.format((Date) value, DatePattern.NORM_DATETIME_PATTERN);
        } else if (value instanceof Timestamp) {
            return DateUtil.format((Timestamp) value, DatePattern.NORM_DATETIME_PATTERN);
        } else if (value instanceof Point) {
            Point point = (Point) value;
            return point.getLon() + "," + point.getLat();
        } else if (value instanceof Boolean || value instanceof Integer || value instanceof Double || value instanceof Long || value instanceof Float) {
            return value;
        } else if (value instanceof List || (value.getClass().isArray() && !(value instanceof byte[])) || value instanceof Map) {
            return JsonUtil.objectToStr(value);
        }else {
            return value;
        }
    }

    @Override
    protected void geoDistanceQuery(Where where, List<Map<String, Object>> geoDistanceList, StringBuilder sb) {
        throw new BusinessException("当前Tidb 操作方法不允许使用范围查询语法");
    }
}
