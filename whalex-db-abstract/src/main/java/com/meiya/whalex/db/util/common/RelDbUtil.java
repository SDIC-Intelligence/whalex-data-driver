package com.meiya.whalex.db.util.common;

import com.meiya.whalex.db.entity.ani.NamedParamsBuildResult;
import com.meiya.whalex.interior.db.search.condition.Sort;
import com.meiya.whalex.interior.db.search.in.Order;
import com.meiya.whalex.interior.db.search.in.Page;
import org.apache.commons.lang3.StringUtils;

import java.sql.SQLException;
import java.util.*;

public class RelDbUtil {

    /**
     * 根据包含命名参数的 SQL 模板构建最终的 SQL 以及参数值列表
     * <br/>
     * 示例：
     * <br/>
     * sql = update Person p set p.name=:new_name, p.sex=:new_sex where p.name=:old_name or p.second_name=:old_name
     * <br/>
     * <br/>
     * 最终生成生成的 SQL 为：update Person p set p.name=?, p.sex=? where p.name=? or p.second_name=?
     *
     * @param sql
     * @param params
     * @return
     * @throws SQLException
     */
    public static NamedParamsBuildResult buildSqlAndParams(String sql, Map<String, Object> params) throws SQLException {
        return buildSqlAndParams(sql, params.keySet().toArray(new String[0]), params.values().toArray());
    }

    /**
     * 根据包含命名参数的 SQL 模板构建最终的 SQL 以及参数值列表
     *
     * @param sql
     * @param keys
     * @param values
     * @return
     * @throws SQLException
     */
    public static NamedParamsBuildResult buildSqlAndParams(String sql, String[] keys, Object[] values) throws SQLException {
        //执行有效性校验
        integrityValidate(sql, keys);

        List<Object[]> tmpList = new LinkedList<>();

        for (int i = 0; i < keys.length; i++) {
            String key = keys[i];
            int startIndex = 0;
            int posi;
            //循环查找所有 param
            while ((posi = sql.indexOf(":" + key, startIndex)) != -1) {
                int keyLength = key.length();
                int index = posi + keyLength + 1;

                if (index >= sql.length()) {
                    tmpList.add(new Object[]{posi, new Object[]{key, values[i]}});
                    break;
                }

                int nextChar = sql.charAt(index);
                if (nextChar == ',' || nextChar == ')' || Character.isSpaceChar(nextChar) || Character.isWhitespace(nextChar)) {
                    tmpList.add(new Object[]{posi, new Object[]{key, values[i]}});
                }

                startIndex = posi + keyLength;
            }
        }

        //使用 ? 替换命名参数
        for (String key : keys) {
            int startIndex = 0;
            int pos;
            while ((pos = sql.indexOf(":" + key, startIndex)) != -1) {
                int keyLength = key.length();
                int index = pos + keyLength + 1;

                if (index >= sql.length()) {
                    sql = sql.substring(0, pos) + "?" + sql.substring(index);
                    break;
                }

                int nextChar = sql.charAt(index);
                if (nextChar == ',' || nextChar == ')' || Character.isSpaceChar(nextChar) || Character.isWhitespace(nextChar)) {
                    sql = sql.substring(0, pos) + "?" + sql.substring(index);
                }

                startIndex = pos + 1;
            }
        }

        //根据 positon 重排序
        Collections.sort(tmpList, new Comparator<Object[]>() {
            @Override
            public int compare(Object[] o1, Object[] o2) {
                Integer key1 = (Integer) o1[0];
                Integer key2 = (Integer) o2[0];
                if (key1.intValue() == key2.intValue()) {
                    return 0;
                } else if (key1 > key2) {
                    return 1;
                }
                return -1;
            }
        });

        //生成最终的 values
        List<Object> _values = new LinkedList<>();
        for (Object[] item : tmpList) {
            _values.add(((Object[]) item[1])[1]);
        }

        return new NamedParamsBuildResult(sql, _values.toArray());
    }

    /**
     * 校验sql参数与列表个数
     *
     * @param sql
     * @param keys
     * @throws SQLException
     */
    private static void integrityValidate(String sql, String[] keys) throws SQLException {
        for (String key : keys) {
            if (!sql.contains(key)) {
                throw new SQLException("SQL 中缺少参数定义：" + key);
            }
        }
    }

    public static boolean orderHandler(Order order) {
        if (order == null || StringUtils.isBlank(order.getField())) {
            return false;
        }
        if (order.getSort() == null) {
            order.setSort(Sort.ASC);
        }
        return true;
    }


    public static Page pageHandler(Page page) {
        if (page == null) {
            page = new Page();
            return page;
        }
        if (page.getOffset() == null || page.getOffset() < 0) {
            page.setOffset(0);
        }
        if (page.getLimit() == null || (!page.getLimit().equals(Page.LIMIT_ALL_DATA) && page.getLimit() <= 0)) {
            page.setLimit(10);
        }
        return page;
    }

}
