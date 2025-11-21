package com.meiya.whalex.db.resolver;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.map.MapUtil;
import com.meiya.whalex.db.entity.PageResult;
import com.meiya.whalex.db.entity.table.infomaration.TableInformation;
import com.meiya.whalex.exception.BusinessException;
import com.meiya.whalex.util.JsonUtil;

import java.util.List;
import java.util.Map;

/**
 * 解析表相关信息返回结果体
 *
 * @author 黄河森
 * @date 2021/9/13
 * @package com.meiya.whalex.db.resolver
 * @project whalex-data-driver
 */
public class TableInformationResolver {
    private PageResult pageResult;

    private TableInformationResolver(PageResult pageResult) {
        this.pageResult = pageResult;
    }

    /**
     * 解析器
     *
     * @param pageResult
     * @return
     */
    public static TableInformationResolver resolver(PageResult pageResult) {
        return new TableInformationResolver(pageResult);
    }

    /**
     * 解析方法
     *
     * @return
     */
    public TableInformation analysis() {
        return analysis(TableInformation.class);
    }

    /**
     * 解析方法
     *
     * @return
     */
    public <T extends TableInformation> T analysis(Class<T> tClass) {
        if (!this.pageResult.getSuccess()) {
            throw new BusinessException(this.pageResult.getCode(), this.pageResult.getMessage());
        }
        List<Map<String, Object>> rows = this.pageResult.getRows();
        if (CollectionUtil.isEmpty(rows)) {
            return null;
        }
        Map<String, Object> tableInformationMap = rows.get(0);
        if (MapUtil.isEmpty(tableInformationMap)) {
            return null;
        }
        T tableInformation = JsonUtil.jsonStrToObject(JsonUtil.objectToStr(tableInformationMap), tClass);
        return tableInformation;
    }
}
