package com.meiya.whalex.db.entity;


import com.meiya.whalex.interior.db.constant.ItemFieldTypeEnum;

/**
 * 字段类型适配
 *
 * @author 黄河森
 * @date 2023/6/20
 * @package com.meiya.whalex.db.entity
 * @project whalex-data-driver
 */
public interface FieldTypeAdapter {

    /**
     * 原始数据库类型
     *
     * @return
     */
    String getDbFieldType();

    /**
     * 标准类型
     *
     * @return
     */
    ItemFieldTypeEnum getFieldType();

    /**
     * 字段默认长度
     * @return
     */
    Integer getFiledLength();

    /**
     * 小数位
     * @return
     */
    Integer getFieldDecimalPoint();

    /**
     * 是否需要长度
     *
     * @return
     */
    ItemFieldTypeEnum.ParamStatus getNeedDataLength();

    /**
     * 是否需要经度
     * @return
     */
    ItemFieldTypeEnum.ParamStatus getNeedDataDecimalPoint();

    /**
     * 是否支持无符合设置
     *
     * @return
     */
    boolean isUnsigned();

}
