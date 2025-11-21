package com.meiya.whalex.jdbc.util;

import com.meiya.whalex.interior.db.constant.ItemFieldTypeEnum;


/**
 * 数据类型转换
 *
 * @author 蔡荣桂
 * @date 2022/06/27
 * @project whalex-dat-sql
 */
public enum DataTypeUtil {

    STRING("varchar", ItemFieldTypeEnum.STRING),
    INTEGER("int", ItemFieldTypeEnum.INTEGER),
    FLOAT("float", ItemFieldTypeEnum.FLOAT),
    DOUBLE("double", ItemFieldTypeEnum.DOUBLE),
    DATE("date", ItemFieldTypeEnum.DATE),
    DATETIME("datetime", ItemFieldTypeEnum.DATE),
    TIMESTAMP("timestamp", ItemFieldTypeEnum.TIME),
    TIME("time", ItemFieldTypeEnum.TIME),
    BOOLEAN("boolean", ItemFieldTypeEnum.BOOLEAN),
    BYTES("bytes", ItemFieldTypeEnum.BYTES),
    POINT("point", ItemFieldTypeEnum.POINT),
    ARRAY("array", ItemFieldTypeEnum.ARRAY),
    TEXT("text", ItemFieldTypeEnum.TEXT),
    OBJECT("object", ItemFieldTypeEnum.OBJECT),
    NONE("none", ItemFieldTypeEnum.NONE);

    private String code;
    private ItemFieldTypeEnum itemFieldTypeEnum;

    DataTypeUtil(String code, ItemFieldTypeEnum itemFieldTypeEnum) {
        this.code = code;
        this.itemFieldTypeEnum = itemFieldTypeEnum;
    }


    public static  ItemFieldTypeEnum getItemFieldTypeEnum(String code) {
        DataTypeUtil[] values = DataTypeUtil.values();
        for (DataTypeUtil value : values) {
            if(value.code.equalsIgnoreCase(code)) {
                return value.itemFieldTypeEnum;
            }
        }
        throw new RuntimeException("未知的数据类型:" + code);
    }

}
