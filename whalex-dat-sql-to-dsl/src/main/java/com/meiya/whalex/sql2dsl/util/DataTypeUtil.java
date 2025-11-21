package com.meiya.whalex.sql2dsl.util;

import com.meiya.whalex.interior.db.constant.ItemFieldTypeEnum;


/**
 * 数据类型转换
 *
 * @author 蔡荣桂
 * @date 2022/06/27
 * @project whalex-dat-sql
 */
public enum DataTypeUtil {

    //------- 整数类型 -------//
    TINYINT("tinyint", ItemFieldTypeEnum.TINYINT),

    SMALLINT("smallint", ItemFieldTypeEnum.SMALLINT),

    MEDIUMINT("mediumint", ItemFieldTypeEnum.MEDIUMINT),

    INTEGER("int", ItemFieldTypeEnum.INTEGER),

    BIGINT("bigint", ItemFieldTypeEnum.LONG),

    //------- 浮点数类型 -------//

    REAL("real", ItemFieldTypeEnum.REAL),

    FLOAT("float", ItemFieldTypeEnum.FLOAT),

    DOUBLE("double", ItemFieldTypeEnum.DOUBLE),

    //------- 定点数类型 -------//

    DECIMAL("decimal", ItemFieldTypeEnum.DECIMAL),

    NUMERIC("numeric", ItemFieldTypeEnum.NUMERIC),

    //------- 货币类型 -------//
    MONEY("money", ItemFieldTypeEnum.MONEY),

    //------- 位类型 -------//

    BIT("bit", ItemFieldTypeEnum.BIT),

    //------- 日期类型 -------//

    /**
     * YYYY
     */
    YEAR("year", ItemFieldTypeEnum.YEAR),
    /**
     * YYYY-MM-DD
     */
    DATE("date", ItemFieldTypeEnum.DATE),
    /**
     * YYYY-MM-DD HH:MM:SS
     */
    DATETIME("datetime", ItemFieldTypeEnum.DATETIME),
    /**
     * 表示 time HH:MM:SS
     */
    TIME("time", ItemFieldTypeEnum.SMART_TIME),
    /**
     * 表示带时区的日期（UTC） YYYY-MM-DD HH:MM:SS UTC
     */
    TIMESTAMP("timestamp", ItemFieldTypeEnum.TIMESTAMP),

    //------- 文本类型 -------//

    /**
     * 定长字符串
     */
    CHAR("char", ItemFieldTypeEnum.CHAR),

    /**
     * 可变字符串
     */
    STRING("varchar", ItemFieldTypeEnum.STRING),

    /**
     * 小文本 0 ~ 255
     */
    TINYTEXT("tinytext", ItemFieldTypeEnum.TINYTEXT),

    /**
     * 文本，可变长度 0 ~ 65535
     */
    TEXT("text", ItemFieldTypeEnum.TEXT),

    /**
     * 中等文本，可变长度 0 ~ 16777215
     */
    MEDIUMTEXT("mediumtext", ItemFieldTypeEnum.MEDIUMTEXT),

    /**
     * 大文本，可变长度 0 ~ 4294967295（4G）
     */
    LONGTEXT("longtext", ItemFieldTypeEnum.LONGTEXT),

    //------- 二进制字符串类型 -------//
    /**
     * 0 ~ 255
     */
    BINARY("binary", ItemFieldTypeEnum.BINARY),

    /**
     * 0 ~ 65535
     */
    VARBINARY("varbinary", ItemFieldTypeEnum.VARBINARY),

    IMAGE("image", ItemFieldTypeEnum.IMAGE),

    //------- 二进制大对象 -------//

    /**
     * 等同于 BYTES，0 ~ 65535
     */
    BLOB("blob", ItemFieldTypeEnum.BLOB),

    /**
     * 0 ~ 255 (64kb)
     */
    TINYBLOB("tinyblob", ItemFieldTypeEnum.TINYBLOB),

    /**
     * 0 ~16777215 (16MB)
     */
    MEDIUMBLOB("mediumblob", ItemFieldTypeEnum.MEDIUMBLOB),

    /**
     * 0 ~ 4294967295 (4GB)
     */
    LONGBLOB("longblob", ItemFieldTypeEnum.LONGBLOB),

    //------- JSON 类型 -------//

    JSON("json", ItemFieldTypeEnum.OBJECT),

    //------- 地理位置 -------//

    POINT("point", ItemFieldTypeEnum.POINT),

    //------- 布尔类型 -------//

    BOOLEAN("boolean", ItemFieldTypeEnum.BOOLEAN),

    //------- 集合类型 -------//

    ARRAY_TINYINT("tinyint[]", ItemFieldTypeEnum.ARRAY_TINYINT),

    ARRAY_SMALLINT("smallint[]", ItemFieldTypeEnum.ARRAY_SMALLINT),

    ARRAY_MEDIUMINT("mediumint[]", ItemFieldTypeEnum.ARRAY_MEDIUMINT),

    ARRAY_INTEGER("int[]", ItemFieldTypeEnum.ARRAY_INTEGER),

    ARRAY_BIGINT("bigint[]", ItemFieldTypeEnum.ARRAY_LONG),

    ARRAY_FLOAT("float[]", ItemFieldTypeEnum.ARRAY_FLOAT),

    ARRAY_DOUBLE("double[]", ItemFieldTypeEnum.ARRAY_DOUBLE),

    ARRAY_DECIMAL("decimal[]", ItemFieldTypeEnum.ARRAY_DECIMAL),

    ARRAY_BIT("bit[]", ItemFieldTypeEnum.ARRAY_BIT),

    ARRAY_CHAR("char[]", ItemFieldTypeEnum.ARRAY_CHAR),

    ARRAY_STRING("varchar[]", ItemFieldTypeEnum.ARRAY_STRING),

    ARRAY_TINYTEXT("tinytext[]", ItemFieldTypeEnum.ARRAY_TINYTEXT),

    ARRAY_TEXT("text[]", ItemFieldTypeEnum.ARRAY_TEXT),

    ARRAY_MEDIUMTEXT("mediumtext[]", ItemFieldTypeEnum.ARRAY_MEDIUMTEXT),

    ARRAY_LONGTEXT("longtext[]", ItemFieldTypeEnum.ARRAY_LONGTEXT),

    ARRAY_POINT("multipoint", ItemFieldTypeEnum.ARRAY_POINT),

    ;

    private String code;
    private ItemFieldTypeEnum itemFieldTypeEnum;

    DataTypeUtil(String code, ItemFieldTypeEnum itemFieldTypeEnum) {
        this.code = code;
        this.itemFieldTypeEnum = itemFieldTypeEnum;
    }


    public static ItemFieldTypeEnum getItemFieldTypeEnum(String code) {
        DataTypeUtil[] values = DataTypeUtil.values();
        for (DataTypeUtil value : values) {
            if (value.code.equalsIgnoreCase(code)) {
                return value.itemFieldTypeEnum;
            }
        }
        throw new RuntimeException("未知的数据类型:" + code);
    }

    public static boolean checkItemFieldTypeEnum(String code) {
        DataTypeUtil[] values = DataTypeUtil.values();
        for (DataTypeUtil value : values) {
            if (value.code.equalsIgnoreCase(code)) {
                return true;
            }
        }
        return false;
    }

}
