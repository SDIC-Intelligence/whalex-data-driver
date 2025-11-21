package com.meiya.whalex.interior.db.constant;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import org.apache.commons.lang3.StringUtils;

/**
 * 数据项字段类型枚举
 *
 * @author Huanghesen
 * @date 2019/3/5
 */
public enum ItemFieldTypeEnum {

    //------- 整数类型 -------//

    /**
     * 0-255
     */
    TINYINT("TinyInt", ParamStatus.SHOULD, ParamStatus.NO, true),
    /**
     * 0-65535
     */
    SMALLINT("SmallInt", ParamStatus.SHOULD, ParamStatus.NO, true),

    /**
     * 0-16777215
     */
    MEDIUMINT("MediumInt", ParamStatus.SHOULD, ParamStatus.NO, true),

    /**
     * 0-4294967295
     */
    INTEGER("Integer", ParamStatus.SHOULD, ParamStatus.NO, true),

    /**
     * 0-18446744073709551615
     */
    LONG("Long", ParamStatus.SHOULD, ParamStatus.NO, true),

    //------- 浮点数类型 -------//

    REAL("Real", ParamStatus.SHOULD, ParamStatus.SHOULD, true),

    FLOAT("Float", ParamStatus.SHOULD, ParamStatus.SHOULD, true),

    DOUBLE("Double", ParamStatus.SHOULD, ParamStatus.SHOULD, true),

    //------- 定点数类型 -------//

    DECIMAL("Decimal", ParamStatus.MUST, ParamStatus.MUST, false),

    NUMERIC("Numeric", ParamStatus.MUST, ParamStatus.MUST, false),

    //------- 货币类型 -------//
    MONEY("Money", ParamStatus.NO, ParamStatus.NO, false),

    //------- 位类型 -------//

    BIT("Bit", ParamStatus.SHOULD, ParamStatus.NO, false),

    //------- 日期类型 -------//

    /**
     * YYYY
     */
    YEAR("Year", ParamStatus.SHOULD, ParamStatus.NO, false),
    /**
     * YYYY-MM-DD
     */
    DATE("Date", ParamStatus.NO, ParamStatus.NO, false),
    /**
     * YYYY-MM-DD HH:MM:SS
     */
    DATETIME("DateTime", ParamStatus.NO, ParamStatus.NO, false),
    /**
     * 历史原因，旧版本当前对象表示 DATETIME ，未避免旧用户变更，保持不变，但是后续新应用不采用
     */
    @Deprecated
    TIME("Time", ParamStatus.NO, ParamStatus.NO, false),
    /**
     * 表示 time HH:MM:SS
     */
    SMART_TIME("SmartTime", ParamStatus.NO, ParamStatus.NO, false),
    /**
     * 表示带时区的日期（UTC） YYYY-MM-DD HH:MM:SS UTC
     */
    TIMESTAMP("Timestamp", ParamStatus.NO, ParamStatus.NO, false),

    //------- 文本类型 -------//

    /**
     * 定长字符串
     */
    CHAR("Char", ParamStatus.SHOULD, ParamStatus.NO, false),

    /**
     * 可变字符串
     */
    STRING("String", ParamStatus.MUST, ParamStatus.NO, false),

    /**
     * 小文本 0 ~ 255
     */
    TINYTEXT("TinyText", ParamStatus.NO, ParamStatus.NO, false),

    /**
     * 文本，可变长度 0 ~ 65535
     */
    TEXT("Text", ParamStatus.NO, ParamStatus.NO, false),

    /**
     * 中等文本，可变长度 0 ~ 16777215
     */
    MEDIUMTEXT("MediumText", ParamStatus.NO, ParamStatus.NO, false),

    /**
     * 大文本，可变长度 0 ~ 4294967295（4G）
     */
    LONGTEXT("LongText", ParamStatus.NO, ParamStatus.NO, false),

    //------- 枚举类型 -------//

    ENUM("Enum", ParamStatus.NO, ParamStatus.NO, false),

    //------- 多选值类型 -------//
    SET("Set", ParamStatus.NO, ParamStatus.NO, false),

    //------- 二进制字符串类型 -------//
    /**
     * 0 ~ 255
     */
    BINARY("Binary", ParamStatus.SHOULD, ParamStatus.NO, false),

    /**
     * 0 ~ 65535
     */
    VARBINARY("Varbinary", ParamStatus.MUST, ParamStatus.NO, false),

    IMAGE("image", ParamStatus.NO, ParamStatus.NO, false),

    //------- 二进制大对象 -------//

    BYTES("Bytes", ParamStatus.NO, ParamStatus.NO, false),

    /**
     * 等同于 BYTES，0 ~ 65535
     */
    BLOB("Blob", ParamStatus.NO, ParamStatus.NO, false),

    /**
     * 0 ~ 255 (64kb)
     */
    TINYBLOB("TinyBlob", ParamStatus.NO, ParamStatus.NO, false),

    /**
     * 0 ~16777215 (16MB)
     */
    MEDIUMBLOB("MediumBlob", ParamStatus.NO, ParamStatus.NO, false),

    /**
     * 0 ~ 4294967295 (4GB)
     */
    LONGBLOB("LongBlob", ParamStatus.NO, ParamStatus.NO, false),

    //------- JSON 类型 -------//

    OBJECT("Object", ParamStatus.NO, ParamStatus.NO, false),

    //------- 地理位置 -------//

    POINT("Point", ParamStatus.NO, ParamStatus.NO, false),

    //------- 布尔类型 -------//

    BOOLEAN("Boolean", ParamStatus.NO, ParamStatus.NO, false),

    //------- 集合类型 -------//

    /**
     * 默认等同于 字符串数组
     */
    ARRAY("Array", ParamStatus.NO, ParamStatus.NO, false),

    ARRAY_TINYINT("Array<TinyInt>", ParamStatus.NO, ParamStatus.NO, false),

    ARRAY_SMALLINT("Array<SmallInt>", ParamStatus.NO, ParamStatus.NO, false),

    ARRAY_MEDIUMINT("Array<MediumInt>", ParamStatus.NO, ParamStatus.NO, false),

    ARRAY_INTEGER("Array<Integer>", ParamStatus.NO, ParamStatus.NO, false),

    ARRAY_LONG("Array<Long>", ParamStatus.NO, ParamStatus.NO, false),

    ARRAY_FLOAT("Array<Float>", ParamStatus.NO, ParamStatus.NO, false),

    ARRAY_DOUBLE("Array<Double>", ParamStatus.NO, ParamStatus.NO, false),

    ARRAY_NUMERIC("Array<Numeric>", ParamStatus.NO, ParamStatus.NO, false),

    ARRAY_DECIMAL("Array<Decimal>", ParamStatus.NO, ParamStatus.NO, false),

    ARRAY_BIT("Array<Bit>", ParamStatus.NO, ParamStatus.NO, false),

    ARRAY_CHAR("Array<Char>", ParamStatus.NO, ParamStatus.NO, false),

    ARRAY_STRING("Array<String>", ParamStatus.NO, ParamStatus.NO, false),

    ARRAY_TINYTEXT("Array<TinyText>", ParamStatus.NO, ParamStatus.NO, false),

    ARRAY_TEXT("Array<Text>", ParamStatus.NO, ParamStatus.NO, false),

    ARRAY_MEDIUMTEXT("Array<MediumText>", ParamStatus.NO, ParamStatus.NO, false),

    ARRAY_LONGTEXT("Array<LongText>", ParamStatus.NO, ParamStatus.NO, false),

    ARRAY_POINT("Array<Point>", ParamStatus.NO, ParamStatus.NO, false),

    //------- 其他类型 -------//

    /**
     * ES 特有类型
     */
    COMPLETION("Completion", ParamStatus.NO, ParamStatus.NO, false),

    /**
     * MONGODB 特有类型
     */
    OBJECT_ID(ParamTypeConstant.OBJECTID, ParamStatus.NO, ParamStatus.NO, false),

    NONE("None", ParamStatus.SHOULD, ParamStatus.NO, false)
    ;

    private String val;

    private ParamStatus dataLength;

    private ParamStatus dataDecimalPoint;

    private boolean unsigned;

    ItemFieldTypeEnum(String val, ParamStatus dataLength, ParamStatus dataDecimalPoint, boolean unsigned) {
        this.val = val;
        this.dataLength = dataLength;
        this.dataDecimalPoint = dataDecimalPoint;
        this.unsigned = unsigned;
    }

    @JsonValue
    public String getVal() {
        return val;
    }

    public ParamStatus getDataLength() {
        return dataLength;
    }

    public ParamStatus getDataDecimalPoint() {
        return dataDecimalPoint;
    }

    public boolean isUnsigned() {
        return unsigned;
    }

    @JsonCreator
    public static ItemFieldTypeEnum findFieldTypeEnum(String value) {
        if (StringUtils.isBlank(value)) {
            return ItemFieldTypeEnum.STRING;
        }
        ItemFieldTypeEnum result = null;
        for (ItemFieldTypeEnum fieldTypeEnum : ItemFieldTypeEnum.values()) {
            if (value.equalsIgnoreCase(fieldTypeEnum.val)) {
                result =  fieldTypeEnum;
                break;
            }
        }
        if (result == null) {
            result = ItemFieldTypeEnum.STRING;
        }
        return result;
    }

    /**
     * 参数状态
     */
    public enum ParamStatus {
        MUST,
        SHOULD,
        NO
    }
}
