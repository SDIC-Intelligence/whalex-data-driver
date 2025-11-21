package com.meiya.whalex.db.entity.dwh;

import com.meiya.whalex.db.entity.FieldTypeAdapter;
import com.meiya.whalex.interior.db.constant.ItemFieldTypeEnum;

/**
 * Hive 字段枚举
 *
 * @author 黄河森
 * @date 2019/12/24
 * @project whale-cloud-platformX
 */
public enum HiveFieldTypeEnum implements FieldTypeAdapter {

    //------- 整数类型 -------//

    TINYINT("tinyint", ItemFieldTypeEnum.TINYINT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    SMALLINT("smallint", ItemFieldTypeEnum.SMALLINT, null, null,ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    INTEGER("int", ItemFieldTypeEnum.INTEGER, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    MEDIUMINT("int", ItemFieldTypeEnum.MEDIUMINT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    LONG("bigint", ItemFieldTypeEnum.LONG, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    //------- 浮点数类型 -------//


    FLOAT("float", ItemFieldTypeEnum.FLOAT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    REAL("float", ItemFieldTypeEnum.REAL, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    DOUBLE("double", ItemFieldTypeEnum.DOUBLE, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    //------- 定点数类型 -------//

    DECIMAL("decimal", ItemFieldTypeEnum.DECIMAL, null, null, ItemFieldTypeEnum.ParamStatus.SHOULD, ItemFieldTypeEnum.ParamStatus.SHOULD, false),

    NUMERIC("numeric", ItemFieldTypeEnum.NUMERIC, null, null, ItemFieldTypeEnum.ParamStatus.SHOULD, ItemFieldTypeEnum.ParamStatus.SHOULD, false),

    //------- 货币类型 -------//
    MONEY("decimal", ItemFieldTypeEnum.MONEY, 17, 2, ItemFieldTypeEnum.ParamStatus.MUST, ItemFieldTypeEnum.ParamStatus.MUST, false),

    //------- 二进制字符串类型 -------//

    BINARY("binary", ItemFieldTypeEnum.BINARY, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    VARBINARY("binary", ItemFieldTypeEnum.VARBINARY, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    //------- 位类型 -------//

    BIT("binary", ItemFieldTypeEnum.BIT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    //------- 日期类型 -------//

    YEAR("date", ItemFieldTypeEnum.YEAR, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    DATE("timestamp", ItemFieldTypeEnum.DATE, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    DATETIME("timestamp", ItemFieldTypeEnum.DATETIME, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    DATETIME_2("timestamp", ItemFieldTypeEnum.TIME, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    TIME("timestamp", ItemFieldTypeEnum.SMART_TIME, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    TIMESTAMP("timestamp", ItemFieldTypeEnum.TIMESTAMP, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    //------- 文本类型 -------//
    STRING("string", ItemFieldTypeEnum.STRING, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    CHAR("string", ItemFieldTypeEnum.CHAR, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    TINYTEXT("string", ItemFieldTypeEnum.TINYTEXT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    TEXT("string", ItemFieldTypeEnum.TEXT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    MEDIUMTEXT("string", ItemFieldTypeEnum.MEDIUMTEXT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    LONGTEXT("string", ItemFieldTypeEnum.LONGTEXT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    //------- 枚举类型 -------//

    ENUM("string", ItemFieldTypeEnum.ENUM, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    //------- 多选值类型 -------//

    SET("string", ItemFieldTypeEnum.SET, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    //------- 二进制大对象 -------//

    BLOB("string", ItemFieldTypeEnum.BLOB, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    BYTES("string", ItemFieldTypeEnum.BYTES, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    TINYBLOB("string", ItemFieldTypeEnum.TINYBLOB, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    MEDIUMBLOB("string", ItemFieldTypeEnum.MEDIUMBLOB, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    LONGBLOB("string", ItemFieldTypeEnum.LONGBLOB, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    IMAGE("string", ItemFieldTypeEnum.IMAGE, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    //------- JSON 类型 -------//

    JSON("string", ItemFieldTypeEnum.OBJECT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    //------- 地理位置 -------//

    POINT("string", ItemFieldTypeEnum.POINT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    //------- 布尔类型 -------//

    BOOLEAN("boolean", ItemFieldTypeEnum.BOOLEAN, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    //------- 集合类型 -------//

    ARRAY("string", ItemFieldTypeEnum.ARRAY, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_TINYINT("array<tinyint>", ItemFieldTypeEnum.ARRAY_TINYINT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_SMALLINT("array<smallint>", ItemFieldTypeEnum.ARRAY_SMALLINT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_INTEGER("array<int>", ItemFieldTypeEnum.ARRAY_INTEGER, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_MEDIUMINT("array<int>", ItemFieldTypeEnum.ARRAY_MEDIUMINT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_LONG("array<bigint>", ItemFieldTypeEnum.ARRAY_LONG, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_FLOAT("array<float>", ItemFieldTypeEnum.ARRAY_FLOAT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_DOUBLE("array<double>", ItemFieldTypeEnum.ARRAY_DOUBLE, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_NUMERIC("array<numeric>", ItemFieldTypeEnum.ARRAY_NUMERIC, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    ARRAY_DECIMAL("array<decimal>", ItemFieldTypeEnum.ARRAY_DECIMAL, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_BIT("array<binary>", ItemFieldTypeEnum.ARRAY_BIT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_CHAR("array<char>", ItemFieldTypeEnum.ARRAY_CHAR, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_STRING("array<string>", ItemFieldTypeEnum.ARRAY_STRING, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_TINYTEXT("array<string>", ItemFieldTypeEnum.ARRAY_TINYTEXT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_TEXT("array<string>", ItemFieldTypeEnum.ARRAY_TEXT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_MEDIUMTEXT("array<string>", ItemFieldTypeEnum.ARRAY_MEDIUMTEXT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_LONGTEXT("array<string>", ItemFieldTypeEnum.ARRAY_LONGTEXT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_POINT("array<string>", ItemFieldTypeEnum.ARRAY_POINT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    NONE("string", ItemFieldTypeEnum.NONE, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false);


    private String dbFieldType;

    private ItemFieldTypeEnum fieldType;

    private Integer filedLength;

    private Integer fieldDecimalPoint;

    private ItemFieldTypeEnum.ParamStatus dataLength;

    private ItemFieldTypeEnum.ParamStatus dataDecimalPoint;

    private boolean unsigned;

    @Override
    public String getDbFieldType() {
        return dbFieldType;
    }

    @Override
    public ItemFieldTypeEnum getFieldType() {
        return fieldType;
    }

    @Override
    public Integer getFiledLength() {
        return filedLength;
    }

    @Override
    public Integer getFieldDecimalPoint() {
        return fieldDecimalPoint;
    }

    @Override
    public ItemFieldTypeEnum.ParamStatus getNeedDataLength() {
        return dataLength;
    }

    @Override
    public ItemFieldTypeEnum.ParamStatus getNeedDataDecimalPoint() {
        return dataDecimalPoint;
    }

    @Override
    public boolean isUnsigned() {
        return unsigned;
    }

    HiveFieldTypeEnum(String dbFieldType, ItemFieldTypeEnum fieldType, Integer filedLength, Integer fieldDecimalPoint, ItemFieldTypeEnum.ParamStatus dataLength, ItemFieldTypeEnum.ParamStatus dataDecimalPoint, boolean unsigned) {
        this.dbFieldType = dbFieldType;
        this.fieldType = fieldType;
        this.filedLength = filedLength;
        this.fieldDecimalPoint = fieldDecimalPoint;
        this.dataLength = dataLength;
        this.dataDecimalPoint = dataDecimalPoint;
        this.unsigned = unsigned;
    }

    public static HiveFieldTypeEnum findFieldTypeEnum(String fieldTypeStr) {
        HiveFieldTypeEnum result = null;
        ItemFieldTypeEnum fieldType = ItemFieldTypeEnum.findFieldTypeEnum(fieldTypeStr);
        for (HiveFieldTypeEnum fieldTypeEnum : HiveFieldTypeEnum.values()) {
            if (fieldType.equals(fieldTypeEnum.fieldType)) {
                result = fieldTypeEnum;
                break;
            }
        }
        if (result == null) {
            result = NONE;
        }
        return result;
    }

    public static ItemFieldTypeEnum dbFieldType2FieldType(String dbFieldType, Integer fieldLength) {
        ItemFieldTypeEnum result = null;
        for (HiveFieldTypeEnum fieldTypeEnum : HiveFieldTypeEnum.values()) {
            if (dbFieldType.equalsIgnoreCase(fieldTypeEnum.dbFieldType)) {
                result = fieldTypeEnum.fieldType;
                break;
            }
        }
        if (result == null) {
            result = NONE.fieldType;
        }
        return result;
    }

}
