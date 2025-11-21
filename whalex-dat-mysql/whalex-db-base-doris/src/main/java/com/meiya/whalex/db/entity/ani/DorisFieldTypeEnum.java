package com.meiya.whalex.db.entity.ani;

import com.meiya.whalex.db.entity.FieldTypeAdapter;
import com.meiya.whalex.interior.db.constant.ItemFieldTypeEnum;

/**
 * doris 字段枚举
 *
 * @author 黄河森
 * @date 2019/12/24
 * @project whale-cloud-platformX
 */
public enum DorisFieldTypeEnum implements FieldTypeAdapter {

    //------- 整数类型 -------//

    TINYINT("tinyint", ItemFieldTypeEnum.TINYINT, null, null, ItemFieldTypeEnum.ParamStatus.SHOULD, ItemFieldTypeEnum.ParamStatus.NO, true),

    SMALLINT("smallint", ItemFieldTypeEnum.SMALLINT, null, null,ItemFieldTypeEnum.ParamStatus.SHOULD, ItemFieldTypeEnum.ParamStatus.NO, true),

    MEDIUMINT("int", ItemFieldTypeEnum.MEDIUMINT, null, null, ItemFieldTypeEnum.ParamStatus.SHOULD, ItemFieldTypeEnum.ParamStatus.NO, true),

    INTEGER("int", ItemFieldTypeEnum.INTEGER, null, null, ItemFieldTypeEnum.ParamStatus.SHOULD, ItemFieldTypeEnum.ParamStatus.NO, true),

    LONG("bigint", ItemFieldTypeEnum.LONG, null, null, ItemFieldTypeEnum.ParamStatus.SHOULD, ItemFieldTypeEnum.ParamStatus.NO, true),

    //------- 浮点数类型 -------//

    REAL("real", ItemFieldTypeEnum.REAL, null, null, ItemFieldTypeEnum.ParamStatus.SHOULD, ItemFieldTypeEnum.ParamStatus.SHOULD, true),

    FLOAT("float", ItemFieldTypeEnum.FLOAT, null, null, ItemFieldTypeEnum.ParamStatus.SHOULD, ItemFieldTypeEnum.ParamStatus.SHOULD, true),

    DOUBLE("double", ItemFieldTypeEnum.DOUBLE, null, null, ItemFieldTypeEnum.ParamStatus.SHOULD, ItemFieldTypeEnum.ParamStatus.SHOULD, true),

    //------- 定点数类型 -------//

    DECIMAL("decimal", ItemFieldTypeEnum.DECIMAL, null, null, ItemFieldTypeEnum.ParamStatus.SHOULD, ItemFieldTypeEnum.ParamStatus.SHOULD, false),

    NUMERIC("decimal", ItemFieldTypeEnum.NUMERIC, null, null, ItemFieldTypeEnum.ParamStatus.SHOULD, ItemFieldTypeEnum.ParamStatus.SHOULD, false),

    //------- 货币类型 -------//
    MONEY("decimal", ItemFieldTypeEnum.MONEY, 17, 2, ItemFieldTypeEnum.ParamStatus.MUST, ItemFieldTypeEnum.ParamStatus.MUST, false),

    //------- 位类型 -------//

    BIT("boolean", ItemFieldTypeEnum.BIT, null, null, ItemFieldTypeEnum.ParamStatus.SHOULD, ItemFieldTypeEnum.ParamStatus.NO, false),

    //------- 日期类型 -------//

    YEAR("date", ItemFieldTypeEnum.YEAR, null, null, ItemFieldTypeEnum.ParamStatus.SHOULD, ItemFieldTypeEnum.ParamStatus.NO, false),

    DATE("date", ItemFieldTypeEnum.DATE, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    DATETIME("datetime", ItemFieldTypeEnum.DATETIME, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    DATETIME_2("datetime", ItemFieldTypeEnum.TIME, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    TIME("varchar", ItemFieldTypeEnum.SMART_TIME, 20, null, ItemFieldTypeEnum.ParamStatus.MUST, ItemFieldTypeEnum.ParamStatus.NO, false),

    TIMESTAMP("datetime", ItemFieldTypeEnum.TIMESTAMP, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    //------- 文本类型 -------//

    CHAR("char", ItemFieldTypeEnum.CHAR, null, null, ItemFieldTypeEnum.ParamStatus.SHOULD, ItemFieldTypeEnum.ParamStatus.NO, false),

    STRING("varchar", ItemFieldTypeEnum.STRING, 1000, null, ItemFieldTypeEnum.ParamStatus.MUST, ItemFieldTypeEnum.ParamStatus.NO, false),

    TINYTEXT("text", ItemFieldTypeEnum.TINYTEXT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    TEXT("text", ItemFieldTypeEnum.TEXT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    MEDIUMTEXT("text", ItemFieldTypeEnum.MEDIUMTEXT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    LONGTEXT("text", ItemFieldTypeEnum.LONGTEXT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    //------- 枚举类型 -------//

    ENUM("string", ItemFieldTypeEnum.ENUM, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    //------- 多选值类型 -------//

    SET("array<string>", ItemFieldTypeEnum.SET, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    //------- 二进制字符串类型 -------//

    BINARY("char", ItemFieldTypeEnum.BINARY, null, null, ItemFieldTypeEnum.ParamStatus.SHOULD, ItemFieldTypeEnum.ParamStatus.NO, false),

    VARBINARY("varchar", ItemFieldTypeEnum.VARBINARY, 1000, null, ItemFieldTypeEnum.ParamStatus.MUST, ItemFieldTypeEnum.ParamStatus.NO, false),

    //------- 二进制大对象 -------//

    BLOB("blob", ItemFieldTypeEnum.BLOB, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    BYTES("blob", ItemFieldTypeEnum.BYTES, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    TINYBLOB("blob", ItemFieldTypeEnum.TINYBLOB, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    MEDIUMBLOB("blob", ItemFieldTypeEnum.MEDIUMBLOB, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    LONGBLOB("blob", ItemFieldTypeEnum.LONGBLOB, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    IMAGE("blob", ItemFieldTypeEnum.IMAGE, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    //------- JSON 类型 -------//

    // 1.2.x 版本之后具备 jsonb 类型， 2.0.0 之后具备 json 类型
    JSON("string", ItemFieldTypeEnum.OBJECT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    //------- 地理位置 -------//

    POINT("string", ItemFieldTypeEnum.POINT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    //------- 布尔类型 -------//

    BOOLEAN("boolean", ItemFieldTypeEnum.BOOLEAN, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    //------- 集合类型 -------//

    ARRAY("array<string>", ItemFieldTypeEnum.ARRAY, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_TINYINT("array<tinyint>", ItemFieldTypeEnum.ARRAY_TINYINT, null, null, ItemFieldTypeEnum.ParamStatus.SHOULD, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_SMALLINT("array<smallint>", ItemFieldTypeEnum.ARRAY_SMALLINT, null, null, ItemFieldTypeEnum.ParamStatus.SHOULD, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_MEDIUMINT("array<int>", ItemFieldTypeEnum.ARRAY_MEDIUMINT, null, null, ItemFieldTypeEnum.ParamStatus.SHOULD, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_INTEGER("array<int>", ItemFieldTypeEnum.ARRAY_INTEGER, null, null, ItemFieldTypeEnum.ParamStatus.SHOULD, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_LONG("array<bigint>", ItemFieldTypeEnum.ARRAY_LONG, null, null, ItemFieldTypeEnum.ParamStatus.SHOULD, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_FLOAT("array<float>", ItemFieldTypeEnum.ARRAY_FLOAT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_DOUBLE("array<double>", ItemFieldTypeEnum.ARRAY_DOUBLE, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_DECIMAL("array<decimal>", ItemFieldTypeEnum.ARRAY_DECIMAL, null, null, ItemFieldTypeEnum.ParamStatus.SHOULD, ItemFieldTypeEnum.ParamStatus.SHOULD, false),
    ARRAY_NUMERIC("array<decimal>", ItemFieldTypeEnum.ARRAY_NUMERIC, null, null, ItemFieldTypeEnum.ParamStatus.SHOULD, ItemFieldTypeEnum.ParamStatus.SHOULD, false),

    ARRAY_BIT("array<boolean>", ItemFieldTypeEnum.ARRAY_BIT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_CHAR("array<char>", ItemFieldTypeEnum.ARRAY_CHAR, null, null, ItemFieldTypeEnum.ParamStatus.SHOULD, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_STRING("array<string>", ItemFieldTypeEnum.ARRAY_STRING, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_TINYTEXT("array<text>", ItemFieldTypeEnum.ARRAY_TINYTEXT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_TEXT("array<text>", ItemFieldTypeEnum.ARRAY_TEXT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_MEDIUMTEXT("array<text>", ItemFieldTypeEnum.ARRAY_MEDIUMTEXT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_LONGTEXT("array<text>", ItemFieldTypeEnum.ARRAY_LONGTEXT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_POINT("array<string>", ItemFieldTypeEnum.ARRAY_POINT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    NONE("string", ItemFieldTypeEnum.NONE, null, null, ItemFieldTypeEnum.ParamStatus.SHOULD, ItemFieldTypeEnum.ParamStatus.NO, false);


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

    DorisFieldTypeEnum(String dbFieldType, ItemFieldTypeEnum fieldType, Integer filedLength, Integer fieldDecimalPoint, ItemFieldTypeEnum.ParamStatus dataLength, ItemFieldTypeEnum.ParamStatus dataDecimalPoint, boolean unsigned) {
        this.dbFieldType = dbFieldType;
        this.fieldType = fieldType;
        this.filedLength = filedLength;
        this.fieldDecimalPoint = fieldDecimalPoint;
        this.dataLength = dataLength;
        this.dataDecimalPoint = dataDecimalPoint;
        this.unsigned = unsigned;
    }

    public static DorisFieldTypeEnum findFieldTypeEnum(String fieldTypeStr) {
        DorisFieldTypeEnum result = null;
        ItemFieldTypeEnum fieldType = ItemFieldTypeEnum.findFieldTypeEnum(fieldTypeStr);
        for (DorisFieldTypeEnum fieldTypeEnum : DorisFieldTypeEnum.values()) {
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
        for (DorisFieldTypeEnum fieldTypeEnum : DorisFieldTypeEnum.values()) {
            if (dbFieldType.equalsIgnoreCase(fieldTypeEnum.dbFieldType)) {
                if (fieldTypeEnum.equals(DorisFieldTypeEnum.BIT)) {
                    if (fieldLength != null && fieldLength == 1) {
                        result = DorisFieldTypeEnum.BOOLEAN.fieldType;
                    } else {
                        result = fieldTypeEnum.fieldType;
                    }
                } else if (fieldTypeEnum.equals(DorisFieldTypeEnum.BOOLEAN)) {
                    if (fieldLength != null && fieldLength == 1) {
                        result = DorisFieldTypeEnum.BOOLEAN.fieldType;
                    } else {
                        result = DorisFieldTypeEnum.TINYINT.fieldType;
                    }
                } else {
                    result = fieldTypeEnum.fieldType;
                }
                break;
            }
        }
        if (result == null) {
            result = NONE.fieldType;
        }
        return result;
    }

}
