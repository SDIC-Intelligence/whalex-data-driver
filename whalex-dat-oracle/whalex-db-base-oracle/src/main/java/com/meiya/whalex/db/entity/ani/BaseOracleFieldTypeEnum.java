package com.meiya.whalex.db.entity.ani;

import com.meiya.whalex.db.entity.FieldTypeAdapter;
import com.meiya.whalex.interior.db.constant.ItemFieldTypeEnum;
import org.apache.commons.lang3.StringUtils;

/**
 * Oracle 字段枚举
 *
 * @author 蔡荣桂
 * @date 2021/4/19
 * @project whale-cloud-platformX
 */
public enum BaseOracleFieldTypeEnum implements FieldTypeAdapter {
    //------- 整数类型 -------//
    TINYINT("NUMBER(3)", ItemFieldTypeEnum.TINYINT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, true),

    SMALLINT("NUMBER(5)", ItemFieldTypeEnum.SMALLINT, null, null,ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, true),

    MEDIUMINT("NUMBER(7)", ItemFieldTypeEnum.MEDIUMINT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, true),

    INTEGER("NUMBER(10)", ItemFieldTypeEnum.INTEGER, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, true),

    LONG("NUMBER(19)", ItemFieldTypeEnum.LONG, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, true),

    //------- 浮点数类型 -------//
    FLOAT("FLOAT", ItemFieldTypeEnum.FLOAT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, true),
    REAL("FLOAT", ItemFieldTypeEnum.REAL, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, true),
    DOUBLE("BINARY_DOUBLE", ItemFieldTypeEnum.DOUBLE, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, true),

    //------- 定点数类型 -------//
    DECIMAL("NUMBER", ItemFieldTypeEnum.DECIMAL, null, null, ItemFieldTypeEnum.ParamStatus.SHOULD, ItemFieldTypeEnum.ParamStatus.SHOULD, false),

    NUMERIC("NUMBER", ItemFieldTypeEnum.NUMERIC, null, null, ItemFieldTypeEnum.ParamStatus.SHOULD, ItemFieldTypeEnum.ParamStatus.SHOULD, false),

    //------- 货币类型 -------//
    MONEY("NUMBER", ItemFieldTypeEnum.MONEY, 17, 2, ItemFieldTypeEnum.ParamStatus.MUST, ItemFieldTypeEnum.ParamStatus.MUST, false),

    //------- 位类型 -------//
    BIT("RAW", ItemFieldTypeEnum.BIT, 1, null, ItemFieldTypeEnum.ParamStatus.MUST, ItemFieldTypeEnum.ParamStatus.NO, false),

    //------- 日期类型 -------//

    DATE("DATE", ItemFieldTypeEnum.DATE, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    YEAR("DATE", ItemFieldTypeEnum.YEAR, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    TIMESTAMP("TIMESTAMP", ItemFieldTypeEnum.TIMESTAMP, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    DATETIME("TIMESTAMP", ItemFieldTypeEnum.DATETIME, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    DATETIME_2("TIMESTAMP", ItemFieldTypeEnum.TIME, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    TIME("TIMESTAMP", ItemFieldTypeEnum.SMART_TIME, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    //------- 文本类型 -------//
    CHAR("CHAR", ItemFieldTypeEnum.CHAR, null, null, ItemFieldTypeEnum.ParamStatus.SHOULD, ItemFieldTypeEnum.ParamStatus.NO, false),

    STRING("VARCHAR2", ItemFieldTypeEnum.STRING, 1000, null, ItemFieldTypeEnum.ParamStatus.MUST, ItemFieldTypeEnum.ParamStatus.NO, false),

    TEXT("CLOB", ItemFieldTypeEnum.TEXT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    TINYTEXT("CLOB", ItemFieldTypeEnum.TINYTEXT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    MEDIUMTEXT("CLOB", ItemFieldTypeEnum.MEDIUMTEXT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    LONGTEXT("CLOB", ItemFieldTypeEnum.LONGTEXT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    //------- 枚举类型 -------//
    // 通过约束来做
    ENUM("VARCHAR2", ItemFieldTypeEnum.ENUM, 1000, null, ItemFieldTypeEnum.ParamStatus.MUST, ItemFieldTypeEnum.ParamStatus.NO, false),

    //------- 多选值类型 -------//

    SET("VARCHAR2", ItemFieldTypeEnum.SET, 1000, null, ItemFieldTypeEnum.ParamStatus.MUST, ItemFieldTypeEnum.ParamStatus.NO, false),

    //------- 二进制字符串类型 -------//
    BINARY("RAW", ItemFieldTypeEnum.BINARY, 1, null, ItemFieldTypeEnum.ParamStatus.MUST, ItemFieldTypeEnum.ParamStatus.NO, false),

    //------- 二进制大对象 -------//

    BLOB("BLOB", ItemFieldTypeEnum.BLOB, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    BYTES("BLOB", ItemFieldTypeEnum.BYTES, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    TINYBLOB("BLOB", ItemFieldTypeEnum.TINYBLOB, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    MEDIUMBLOB("BLOB", ItemFieldTypeEnum.MEDIUMBLOB, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    LONGBLOB("BLOB", ItemFieldTypeEnum.LONGBLOB, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    IMAGE("ORDImage", ItemFieldTypeEnum.IMAGE, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    //------- 二进制字符串类型 -------//
    VARBINARY("BLOB", ItemFieldTypeEnum.VARBINARY, null, null, ItemFieldTypeEnum.ParamStatus.SHOULD, ItemFieldTypeEnum.ParamStatus.NO, false),

    //------- JSON 类型 -------//

    /**
     * 11g 不支持 json
     * 12c 支持 json
     */
    JSON("CLOB", ItemFieldTypeEnum.OBJECT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    //------- 地理位置 -------//
    POINT("SDO_GEOMETRY", ItemFieldTypeEnum.POINT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    //------- 布尔类型 -------//
    BOOLEAN("NUMBER(1)", ItemFieldTypeEnum.BOOLEAN, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    NONE("VARCHAR2", ItemFieldTypeEnum.NONE, 1000, null, ItemFieldTypeEnum.ParamStatus.MUST, ItemFieldTypeEnum.ParamStatus.NO, false);

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

    BaseOracleFieldTypeEnum(String dbFieldType, ItemFieldTypeEnum fieldType, Integer filedLength, Integer fieldDecimalPoint, ItemFieldTypeEnum.ParamStatus dataLength, ItemFieldTypeEnum.ParamStatus dataDecimalPoint, boolean unsigned) {
        this.dbFieldType = dbFieldType;
        this.fieldType = fieldType;
        this.filedLength = filedLength;
        this.fieldDecimalPoint = fieldDecimalPoint;
        this.dataLength = dataLength;
        this.dataDecimalPoint = dataDecimalPoint;
        this.unsigned = unsigned;
    }

    public static BaseOracleFieldTypeEnum findFieldTypeEnum(String fieldTypeStr) {
        BaseOracleFieldTypeEnum result = null;
        ItemFieldTypeEnum fieldType = ItemFieldTypeEnum.findFieldTypeEnum(fieldTypeStr);
        for (BaseOracleFieldTypeEnum fieldTypeEnum : BaseOracleFieldTypeEnum.values()) {
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

    public static ItemFieldTypeEnum dbFieldType2FieldType(String dbFieldType, Integer dbFieldLength, Integer decimalDigits) {
        ItemFieldTypeEnum result = null;

        if (StringUtils.equalsIgnoreCase(dbFieldType, "NUMBER") && dbFieldLength != null && (decimalDigits == null || decimalDigits.equals(0))) {
            if (dbFieldLength.equals(1)) {
                return ItemFieldTypeEnum.BOOLEAN;
            } else if (dbFieldLength.equals(3)) {
                return ItemFieldTypeEnum.TINYINT;
            } else if (dbFieldLength.equals(5)) {
                return ItemFieldTypeEnum.SMALLINT;
            } else if (dbFieldLength.equals(7)) {
                return ItemFieldTypeEnum.MEDIUMINT;
            } else if (dbFieldLength.equals(10)) {
                return ItemFieldTypeEnum.INTEGER;
            } else if (dbFieldLength.equals(19)) {
                return ItemFieldTypeEnum.LONG;
            }
        }

        if (StringUtils.contains(dbFieldType, "(")) {
            dbFieldType = StringUtils.substringBefore(dbFieldType, "(");
        }

        for (BaseOracleFieldTypeEnum fieldTypeEnum : BaseOracleFieldTypeEnum.values()) {
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
