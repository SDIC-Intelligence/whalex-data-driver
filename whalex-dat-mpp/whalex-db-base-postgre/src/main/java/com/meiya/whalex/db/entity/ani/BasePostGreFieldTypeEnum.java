package com.meiya.whalex.db.entity.ani;


import com.meiya.whalex.db.entity.FieldTypeAdapter;
import com.meiya.whalex.interior.db.constant.ItemFieldTypeEnum;

/**
 * PostGre 字段枚举
 *
 * @author 黄河森
 * @date 2019/12/24
 * @project whale-cloud-platformX
 */
public enum BasePostGreFieldTypeEnum implements FieldTypeAdapter {

    //------- 整数类型 -------//

    INT2("int2", ItemFieldTypeEnum.SMALLINT, null, null,ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),
    SMALLINT("smallint", ItemFieldTypeEnum.SMALLINT, null, null,ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    TINYINT("int2", ItemFieldTypeEnum.TINYINT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    INT4("int4", ItemFieldTypeEnum.INTEGER, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),
    /**
     * INT4 主键自增情况下类型为 serial
     */
    SERIAL("serial", ItemFieldTypeEnum.INTEGER, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    INTEGER("integer", ItemFieldTypeEnum.INTEGER, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    MEDIUMINT("int4", ItemFieldTypeEnum.MEDIUMINT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    INT8("int8", ItemFieldTypeEnum.LONG, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),
    LONG("bigint", ItemFieldTypeEnum.LONG, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    //------- 浮点数类型 -------//

    REAL("real", ItemFieldTypeEnum.REAL, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, true),

    FLOAT("float4", ItemFieldTypeEnum.FLOAT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, true),

    FLOAT8("float8", ItemFieldTypeEnum.DOUBLE, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, true),
    DOUBLE("double precision", ItemFieldTypeEnum.DOUBLE, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, true),

    //------- 定点数类型 -------//

    DECIMAL("decimal", ItemFieldTypeEnum.DECIMAL, null, null, ItemFieldTypeEnum.ParamStatus.SHOULD, ItemFieldTypeEnum.ParamStatus.SHOULD, false),

    NUMERIC("numeric", ItemFieldTypeEnum.NUMERIC, null, null, ItemFieldTypeEnum.ParamStatus.SHOULD, ItemFieldTypeEnum.ParamStatus.SHOULD, false),

    //------- 货币类型 -------//
    MONEY("money", ItemFieldTypeEnum.MONEY, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    //------- 位类型 -------//

    BIT("bit", ItemFieldTypeEnum.BIT, null, null, ItemFieldTypeEnum.ParamStatus.SHOULD, ItemFieldTypeEnum.ParamStatus.NO, false),

    //------- 日期类型 -------//

    DATE("date", ItemFieldTypeEnum.DATE, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    YEAR("date", ItemFieldTypeEnum.YEAR, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    TIMESTAMP("timestamp", ItemFieldTypeEnum.TIMESTAMP, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    DATETIME("timestamp", ItemFieldTypeEnum.DATETIME, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    DATETIME_2("timestamp", ItemFieldTypeEnum.TIME, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    TIME("time", ItemFieldTypeEnum.SMART_TIME, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    //------- 文本类型 -------//

    CHAR("char", ItemFieldTypeEnum.CHAR, null, null, ItemFieldTypeEnum.ParamStatus.SHOULD, ItemFieldTypeEnum.ParamStatus.NO, false),

    STRING("varchar", ItemFieldTypeEnum.STRING, null, null, ItemFieldTypeEnum.ParamStatus.SHOULD, ItemFieldTypeEnum.ParamStatus.NO, false),

    TEXT("text", ItemFieldTypeEnum.TEXT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    TINYTEXT("text", ItemFieldTypeEnum.TINYTEXT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),


    MEDIUMTEXT("text", ItemFieldTypeEnum.MEDIUMTEXT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    LONGTEXT("text", ItemFieldTypeEnum.LONGTEXT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    //------- 枚举类型 -------//

    ENUM("enum", ItemFieldTypeEnum.ENUM, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    //------- 多选值类型 -------//

    SET("text", ItemFieldTypeEnum.SET, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    //------- 二进制大对象 -------//

    BLOB("bytea", ItemFieldTypeEnum.BLOB, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    BYTES("bytea", ItemFieldTypeEnum.BYTES, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    TINYBLOB("bytea", ItemFieldTypeEnum.TINYBLOB, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    MEDIUMBLOB("bytea", ItemFieldTypeEnum.MEDIUMBLOB, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    LONGBLOB("bytea", ItemFieldTypeEnum.LONGBLOB, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    IMAGE("bytea", ItemFieldTypeEnum.IMAGE, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    //------- 二进制字符串类型 -------//

    BINARY("bytea", ItemFieldTypeEnum.BINARY, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    VARBINARY("bytea", ItemFieldTypeEnum.VARBINARY, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    //------- JSON 类型 -------//

    JSON("json", ItemFieldTypeEnum.OBJECT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    //------- 地理位置 -------//

    POINT("point", ItemFieldTypeEnum.POINT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    //------- 布尔类型 -------//

    BOOLEAN("boolean", ItemFieldTypeEnum.BOOLEAN, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    //------- 集合类型 -------//

    ARRAY_INT2("int2[]", ItemFieldTypeEnum.ARRAY_SMALLINT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_SMALLINT("smallint[]", ItemFieldTypeEnum.ARRAY_SMALLINT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_TINYINT("int2[]", ItemFieldTypeEnum.ARRAY_TINYINT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_INTEGER("integer[]", ItemFieldTypeEnum.ARRAY_INTEGER, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_INT4("int4[]", ItemFieldTypeEnum.ARRAY_INTEGER, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_MEDIUMINT("integer[]", ItemFieldTypeEnum.ARRAY_MEDIUMINT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_INT8("int8[]", ItemFieldTypeEnum.ARRAY_LONG, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_LONG("bigint[]", ItemFieldTypeEnum.ARRAY_LONG, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_FLOAT("float4[]", ItemFieldTypeEnum.ARRAY_FLOAT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_DOUBLE("double precision[]", ItemFieldTypeEnum.ARRAY_DOUBLE, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_NUMERIC("numeric[]", ItemFieldTypeEnum.ARRAY_NUMERIC, null, null, ItemFieldTypeEnum.ParamStatus.SHOULD, ItemFieldTypeEnum.ParamStatus.SHOULD, false),
    ARRAY_DECIMAL("decimal[]", ItemFieldTypeEnum.ARRAY_DECIMAL, null, null, ItemFieldTypeEnum.ParamStatus.SHOULD, ItemFieldTypeEnum.ParamStatus.SHOULD, false),
    ARRAY_BIT("bit[]", ItemFieldTypeEnum.ARRAY_BIT, null, null, ItemFieldTypeEnum.ParamStatus.SHOULD, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_CHAR("char[]", ItemFieldTypeEnum.ARRAY_CHAR, null, null, ItemFieldTypeEnum.ParamStatus.SHOULD, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_STRING("varchar", ItemFieldTypeEnum.ARRAY_STRING, null, null, ItemFieldTypeEnum.ParamStatus.SHOULD, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_TEXT("text[]", ItemFieldTypeEnum.ARRAY_TEXT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_TINYTEXT("text[]", ItemFieldTypeEnum.ARRAY_TINYTEXT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_MEDIUMTEXT("text[]", ItemFieldTypeEnum.ARRAY_MEDIUMTEXT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_LONGTEXT("text[]", ItemFieldTypeEnum.ARRAY_LONGTEXT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_POINT("point[]", ItemFieldTypeEnum.ARRAY_POINT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY("text[]", ItemFieldTypeEnum.ARRAY, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),
    NONE("varchar", ItemFieldTypeEnum.NONE, null, null, ItemFieldTypeEnum.ParamStatus.SHOULD, ItemFieldTypeEnum.ParamStatus.NO, false);

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

    BasePostGreFieldTypeEnum(String dbFieldType, ItemFieldTypeEnum fieldType, Integer filedLength, Integer fieldDecimalPoint, ItemFieldTypeEnum.ParamStatus dataLength, ItemFieldTypeEnum.ParamStatus dataDecimalPoint, boolean unsigned) {
        this.dbFieldType = dbFieldType;
        this.fieldType = fieldType;
        this.filedLength = filedLength;
        this.fieldDecimalPoint = fieldDecimalPoint;
        this.dataLength = dataLength;
        this.dataDecimalPoint = dataDecimalPoint;
        this.unsigned = unsigned;
    }

    public static BasePostGreFieldTypeEnum findFieldTypeEnum(String fieldTypeStr) {
        BasePostGreFieldTypeEnum result = null;
        ItemFieldTypeEnum fieldType = ItemFieldTypeEnum.findFieldTypeEnum(fieldTypeStr);
        for (BasePostGreFieldTypeEnum fieldTypeEnum : BasePostGreFieldTypeEnum.values()) {
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

    public static ItemFieldTypeEnum dbFieldType2FieldType(String dbFieldType) {
        ItemFieldTypeEnum result = null;
        for (BasePostGreFieldTypeEnum fieldTypeEnum : BasePostGreFieldTypeEnum.values()) {
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
