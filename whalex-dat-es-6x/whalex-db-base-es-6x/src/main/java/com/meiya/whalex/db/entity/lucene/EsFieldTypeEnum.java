package com.meiya.whalex.db.entity.lucene;

import com.meiya.whalex.db.entity.FieldTypeAdapter;
import com.meiya.whalex.interior.db.constant.ItemFieldTypeEnum;

/**
 * PostGre 字段枚举
 *
 * @author 黄河森
 * @date 2019/12/24
 * @project whale-cloud-platformX
 */
public enum EsFieldTypeEnum implements FieldTypeAdapter {

    //------- 整数类型 -------//

    INTEGER("integer", ItemFieldTypeEnum.INTEGER, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    TINYINT("integer", ItemFieldTypeEnum.TINYINT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    SMALLINT("integer", ItemFieldTypeEnum.SMALLINT, null, null,ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    MEDIUMINT("integer", ItemFieldTypeEnum.MEDIUMINT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    LONG("long", ItemFieldTypeEnum.LONG, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    //------- 浮点数类型 -------//

    FLOAT("float", ItemFieldTypeEnum.FLOAT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    REAL("float", ItemFieldTypeEnum.REAL, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    DOUBLE("double", ItemFieldTypeEnum.DOUBLE, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    //------- 日期类型 -------//

    DATE("date", ItemFieldTypeEnum.DATE, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    YEAR("date", ItemFieldTypeEnum.YEAR, null, null, ItemFieldTypeEnum.ParamStatus.SHOULD, ItemFieldTypeEnum.ParamStatus.NO, false),

    DATETIME("date", ItemFieldTypeEnum.DATETIME, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    DATETIME_2("date", ItemFieldTypeEnum.TIME, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    TIME("date", ItemFieldTypeEnum.SMART_TIME, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    TIMESTAMP("date", ItemFieldTypeEnum.TIMESTAMP, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    //------- 文本类型 -------//

    TEXT("text", ItemFieldTypeEnum.TEXT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    STRING("text", ItemFieldTypeEnum.STRING, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    STRING_KEYWORD("keyword", ItemFieldTypeEnum.STRING, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    CHAR("keyword", ItemFieldTypeEnum.CHAR, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    TINYTEXT("text", ItemFieldTypeEnum.TINYTEXT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    MEDIUMTEXT("text", ItemFieldTypeEnum.MEDIUMTEXT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    LONGTEXT("text", ItemFieldTypeEnum.LONGTEXT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    //------- 定点数类型 -------//

    DECIMAL("keyword", ItemFieldTypeEnum.DECIMAL, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    NUMERIC("keyword", ItemFieldTypeEnum.NUMERIC, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    //------- 货币类型 -------//
    MONEY("keyword", ItemFieldTypeEnum.MONEY, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    //------- 位类型 -------//

    BIT("keyword", ItemFieldTypeEnum.BIT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    //------- 枚举类型 -------//

    ENUM("keyword", ItemFieldTypeEnum.ENUM, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    //------- 多选值类型 -------//

    SET("keyword", ItemFieldTypeEnum.SET, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    //------- 二进制大对象 -------//

    BLOB("binary", ItemFieldTypeEnum.BLOB, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    BYTES("binary", ItemFieldTypeEnum.BYTES, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    TINYBLOB("binary", ItemFieldTypeEnum.TINYBLOB, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    MEDIUMBLOB("binary", ItemFieldTypeEnum.MEDIUMBLOB, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    LONGBLOB("binary", ItemFieldTypeEnum.LONGBLOB, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    IMAGE("binary", ItemFieldTypeEnum.IMAGE, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    //------- 二进制字符串类型 -------//

    BINARY("binary", ItemFieldTypeEnum.BINARY, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    VARBINARY("binary", ItemFieldTypeEnum.VARBINARY, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    //------- JSON 类型 -------//

    JSON("nested", ItemFieldTypeEnum.OBJECT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    //------- 地理位置 -------//

    POINT("geo_point", ItemFieldTypeEnum.POINT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    //------- 布尔类型 -------//

    BOOLEAN("boolean", ItemFieldTypeEnum.BOOLEAN, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    //------- 集合类型 -------//

    ARRAY("text", ItemFieldTypeEnum.ARRAY, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_TINYINT("integer", ItemFieldTypeEnum.ARRAY_TINYINT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_SMALLINT("integer", ItemFieldTypeEnum.ARRAY_SMALLINT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_MEDIUMINT("integer", ItemFieldTypeEnum.ARRAY_MEDIUMINT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_INTEGER("integer", ItemFieldTypeEnum.ARRAY_INTEGER, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_LONG("long", ItemFieldTypeEnum.ARRAY_LONG, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_FLOAT("float", ItemFieldTypeEnum.ARRAY_FLOAT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_DOUBLE("double", ItemFieldTypeEnum.ARRAY_DOUBLE, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_NUMERIC("keyword", ItemFieldTypeEnum.ARRAY_NUMERIC, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    ARRAY_DECIMAL("keyword", ItemFieldTypeEnum.ARRAY_DECIMAL, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_BIT("keyword", ItemFieldTypeEnum.ARRAY_BIT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_CHAR("keyword", ItemFieldTypeEnum.ARRAY_CHAR, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_STRING("text", ItemFieldTypeEnum.ARRAY_STRING, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_TINYTEXT("text", ItemFieldTypeEnum.ARRAY_TINYTEXT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_TEXT("text", ItemFieldTypeEnum.ARRAY_TEXT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_MEDIUMTEXT("text", ItemFieldTypeEnum.ARRAY_MEDIUMTEXT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_LONGTEXT("text", ItemFieldTypeEnum.ARRAY_LONGTEXT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_POINT("geo_point", ItemFieldTypeEnum.ARRAY_POINT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    COMPLETION("completion", ItemFieldTypeEnum.COMPLETION, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    NONE("text", ItemFieldTypeEnum.NONE, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false);

    ;
    /*TEXT("text"),
    KEYWORD("keyword"),
    LONG("long"),
    INTEGER("integer"),
    SHORT("short"),
    BYTE("byte"),
    DOUBLE("double"),
    FLOAT("float"),
    DATE("date"),
    TIME("time"),
    BOOLEAN("boolean"),
    GEO_POINT("geo_point"),
    NESTED("nested"),
    BINARY("binary"),
    COMPLETION("completion");*/

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

    EsFieldTypeEnum(String dbFieldType, ItemFieldTypeEnum fieldType, Integer filedLength, Integer fieldDecimalPoint, ItemFieldTypeEnum.ParamStatus dataLength, ItemFieldTypeEnum.ParamStatus dataDecimalPoint, boolean unsigned) {
        this.dbFieldType = dbFieldType;
        this.fieldType = fieldType;
        this.filedLength = filedLength;
        this.fieldDecimalPoint = fieldDecimalPoint;
        this.dataLength = dataLength;
        this.dataDecimalPoint = dataDecimalPoint;
        this.unsigned = unsigned;
    }

    public static EsFieldTypeEnum findFieldTypeEnum(String fieldTypeStr) {
        EsFieldTypeEnum result = null;
        ItemFieldTypeEnum fieldType = ItemFieldTypeEnum.findFieldTypeEnum(fieldTypeStr);
        for (EsFieldTypeEnum fieldTypeEnum : EsFieldTypeEnum.values()) {
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
        for (EsFieldTypeEnum fieldTypeEnum : EsFieldTypeEnum.values()) {
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
