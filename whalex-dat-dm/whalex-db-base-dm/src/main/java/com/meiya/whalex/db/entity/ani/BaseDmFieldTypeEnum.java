package com.meiya.whalex.db.entity.ani;

import com.meiya.whalex.db.entity.FieldTypeAdapter;
import com.meiya.whalex.interior.db.constant.ItemFieldTypeEnum;

/**
 * DM 字段枚举
 *
 * @author 蔡荣桂
 * @date 2021/4/19
 * @project whale-cloud-platformX
 */
public enum BaseDmFieldTypeEnum implements FieldTypeAdapter {
    //------- 整数类型 -------//

    TINYINT("tinyint", ItemFieldTypeEnum.TINYINT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, true),

    SMALLINT("smallint", ItemFieldTypeEnum.SMALLINT, null, null,ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, true),

    INTEGER("int", ItemFieldTypeEnum.INTEGER, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, true),

    MEDIUMINT("int", ItemFieldTypeEnum.MEDIUMINT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, true),

    LONG("bigint", ItemFieldTypeEnum.LONG, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, true),

    //------- 浮点数类型 -------//

    REAL("real", ItemFieldTypeEnum.REAL, null, null, ItemFieldTypeEnum.ParamStatus.SHOULD, ItemFieldTypeEnum.ParamStatus.NO, true),

    FLOAT("float", ItemFieldTypeEnum.FLOAT, null, null, ItemFieldTypeEnum.ParamStatus.SHOULD, ItemFieldTypeEnum.ParamStatus.NO, true),

    DOUBLE("double", ItemFieldTypeEnum.DOUBLE, null, null, ItemFieldTypeEnum.ParamStatus.SHOULD, ItemFieldTypeEnum.ParamStatus.NO, true),

    //------- 定点数类型 -------//

    DECIMAL("decimal", ItemFieldTypeEnum.DECIMAL, null, null, ItemFieldTypeEnum.ParamStatus.SHOULD, ItemFieldTypeEnum.ParamStatus.SHOULD, false),

    NUMERIC("numeric", ItemFieldTypeEnum.NUMERIC, null, null, ItemFieldTypeEnum.ParamStatus.SHOULD, ItemFieldTypeEnum.ParamStatus.SHOULD, false),

    //------- 货币类型 -------//
    MONEY("decimal", ItemFieldTypeEnum.MONEY, 17, 2, ItemFieldTypeEnum.ParamStatus.MUST, ItemFieldTypeEnum.ParamStatus.MUST, false),

    //------- 位类型 -------//

    BIT("bit", ItemFieldTypeEnum.BIT, null, null, ItemFieldTypeEnum.ParamStatus.SHOULD, ItemFieldTypeEnum.ParamStatus.NO, false),

    //------- 日期类型 -------//
    DATE("date", ItemFieldTypeEnum.DATE, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    YEAR("date", ItemFieldTypeEnum.YEAR, null, null, ItemFieldTypeEnum.ParamStatus.SHOULD, ItemFieldTypeEnum.ParamStatus.NO, false),

    DATETIME("datetime", ItemFieldTypeEnum.DATETIME, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    DATETIME_2("datetime", ItemFieldTypeEnum.TIME, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    TIME("time", ItemFieldTypeEnum.SMART_TIME, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    TIMESTAMP("timestamp", ItemFieldTypeEnum.TIMESTAMP, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    //------- 文本类型 -------//

    CHAR("char", ItemFieldTypeEnum.CHAR, null, null, ItemFieldTypeEnum.ParamStatus.SHOULD, ItemFieldTypeEnum.ParamStatus.NO, false),

    STRING("varchar", ItemFieldTypeEnum.STRING, 1000, null, ItemFieldTypeEnum.ParamStatus.MUST, ItemFieldTypeEnum.ParamStatus.NO, false),

    TEXT("text", ItemFieldTypeEnum.TEXT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    TINYTEXT("text", ItemFieldTypeEnum.TINYTEXT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    MEDIUMTEXT("longvarchar", ItemFieldTypeEnum.MEDIUMTEXT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    LONGTEXT("clob", ItemFieldTypeEnum.LONGTEXT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    //------- 枚举类型 -------//

    ENUM("text", ItemFieldTypeEnum.ENUM, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    //------- 多选值类型 -------//

    SET("text", ItemFieldTypeEnum.SET, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    //------- 二进制字符串类型 -------//

    BINARY("binary", ItemFieldTypeEnum.BINARY, null, null, ItemFieldTypeEnum.ParamStatus.SHOULD, ItemFieldTypeEnum.ParamStatus.NO, false),

    VARBINARY("varbinary", ItemFieldTypeEnum.VARBINARY, null, null, ItemFieldTypeEnum.ParamStatus.SHOULD, ItemFieldTypeEnum.ParamStatus.NO, false),

    //------- 二进制大对象 -------//

    BLOB("blob", ItemFieldTypeEnum.BLOB, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    BYTES("blob", ItemFieldTypeEnum.BYTES, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    TINYBLOB("blob", ItemFieldTypeEnum.TINYBLOB, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    MEDIUMBLOB("blob", ItemFieldTypeEnum.MEDIUMBLOB, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    LONGBLOB("blob", ItemFieldTypeEnum.LONGBLOB, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    IMAGE("image", ItemFieldTypeEnum.IMAGE, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    //------- JSON 类型 -------//

    JSON("text", ItemFieldTypeEnum.OBJECT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    //------- 地理位置 -------//

    /**
     * 达梦开启 DMGEO 系统包方式
     *
     * 开启 geo 系统包
     * SP_INIT_GEO_SYS(1)
     *
     * 校验是否开启成功 1 表示开启
     * SELECT SF_CHECK_GEO_SYS();
     */
    POINT("st_point", ItemFieldTypeEnum.POINT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    //------- 布尔类型 -------//

    BOOLEAN("bit", ItemFieldTypeEnum.BOOLEAN, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    //------- 集合类型 -------//

    ARRAY("varchar", ItemFieldTypeEnum.ARRAY, 5000, null, ItemFieldTypeEnum.ParamStatus.MUST, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_TINYINT("varchar", ItemFieldTypeEnum.ARRAY_TINYINT, 5000, null, ItemFieldTypeEnum.ParamStatus.MUST, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_SMALLINT("varchar", ItemFieldTypeEnum.ARRAY_SMALLINT, 5000, null, ItemFieldTypeEnum.ParamStatus.MUST, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_MEDIUMINT("varchar", ItemFieldTypeEnum.ARRAY_MEDIUMINT, 5000, null, ItemFieldTypeEnum.ParamStatus.MUST, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_INTEGER("varchar", ItemFieldTypeEnum.ARRAY_INTEGER, 5000, null, ItemFieldTypeEnum.ParamStatus.MUST, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_LONG("varchar", ItemFieldTypeEnum.ARRAY_LONG, 5000, null, ItemFieldTypeEnum.ParamStatus.MUST, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_FLOAT("varchar", ItemFieldTypeEnum.ARRAY_FLOAT, 5000, null, ItemFieldTypeEnum.ParamStatus.MUST, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_DOUBLE("varchar", ItemFieldTypeEnum.ARRAY_DOUBLE, 5000, null, ItemFieldTypeEnum.ParamStatus.MUST, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_NUMERIC("varchar", ItemFieldTypeEnum.ARRAY_NUMERIC, 5000, null, ItemFieldTypeEnum.ParamStatus.MUST, ItemFieldTypeEnum.ParamStatus.NO, false),

    ARRAY_DECIMAL("varchar", ItemFieldTypeEnum.ARRAY_DECIMAL, 5000, null, ItemFieldTypeEnum.ParamStatus.MUST, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_BIT("varchar", ItemFieldTypeEnum.ARRAY_BIT, 5000, null, ItemFieldTypeEnum.ParamStatus.MUST, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_CHAR("varchar", ItemFieldTypeEnum.ARRAY_CHAR, 5000, null, ItemFieldTypeEnum.ParamStatus.MUST, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_STRING("varchar", ItemFieldTypeEnum.ARRAY_STRING, 5000, null, ItemFieldTypeEnum.ParamStatus.MUST, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_TINYTEXT("varchar", ItemFieldTypeEnum.ARRAY_TINYTEXT, 5000, null, ItemFieldTypeEnum.ParamStatus.MUST, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_TEXT("varchar", ItemFieldTypeEnum.ARRAY_TEXT, 5000, null, ItemFieldTypeEnum.ParamStatus.MUST, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_MEDIUMTEXT("varchar", ItemFieldTypeEnum.ARRAY_MEDIUMTEXT, 5000, null, ItemFieldTypeEnum.ParamStatus.MUST, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_LONGTEXT("varchar", ItemFieldTypeEnum.ARRAY_LONGTEXT, 5000, null, ItemFieldTypeEnum.ParamStatus.MUST, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_POINT("varchar", ItemFieldTypeEnum.ARRAY_POINT, 5000, null, ItemFieldTypeEnum.ParamStatus.MUST, ItemFieldTypeEnum.ParamStatus.NO, false),

    NONE("varchar", ItemFieldTypeEnum.NONE, 1000, null, ItemFieldTypeEnum.ParamStatus.MUST, ItemFieldTypeEnum.ParamStatus.NO, false);

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

    BaseDmFieldTypeEnum(String dbFieldType, ItemFieldTypeEnum fieldType, Integer filedLength, Integer fieldDecimalPoint, ItemFieldTypeEnum.ParamStatus dataLength, ItemFieldTypeEnum.ParamStatus dataDecimalPoint, boolean unsigned) {
        this.dbFieldType = dbFieldType;
        this.fieldType = fieldType;
        this.filedLength = filedLength;
        this.fieldDecimalPoint = fieldDecimalPoint;
        this.dataLength = dataLength;
        this.dataDecimalPoint = dataDecimalPoint;
        this.unsigned = unsigned;
    }

    public static BaseDmFieldTypeEnum findFieldTypeEnum(String fieldTypeStr) {
        BaseDmFieldTypeEnum result = null;
        ItemFieldTypeEnum fieldType = ItemFieldTypeEnum.findFieldTypeEnum(fieldTypeStr);
        for (BaseDmFieldTypeEnum fieldTypeEnum : BaseDmFieldTypeEnum.values()) {
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
        for (BaseDmFieldTypeEnum fieldTypeEnum : BaseDmFieldTypeEnum.values()) {
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
