package com.meiya.whalex.db.entity.document;

import com.meiya.whalex.db.entity.FieldTypeAdapter;
import com.meiya.whalex.interior.db.constant.ItemFieldTypeEnum;
import com.mongodb.BasicDBList;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.types.BSONTimestamp;
import org.bson.types.Binary;
import org.bson.types.Decimal128;

import java.util.ArrayList;
import java.util.Date;
import java.util.Map;

/**
 * MySql 字段枚举
 *
 * @author 黄河森
 * @date 2019/12/24
 * @project whale-cloud-platformX
 */
public enum MongoDbFieldTypeEnum implements FieldTypeAdapter {

    //------- 整数类型 -------//

    INTEGER(Integer.class.getSimpleName(), ItemFieldTypeEnum.INTEGER, null, null, ItemFieldTypeEnum.ParamStatus.SHOULD, ItemFieldTypeEnum.ParamStatus.NO, true),

    TINYINT(Integer.class.getSimpleName(), ItemFieldTypeEnum.TINYINT, null, null, ItemFieldTypeEnum.ParamStatus.SHOULD, ItemFieldTypeEnum.ParamStatus.NO, true),

    SMALLINT(Integer.class.getSimpleName(), ItemFieldTypeEnum.SMALLINT, null, null,ItemFieldTypeEnum.ParamStatus.SHOULD, ItemFieldTypeEnum.ParamStatus.NO, true),

    MEDIUMINT(Integer.class.getSimpleName(), ItemFieldTypeEnum.MEDIUMINT, null, null, ItemFieldTypeEnum.ParamStatus.SHOULD, ItemFieldTypeEnum.ParamStatus.NO, true),

    LONG(Long.class.getSimpleName(), ItemFieldTypeEnum.LONG, null, null, ItemFieldTypeEnum.ParamStatus.SHOULD, ItemFieldTypeEnum.ParamStatus.NO, true),

    //------- 浮点数类型 -------//

    DOUBLE(Double.class.getSimpleName(), ItemFieldTypeEnum.DOUBLE, null, null, ItemFieldTypeEnum.ParamStatus.SHOULD, ItemFieldTypeEnum.ParamStatus.SHOULD, true),

    REAL(Double.class.getSimpleName(), ItemFieldTypeEnum.REAL, null, null, ItemFieldTypeEnum.ParamStatus.SHOULD, ItemFieldTypeEnum.ParamStatus.SHOULD, true),

    FLOAT(Float.class.getSimpleName(), ItemFieldTypeEnum.FLOAT, null, null, ItemFieldTypeEnum.ParamStatus.SHOULD, ItemFieldTypeEnum.ParamStatus.SHOULD, true),

    //------- 定点数类型 -------//

    DECIMAL(Decimal128.class.getSimpleName(), ItemFieldTypeEnum.DECIMAL, null, null, ItemFieldTypeEnum.ParamStatus.SHOULD, ItemFieldTypeEnum.ParamStatus.SHOULD, false),

    NUMERIC(Decimal128.class.getSimpleName(), ItemFieldTypeEnum.NUMERIC, null, null, ItemFieldTypeEnum.ParamStatus.SHOULD, ItemFieldTypeEnum.ParamStatus.SHOULD, false),

    //------- 货币类型 -------//
    MONEY(Decimal128.class.getSimpleName(), ItemFieldTypeEnum.MONEY, 17, 2, ItemFieldTypeEnum.ParamStatus.MUST, ItemFieldTypeEnum.ParamStatus.MUST, false),

    //------- 位类型 -------//

    BIT(Byte.class.getSimpleName(), ItemFieldTypeEnum.BIT, null, null, ItemFieldTypeEnum.ParamStatus.SHOULD, ItemFieldTypeEnum.ParamStatus.NO, false),

    //------- 日期类型 -------//

    DATETIME(Date.class.getSimpleName(), ItemFieldTypeEnum.DATETIME, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    YEAR(Date.class.getSimpleName(), ItemFieldTypeEnum.YEAR, null, null, ItemFieldTypeEnum.ParamStatus.SHOULD, ItemFieldTypeEnum.ParamStatus.NO, false),

    DATE(Date.class.getSimpleName(), ItemFieldTypeEnum.DATE, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    DATETIME_2(Date.class.getSimpleName(), ItemFieldTypeEnum.TIME, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    TIME(Date.class.getSimpleName(), ItemFieldTypeEnum.SMART_TIME, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    TIMESTAMP(BSONTimestamp.class.getSimpleName(), ItemFieldTypeEnum.TIMESTAMP, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    //------- 文本类型 -------//

    STRING(String.class.getSimpleName(), ItemFieldTypeEnum.STRING, 1000, null, ItemFieldTypeEnum.ParamStatus.MUST, ItemFieldTypeEnum.ParamStatus.NO, false),

    CHAR(String.class.getSimpleName(), ItemFieldTypeEnum.CHAR, null, null, ItemFieldTypeEnum.ParamStatus.SHOULD, ItemFieldTypeEnum.ParamStatus.NO, false),

    TINYTEXT(String.class.getSimpleName(), ItemFieldTypeEnum.TINYTEXT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    TEXT(String.class.getSimpleName(), ItemFieldTypeEnum.TEXT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    MEDIUMTEXT(String.class.getSimpleName(), ItemFieldTypeEnum.MEDIUMTEXT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    LONGTEXT(String.class.getSimpleName(), ItemFieldTypeEnum.LONGTEXT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    //------- 二进制字符串类型 -------//

    BINARY(Binary.class.getSimpleName(), ItemFieldTypeEnum.BINARY, null, null, ItemFieldTypeEnum.ParamStatus.SHOULD, ItemFieldTypeEnum.ParamStatus.NO, false),

    VARBINARY(Binary.class.getSimpleName(), ItemFieldTypeEnum.VARBINARY, 1000, null, ItemFieldTypeEnum.ParamStatus.MUST, ItemFieldTypeEnum.ParamStatus.NO, false),

    //------- 二进制大对象 -------//

    BLOB(Binary.class.getSimpleName(), ItemFieldTypeEnum.BLOB, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    BYTES(Binary.class.getSimpleName(), ItemFieldTypeEnum.BYTES, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    TINYBLOB(Binary.class.getSimpleName(), ItemFieldTypeEnum.TINYBLOB, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    MEDIUMBLOB(Binary.class.getSimpleName(), ItemFieldTypeEnum.MEDIUMBLOB, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    LONGBLOB(Binary.class.getSimpleName(), ItemFieldTypeEnum.LONGBLOB, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    IMAGE(Binary.class.getSimpleName(), ItemFieldTypeEnum.IMAGE, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    //------- JSON 类型 -------//

    JSON(Document.class.getSimpleName(), ItemFieldTypeEnum.OBJECT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    //------- 布尔类型 -------//

    BOOLEAN(Boolean.class.getSimpleName(), ItemFieldTypeEnum.BOOLEAN, 1, null, ItemFieldTypeEnum.ParamStatus.MUST, ItemFieldTypeEnum.ParamStatus.NO, false),

    //------- 集合类型 -------//

    ARRAY(ArrayList.class.getSimpleName(), ItemFieldTypeEnum.ARRAY, 5000, null, ItemFieldTypeEnum.ParamStatus.MUST, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_TINYINT(ArrayList.class.getSimpleName(), ItemFieldTypeEnum.ARRAY_TINYINT, 5000, null, ItemFieldTypeEnum.ParamStatus.MUST, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_SMALLINT(ArrayList.class.getSimpleName(), ItemFieldTypeEnum.ARRAY_SMALLINT, 5000, null, ItemFieldTypeEnum.ParamStatus.MUST, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_MEDIUMINT(ArrayList.class.getSimpleName(), ItemFieldTypeEnum.ARRAY_MEDIUMINT, 5000, null, ItemFieldTypeEnum.ParamStatus.MUST, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_INTEGER(ArrayList.class.getSimpleName(), ItemFieldTypeEnum.ARRAY_INTEGER, 5000, null, ItemFieldTypeEnum.ParamStatus.MUST, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_LONG(ArrayList.class.getSimpleName(), ItemFieldTypeEnum.ARRAY_LONG, 5000, null, ItemFieldTypeEnum.ParamStatus.MUST, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_FLOAT(ArrayList.class.getSimpleName(), ItemFieldTypeEnum.ARRAY_FLOAT, 5000, null, ItemFieldTypeEnum.ParamStatus.MUST, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_DOUBLE(ArrayList.class.getSimpleName(), ItemFieldTypeEnum.ARRAY_DOUBLE, 5000, null, ItemFieldTypeEnum.ParamStatus.MUST, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_NUMERIC(ArrayList.class.getSimpleName(), ItemFieldTypeEnum.ARRAY_NUMERIC, 5000, null, ItemFieldTypeEnum.ParamStatus.MUST, ItemFieldTypeEnum.ParamStatus.NO, false),

    ARRAY_DECIMAL(ArrayList.class.getSimpleName(), ItemFieldTypeEnum.ARRAY_DECIMAL, 5000, null, ItemFieldTypeEnum.ParamStatus.MUST, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_BIT(ArrayList.class.getSimpleName(), ItemFieldTypeEnum.ARRAY_BIT, 5000, null, ItemFieldTypeEnum.ParamStatus.MUST, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_CHAR(ArrayList.class.getSimpleName(), ItemFieldTypeEnum.ARRAY_CHAR, 5000, null, ItemFieldTypeEnum.ParamStatus.MUST, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_STRING(ArrayList.class.getSimpleName(), ItemFieldTypeEnum.ARRAY_STRING, 5000, null, ItemFieldTypeEnum.ParamStatus.MUST, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_TINYTEXT(ArrayList.class.getSimpleName(), ItemFieldTypeEnum.ARRAY_TINYTEXT, 5000, null, ItemFieldTypeEnum.ParamStatus.MUST, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_TEXT(ArrayList.class.getSimpleName(), ItemFieldTypeEnum.ARRAY_TEXT, 5000, null, ItemFieldTypeEnum.ParamStatus.MUST, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_MEDIUMTEXT(ArrayList.class.getSimpleName(), ItemFieldTypeEnum.ARRAY_MEDIUMTEXT, 5000, null, ItemFieldTypeEnum.ParamStatus.MUST, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_LONGTEXT(ArrayList.class.getSimpleName(), ItemFieldTypeEnum.ARRAY_LONGTEXT, 5000, null, ItemFieldTypeEnum.ParamStatus.MUST, ItemFieldTypeEnum.ParamStatus.NO, false),
    ARRAY_POINT(ArrayList.class.getSimpleName(), ItemFieldTypeEnum.ARRAY_POINT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    //------- 地理位置 -------//

    POINT(ArrayList.class.getSimpleName(), ItemFieldTypeEnum.POINT, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),


    //------- 枚举类型 -------//

    ENUM(ArrayList.class.getSimpleName(), ItemFieldTypeEnum.ENUM, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),

    //------- 多选值类型 -------//

    SET(ArrayList.class.getSimpleName(), ItemFieldTypeEnum.SET, null, null, ItemFieldTypeEnum.ParamStatus.NO, ItemFieldTypeEnum.ParamStatus.NO, false),


    NONE(String.class.getSimpleName(), ItemFieldTypeEnum.NONE, 1000, null, ItemFieldTypeEnum.ParamStatus.MUST, ItemFieldTypeEnum.ParamStatus.NO, false);


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

    MongoDbFieldTypeEnum(String dbFieldType, ItemFieldTypeEnum fieldType, Integer filedLength, Integer fieldDecimalPoint, ItemFieldTypeEnum.ParamStatus dataLength, ItemFieldTypeEnum.ParamStatus dataDecimalPoint, boolean unsigned) {
        this.dbFieldType = dbFieldType;
        this.fieldType = fieldType;
        this.filedLength = filedLength;
        this.fieldDecimalPoint = fieldDecimalPoint;
        this.dataLength = dataLength;
        this.dataDecimalPoint = dataDecimalPoint;
        this.unsigned = unsigned;
    }

    public static MongoDbFieldTypeEnum findFieldTypeEnum(String fieldTypeStr) {
        MongoDbFieldTypeEnum result = null;
        ItemFieldTypeEnum fieldType = ItemFieldTypeEnum.findFieldTypeEnum(fieldTypeStr);
        for (MongoDbFieldTypeEnum fieldTypeEnum : MongoDbFieldTypeEnum.values()) {
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
        if (StringUtils.isNotBlank(dbFieldType)) {
            if (StringUtils.equalsIgnoreCase(dbFieldType, BasicDBList.class.getSimpleName())) {
                dbFieldType = ArrayList.class.getSimpleName();
            } else if (StringUtils.equalsIgnoreCase(dbFieldType, Map.class.getSimpleName())) {
                dbFieldType = Document.class.getSimpleName();
            }
        }
        for (MongoDbFieldTypeEnum fieldTypeEnum : MongoDbFieldTypeEnum.values()) {
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
