package com.meiya.whalex.db.entity.ani;

import com.meiya.whalex.interior.db.constant.ItemFieldTypeEnum;

/**
 * @author 黄河森
 * @date 2022/7/6
 * @package com.meiya.whalex.db.entity.ani
 * @project whalex-data-driver
 */
public enum  ClickHouseFieldTypeEnum {
    STRING("String", "String", null, true),
    INTEGER8("Int8", "Integer", 8, false),
    INTEGER16("Int16", "Integer", 16, false),
    INTEGER32("Int32", "Integer", 32, false),
    INTEGER64("Int64", "Long", 64, false),
    LONG4("Float32", "Float", null, false),
    LONG8("Float64", "Double", null, false),
    DATE("TIMESTAMP", "Date", null, false),
    TIME("TIMESTAMP", "Time", null, false),
    BOOLEAN("boolean", "Boolean", null, false),
    BLOB("BLOB", "Bytes", null, false),
    POINT("Point", "Point", null, false),
    ARRAY("Array", "Array", null, false),
    TEXT("Text", "Text", null, false),
    NONE("String", "None", 100, false);

    private String dbFieldType;

    private String fieldType;

    private Integer filedLength;

    private boolean needLength;

    public String getDbFieldType() {
        return dbFieldType;
    }

    public void setDbFieldType(String dbFieldType) {
        this.dbFieldType = dbFieldType;
    }

    public String getFieldType() {
        return fieldType;
    }

    public void setFieldType(String fieldType) {
        this.fieldType = fieldType;
    }

    public Integer getFiledLength() {
        return filedLength;
    }

    public void setFiledLength(Integer filedLength) {
        this.filedLength = filedLength;
    }

    public boolean isNeedLength() {
        return needLength;
    }

    public void setNeedLength(boolean needLength) {
        this.needLength = needLength;
    }

    ClickHouseFieldTypeEnum(String dbFieldType, String fieldType, Integer filedLength, boolean needLength) {
        this.dbFieldType = dbFieldType;
        this.fieldType = fieldType;
        this.filedLength = filedLength;
        this.needLength = needLength;
    }

    public static ClickHouseFieldTypeEnum findFieldTypeEnum(String fieldType, Integer fieldLength) {
        ClickHouseFieldTypeEnum result = null;
        for (ClickHouseFieldTypeEnum fieldTypeEnum : ClickHouseFieldTypeEnum.values()) {
            if (fieldType.equalsIgnoreCase(fieldTypeEnum.fieldType)) {
                if (fieldType.equals(ItemFieldTypeEnum.INTEGER.getVal())) {
                    if (fieldLength != null && fieldLength >= INTEGER32.filedLength) {
                        result = INTEGER32;
                        break;
                    }
                    if (fieldLength != null && fieldTypeEnum.getFiledLength() >= fieldLength) {
                        result = fieldTypeEnum;
                        break;
                    }
                } else {
                    result = fieldTypeEnum;
                    break;
                }
            }
        }
        if (fieldType.equals(ItemFieldTypeEnum.INTEGER.getVal())) {
            result = ClickHouseFieldTypeEnum.INTEGER16;
        }
        if (result == null) {
            result = NONE;
        }
        return result;
    }
}
