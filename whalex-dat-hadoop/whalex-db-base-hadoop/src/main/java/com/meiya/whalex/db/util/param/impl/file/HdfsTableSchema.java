package com.meiya.whalex.db.util.param.impl.file;

import com.meiya.whalex.interior.db.constant.ItemFieldTypeEnum;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;


/**
 * @author 黄河森
 * @date 2023/5/6
 * @package com.meiya.whalex.db.util.param.impl.file
 * @project whalex-data-driver
 */
public class HdfsTableSchema {

    public static Type parquetTypeConvert(String field, ItemFieldTypeEnum type) {
        switch (type) {
            case STRING:
            case TEXT:
            case LONGTEXT:
            case OBJECT:
            case ARRAY:
            case POINT:
                return new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BINARY, field, OriginalType.UTF8);
            case TIME:
            case DATE:
                return new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT96, field);
            case BOOLEAN:
                return new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BOOLEAN, field);
            case DOUBLE:
                return new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.DOUBLE, field);
            case FLOAT:
                return new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.FLOAT, field);
            case INTEGER:
                return new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT32, field);
            case LONG:
                return new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT64, field);
            case BYTES:
                return new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, field);
            default:
                return new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BINARY, field, OriginalType.UTF8);
        }
    }

    /**
     * 解析 orc 类型
     *
     * @param type
     * @return
     */
    public static ObjectInspector orcTypeConvert(ItemFieldTypeEnum type) {
        switch (type) {
            case INTEGER:
                return ObjectInspectorFactory.getReflectionObjectInspector(Integer.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
            case LONG:
                return ObjectInspectorFactory.getReflectionObjectInspector(Long.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
            case DOUBLE:
                return ObjectInspectorFactory.getReflectionObjectInspector(Double.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
            case FLOAT:
                return ObjectInspectorFactory.getReflectionObjectInspector(Float.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
            case BOOLEAN:
                return ObjectInspectorFactory.getReflectionObjectInspector(Boolean.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
            case TIME:
            case DATE:
                return ObjectInspectorFactory.getReflectionObjectInspector(TimestampWritableV2.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
            case BYTES:
                return ObjectInspectorFactory.getReflectionObjectInspector(byte[].class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
            default:
                return ObjectInspectorFactory.getReflectionObjectInspector(String.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
        }
    }

}
