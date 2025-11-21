package com.meiya.whalex.interior.db.search.out;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

/**
 * 数据字段类型实体
 *
 * @author Huanghesen
 * @date 2018/10/10
 * @project whale-common-web
 * @package com.meiya.whale.resource.db.condition
 */
@ApiModel(value = "数据字段参数")
public class FieldEntity {

    @ApiModelProperty(value = "字段名", required = true)
    private String field;

    @ApiModelProperty(value = "字段值", required = true)
    private Object value;

    @ApiModelProperty(value = "字段值的类型", notes = "HBase插入是可将数据base64编码")
    private String fieldType;

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public String getFieldType() {
        return fieldType;
    }

    public void setFieldType(String fieldType) {
        this.fieldType = fieldType;
    }
}
