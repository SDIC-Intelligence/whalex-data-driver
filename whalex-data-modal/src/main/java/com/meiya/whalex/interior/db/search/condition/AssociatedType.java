package com.meiya.whalex.interior.db.search.condition;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * @author 黄河森
 * @date 2022/2/22
 * @package com.meiya.whalex.interior.db.search.condition
 * @project whalex-data-driver
 */
public enum  AssociatedType {
    LEFT_JOIN("leftJoin"),
    INNER_JOIN("innerJoin"),
    FULL_JOIN("fullJoin")
    ;

    private String type;

    AssociatedType(String type) {
        this.type = type;
    }

    @JsonValue
    public String getType() {
        return type;
    }

    @JsonCreator
    public static AssociatedType parse(String type) {
        if (type == null) {
            return AssociatedType.LEFT_JOIN;
        }
        for (AssociatedType associatedType : AssociatedType.values()) {
            if (associatedType.type.equalsIgnoreCase(type)) {
                return associatedType;
            }
        }
        return AssociatedType.LEFT_JOIN;
    }
}
