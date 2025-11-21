package com.meiya.whalex.graph.enums;

import com.meiya.whalex.graph.entity.InternalQlValue;
import com.meiya.whalex.graph.entity.QlValue;

/**
 * @author 黄河森
 * @date 2024/3/8
 * @package com.meiya.whalex.graph.enums
 * @project whalex-data-driver
 * @description QlTypeConstructor
 */
public enum QlTypeConstructor {

    ANY {
        public boolean covers(QlValue value) {
            return !value.isNull();
        }
    },
    BOOLEAN,
    BYTES,
    STRING,
    NUMBER {
        public boolean covers(QlValue value) {
            QlTypeConstructor valueType = QlTypeConstructor.typeConstructorOf(value);
            return valueType == this || valueType == INTEGER || valueType == FLOAT;
        }
    },
    INTEGER,
    FLOAT,
    LIST,
    MAP {
        public boolean covers(QlValue value) {
            QlTypeConstructor valueType = QlTypeConstructor.typeConstructorOf(value);
            return valueType == MAP || valueType == NODE || valueType == RELATIONSHIP;
        }
    },
    NODE,
    RELATIONSHIP,
    PATH,
    POINT,
    DATE,
    TIME,
    LOCAL_TIME,
    LOCAL_DATE_TIME,
    DATE_TIME,
    DURATION,
    NULL;

    private QlTypeConstructor() {
    }

    private static QlTypeConstructor typeConstructorOf(QlValue value) {
        return ((InternalQlValue)value).typeConstructor();
    }

    public boolean covers(QlValue value) {
        return this == typeConstructorOf(value);
    }
    
}
