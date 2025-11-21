package com.meiya.whalex.graph.entity;

/**
 * @author 黄河森
 * @date 2024/3/8
 * @package com.meiya.whalex.graph.entity
 * @project whalex-data-driver
 * @description QlBooleanValue
 */
public abstract class QlBooleanValue extends QlValueAdapter {

    public static QlBooleanValue TRUE = new QlTrueValue();
    public static QlBooleanValue FALSE = new QlFalseValue();

    private QlBooleanValue() {
    }

    public static QlBooleanValue fromBoolean(boolean value) {
        return value ? TRUE : FALSE;
    }

    public abstract Boolean asObject();

    public QlType type() {
        return InternalQlTypeSystem.TYPE_SYSTEM.BOOLEAN();
    }

    public int hashCode() {
        Boolean value = this.asBoolean() ? Boolean.TRUE : Boolean.FALSE;
        return value.hashCode();
    }

    private static class QlFalseValue extends QlBooleanValue {
        private QlFalseValue() {
            super();
        }

        public Boolean asObject() {
            return Boolean.FALSE;
        }

        public boolean asBoolean() {
            return false;
        }

        public boolean isTrue() {
            return false;
        }

        public boolean isFalse() {
            return true;
        }

        public boolean equals(Object obj) {
            return obj == FALSE;
        }

        public String toString() {
            return "FALSE";
        }
    }

    private static class QlTrueValue extends QlBooleanValue {
        private QlTrueValue() {
            super();
        }

        public Boolean asObject() {
            return Boolean.TRUE;
        }

        public boolean asBoolean() {
            return true;
        }

        public boolean isTrue() {
            return true;
        }

        public boolean isFalse() {
            return false;
        }

        public boolean equals(Object obj) {
            return obj == TRUE;
        }

        public String toString() {
            return "TRUE";
        }
    }

}
