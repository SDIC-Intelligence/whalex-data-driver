package com.meiya.whalex.db.util.param.impl.graph;

import lombok.Data;

import java.util.List;

/**
 * @author 黄河森
 * @date 2023/3/27
 * @package com.meiya.whalex.db.util.param.impl.graph
 * @project whalex-data-driver
 */
public interface ReturnType {



/*    PATH,
    VALUE_MAP,
    VALUE_MAP_ALL,
    VALUES,
    VALUES_ALL,
    V,
    E,
    LABEL,
    PROPERTIES,
    PROPERTIES_ALL,
    BOTH_V,
    COUNT*/

    @Data
    class PATH implements ReturnType {

        /**
         * 填充 V 或者 E，根据顺序获取对应的记录返回
         */
        private List<ReturnType> returnType;

        public static PATH getReturnType(List<ReturnType> returnType) {
            return new PATH(returnType);
        }

        public PATH(List<ReturnType> returnType) {
            this.returnType = returnType;
        }
    }

    class VALUE_MAP implements ReturnType {
        private static final VALUE_MAP INSTANCE = new VALUE_MAP();

        public static VALUE_MAP getReturnType() {
            return INSTANCE;
        }
    }

    class VALUE_MAP_ALL implements ReturnType {
        private static final VALUE_MAP_ALL INSTANCE = new VALUE_MAP_ALL();

        public static VALUE_MAP_ALL getReturnType() {
            return INSTANCE;
        }
    }

    class VALUES implements ReturnType {
        private static final VALUES INSTANCE = new VALUES();

        public static VALUES getReturnType() {
            return INSTANCE;
        }
    }

    class VALUES_ALL implements ReturnType {
        private static final VALUES_ALL INSTANCE = new VALUES_ALL();

        public static VALUES_ALL getReturnType() {
            return INSTANCE;
        }
    }

    class V implements ReturnType {
        private static final V INSTANCE = new V();

        public static V getReturnType() {
            return INSTANCE;
        }
    }

    class E implements ReturnType {
        private static final E INSTANCE = new E();

        public static E getReturnType() {
            return INSTANCE;
        }
    }

    class LABEL implements ReturnType {
        private static final LABEL INSTANCE = new LABEL();

        public static LABEL getReturnType() {
            return INSTANCE;
        }
    }

    class PROPERTIES implements ReturnType {
        private static final PROPERTIES INSTANCE = new PROPERTIES();

        public static PROPERTIES getReturnType() {
            return INSTANCE;
        }
    }

    class PROPERTIES_ALL implements ReturnType {
        private static final PROPERTIES_ALL INSTANCE = new PROPERTIES_ALL();

        public static PROPERTIES_ALL getReturnType() {
            return INSTANCE;
        }
    }

    class BOTH_V implements ReturnType {
        private static final BOTH_V INSTANCE = new BOTH_V();

        public static BOTH_V getReturnType() {
            return INSTANCE;
        }
    }

    class COUNT implements ReturnType {
        private static final COUNT INSTANCE = new COUNT();

        public static COUNT getReturnType() {
            return INSTANCE;
        }
    }
}
