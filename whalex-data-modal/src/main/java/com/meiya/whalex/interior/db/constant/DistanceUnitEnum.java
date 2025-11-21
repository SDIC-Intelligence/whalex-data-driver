package com.meiya.whalex.interior.db.constant;

/**
 * 距离单位枚举
 *
 * @author 黄河森
 * @date 2021/6/18
 * @project whalex-data-driver
 */
public enum  DistanceUnitEnum {

    KILOMETERS("km"),
    METERS("m"),
    CENTIMETERS("cm"),
    MILLIMETERS("mm")
    ;

    private String unit;

    DistanceUnitEnum(String unit) {
        this.unit = unit;
    }

    public String getUnit() {
        return unit;
    }

    // 千米转化成米
    public static String kmToM(Object str) {
        String s = str.toString().toLowerCase();
        if(s.contains(KILOMETERS.unit)) {
            double d = Double.parseDouble(s.substring(0, s.length() - 2));
            return d * 1000 + "";
        }else if(s.contains(METERS.unit)) {
            return s.substring(0, s.length() - 1);
        }
        return s;
    }

    // 米转化成千米
    public static String mToKm(Object str) {
        String s = str.toString().toLowerCase();
        if(s.contains(KILOMETERS.unit)) {
            return (s.substring(0, s.length() - 2));
        }else if(s.contains(METERS.unit)) {
            return Double.parseDouble(s.substring(0, s.length() - 1)) / 1000 + "";
        }
        return Double.parseDouble(s) / 1000 + "";
    }

//    public static void main(String[] args) {
//        System.out.println(mToKm("1000"));
//    }
}
