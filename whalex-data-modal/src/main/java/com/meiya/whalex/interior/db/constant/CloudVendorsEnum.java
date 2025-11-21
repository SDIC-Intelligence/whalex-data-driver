package com.meiya.whalex.interior.db.constant;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * 云厂商代码
 *
 * @author 黄河森
 * @date 2021/5/7
 * @project whalex-data-driver-ark-plugin
 */
public enum CloudVendorsEnum {

    OPEN("开源组件", "01"),
    KingBase("人大金仓", "07"),
    H3C("新华三", "08"),
    HighGo("瀚高", "10"),
    /**
     * 未定义
     */
    undefine("未定义", "00")
    ;

    CloudVendorsEnum(String name, String code) {
        this.name = name;
        this.code = code;
    }

    private String name;

    private String code;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @JsonValue
    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    @JsonCreator
    public static CloudVendorsEnum parse(String code) {
        if (code == null) {
            return CloudVendorsEnum.undefine;
        }
        for (CloudVendorsEnum cloudVendorsEnum : CloudVendorsEnum.values()) {
            if (cloudVendorsEnum.code.equalsIgnoreCase(code)) {
                return cloudVendorsEnum;
            }
        }
        return CloudVendorsEnum.undefine;
    }
}
