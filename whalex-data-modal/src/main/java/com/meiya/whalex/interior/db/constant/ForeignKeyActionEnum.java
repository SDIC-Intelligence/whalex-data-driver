package com.meiya.whalex.interior.db.constant;

public enum ForeignKeyActionEnum {

    CASCADE("CASCADE"),
    NO_ACTION("NO ACTION"),
    RESTRICT("RESTRICT"),
    SET_NULL("SET NULL");

    public String val;

    ForeignKeyActionEnum(String val) {
        this.val = val;
    }

}
