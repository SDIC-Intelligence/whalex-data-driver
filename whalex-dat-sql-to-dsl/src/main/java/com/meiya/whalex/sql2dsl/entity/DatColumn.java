package com.meiya.whalex.sql2dsl.entity;

public class DatColumn {

    public String name;
    public String dataType;
    public Integer length;
    public Integer decimalPointLength;
    public Object defaultValue;
    public boolean nullable = true;
    public boolean removeDefault;
    public boolean dropNotNull;
    public boolean isAlterDropFlag;
    public String newName;
    public boolean partition = false;
    public boolean primaryKey;
    public boolean autoIncrement;
    public boolean distributed;
    public boolean unsigned = false;
    public int initAutoIncrementValue = 1;
    public Object onUpdate;
    public String comment;
    
    
}
