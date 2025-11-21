package com.meiya.whalex.db.entity;

import lombok.Data;

@Data
public class CreateSequenceBean {

    /**
     * create sequence 序列名
     * [increment by n] --每次增加的值
     * [start with n] --初始值
     * [{maxvalue/minvalue n|nomaxvalue/nominvalue}] --最大/最小值 | 不设最大/最小值
     * [{cycle|nocycle}] --cycle 代表循环， nocycle 代表不循环
     * [{cache n|nocache}] --cache定义存放序列的内存块大小, nocache不对序列进行内存缓冲
     *
     */

    private String sequenceName;

    private Integer incrementBy;

    private Integer startWith;

    private Integer maxValue;

    private boolean noMaxValue;

    private Integer minValue;

    private boolean noMinValue;

    private Boolean cycle; //null 表示不没， false 表示nocycle, true 表示cycle

    private Integer cache;

    private boolean noCache;

}
