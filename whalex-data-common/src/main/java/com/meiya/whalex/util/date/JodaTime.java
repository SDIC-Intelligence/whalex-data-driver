/**
 * Copyright©2013 Xiamen Meiah Pico IT CO., Ltd. 
 * All rights reserved.
 */
package com.meiya.whalex.util.date;

import org.joda.time.DurationFieldType;

/**
 * jodaTime枚举类似类<br/>
 *
 * @author weic
 * @version 1.0 13-4-26 下午5:08
 */
public class JodaTime {

    /**
     * 时代(1000年)
     */
    public static DurationFieldType ERA = DurationFieldType.eras();
    /**
     * 世纪(100年)
     */
    public static DurationFieldType CENTURY = DurationFieldType.centuries();
    /**
     * 一年中的第几周
     */
    public static DurationFieldType WEEK_YEAR = DurationFieldType.weekyears();
    /**
     * 年
     */
    public static DurationFieldType YEAR = DurationFieldType.years();
    /**
     * 月
     */
    public static DurationFieldType MONTH = DurationFieldType.months();
    /**
     * 日
     */
    public static DurationFieldType DAY = DurationFieldType.days();
    /**
     * 周
     */
    public static DurationFieldType WEEK = DurationFieldType.weeks();
    /**
     * 半天
     */
    public static DurationFieldType HALF_DAY = DurationFieldType.halfdays();
    /**
     * 时
     */
    public static DurationFieldType HOUR = DurationFieldType.hours();
    /**
     * 分
     */
    public static DurationFieldType MINUTE = DurationFieldType.minutes();
    /**
     * 秒
     */
    public static DurationFieldType SECOND = DurationFieldType.seconds();
    /**
     * 毫秒
     */
    public static DurationFieldType MILLI = DurationFieldType.millis();
}
