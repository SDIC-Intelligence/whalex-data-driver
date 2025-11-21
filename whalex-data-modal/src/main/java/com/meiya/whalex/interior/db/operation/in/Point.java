package com.meiya.whalex.interior.db.operation.in;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Builder;
import lombok.Data;

/**
 * 数据驱动抽象层坐标类
 *
 * @author 黄河森
 * @date 2020/6/16
 * @project whalex-data-driver
 */
@Data
@Builder
@ApiModel("坐标类")
public class Point {

    @ApiModelProperty("经度")
    private Double lon;

    @ApiModelProperty("维度")
    private Double lat;

    public Point() {
    }

    public Point(Double lon, Double lat) {
        this.lon = lon;
        this.lat = lat;
    }
}
