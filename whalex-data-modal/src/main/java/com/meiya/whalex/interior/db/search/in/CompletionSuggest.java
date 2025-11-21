package com.meiya.whalex.interior.db.search.in;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

import java.io.Serializable;

/**
 * @author 黄河森
 * @date 2022/1/12
 * @package com.meiya.whalex.interior.db.search.in
 * @project whalex-data-driver
 */
@ApiModel(value = "completion 建议词查询，仅 ES 支持")
@Data
public class CompletionSuggest implements Serializable {

    @ApiModelProperty("建议别名")
    private String suggestName;

    @ApiModelProperty("建议文本")
    private String text;

    @ApiModelProperty("查询字段名")
    private String field;

    @ApiModelProperty("返回建议数")
    private Integer size;

    @ApiModelProperty("前缀建议文本")
    private String prefix;

    @ApiModelProperty("是否去除重复建议")
    private Boolean skipDuplicate;

    @ApiModelProperty("模糊查询")
    private Fuzzy fuzzyQuery;

    @ApiModelProperty("正则查询")
    private String regex;

    @ApiModel("模糊搜索")
    @Data
    public static class Fuzzy implements Serializable {
        @ApiModelProperty("模糊因子")
        private Integer fuzziness;
        @ApiModelProperty("转换是否作为更改")
        private Boolean transpositions;
        @ApiModelProperty("返回模糊建议之前输入的最小长度")
        private Integer minLength;
        @ApiModelProperty("输入的最小长度")
        private Integer prefixLength;
        @ApiModelProperty("是否以 Unicode 代码点方式进行度量")
        private Boolean unicodeAware;
    }
}
