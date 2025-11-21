package com.meiya.whalex.interior.db.search.in;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
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
@ApiModel(value = "term 建议词查询，仅 ES 支持")
@Data
public class TermSuggest implements Serializable {

    @ApiModelProperty(value = "建议别名")
    private String suggestName;

    @ApiModelProperty(value = "建议文件")
    private String text;

    @ApiModelProperty(value = "从中获取建议的字段")
    private String field;

    @ApiModelProperty(value = "每个建议文本标注的返回的最大更正数量")
    private Integer size;

    @ApiModelProperty(value = "建议文本排序方式")
    private Sort sort;

    @ApiModelProperty(value = "建议模式")
    private Mode mode;

    @ApiModel("建议排序方式")
    public enum Sort implements Serializable {
        @ApiModelProperty(value = "按匹配分数值排序")
        SCORE("score"),
        @ApiModelProperty(value = "按出现频率排序")
        FREQUENCY("frequency"),
        ;
        private String name;

        Sort(String name) {
            this.name = name;
        }

        @JsonValue
        public String getName() {
            return name;
        }

        @JsonCreator
        public static Sort parse(String name) {
            if (name == null) {
                return Sort.SCORE;
            }
            for (Sort sort : Sort.values()) {
                if (sort.name.equalsIgnoreCase(name)) {
                    return sort;
                }
            }
            return Sort.SCORE;
        }

        @Override
        public String toString() {
            return getName();
        }
    }

    @ApiModel("建议模式")
    public enum Mode implements Serializable {
        @ApiModelProperty(value = "只建议出现在比原始建议文本术语更多的文档中的建议")
        POPULAR("popular"),
        @ApiModelProperty(value = "仅对索引中没有的建议文本术语提供建议")
        MISSING("missing"),
        @ApiModelProperty(value = "根据建议文本中的术语建议任何匹配的建议")
        ALWAYS("always")
        ;
        private String name;

        Mode(String name) {
            this.name = name;
        }

        @JsonValue
        public String getName() {
            return name;
        }

        @JsonCreator
        public static Mode parse(String name) {
            if (name == null) {
                return Mode.POPULAR;
            }
            for (Mode mode : Mode.values()) {
                if (mode.name.equalsIgnoreCase(name)) {
                    return mode;
                }
            }
            return Mode.POPULAR;
        }

        @Override
        public String toString() {
            return getName();
        }
    }
}
