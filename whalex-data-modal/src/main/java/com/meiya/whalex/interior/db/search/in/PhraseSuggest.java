package com.meiya.whalex.interior.db.search.in;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

import java.io.Serializable;
import java.util.List;

/**
 * @author 黄河森
 * @date 2022/1/12
 * @package com.meiya.whalex.interior.db.search.in
 * @project whalex-data-driver
 */
@ApiModel(value = "phrase 建议词查询，仅 ES 支持")
@Data
public class PhraseSuggest implements Serializable {

    @ApiModelProperty(value = "建议别名")
    private String suggestName;

    @ApiModelProperty(value = "建议文本")
    private String text;

    @ApiModelProperty(value = "字段名", notes = "作为基于语言模式的连词查找字段，这个推荐器将会使用这个字段去获取修正分数的统计数据")
    private String field;

    @ApiModelProperty(value = "最大连词数", notes = "设置在 field 字段中连词的最大数值")
    private Integer gramSize;

    @ApiModelProperty(value = "直接生成器配置")
    private List<DirectGenerator> directGenerators;

    @Data
    @ApiModel(value = "直接生成器")
    public static class DirectGenerator implements Serializable {
        @ApiModelProperty(value = "从中获取候选建议的字段")
        private String field;
        @ApiModelProperty(value = "建议模式配置")
        private Mode mode;
    }

    @ApiModel(value = "建议模式")
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
