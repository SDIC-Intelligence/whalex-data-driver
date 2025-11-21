package com.meiya.whalex.interior.db.search.in;

import io.swagger.annotations.ApiModel;
import lombok.Data;

import java.io.Serializable;
import java.util.List;

/**
 * 建议词查询
 *
 * @author 黄河森
 * @date 2022/1/12
 * @package com.meiya.whalex.interior.db.search.in
 * @project whalex-data-driver
 */
@ApiModel(value = "建议词查询语法，仅 ES 支持")
@Data
public class Suggest implements Serializable {

    private List<TermSuggest> termSuggests;

    private List<PhraseSuggest> phraseSuggests;

    private List<CompletionSuggest> completionSuggests;

}
