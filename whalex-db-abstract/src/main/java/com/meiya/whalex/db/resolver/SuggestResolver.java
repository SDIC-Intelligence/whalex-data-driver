package com.meiya.whalex.db.resolver;

import com.meiya.whalex.db.entity.PageResult;
import com.meiya.whalex.exception.BusinessException;
import com.meiya.whalex.util.JsonUtil;
import lombok.Data;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;


/**
 * @author 黄河森
 * @date 2022/1/14
 * @package com.meiya.whalex.db.resolver
 * @project whalex-data-driver
 */

public class SuggestResolver {

    private PageResult pageResult;

    private SuggestResolver(PageResult pageResult) {
        this.pageResult = pageResult;
    }

    public static SuggestResolver resolver(PageResult pageResult) {
        return new SuggestResolver(pageResult);
    }

    /**
     * 术语推荐解析
     *
     * @return
     */
    public Suggest analysisOnTerm() {
        if (!this.pageResult.getSuccess()) {
            throw new BusinessException(this.pageResult.getCode(), this.pageResult.getMessage());
        }
        Suggest suggest = new Suggest();
        Map<String, List<TermSuggest>> termSuggestMap = new HashMap<>();
        suggest.setTermSuggestMap(termSuggestMap);
        List<Map<String, Object>> rows = this.pageResult.getRows();
        Map<String, Object> suggestMap = rows.get(0);
        suggestMap.forEach((k, v) -> {
            List<Map<String, Object>> suggests = (List<Map<String, Object>>) v;
            List<TermSuggest> collect = suggests.stream().flatMap(current -> {
                TermSuggest termSuggest = JsonUtil.jsonStrToObject(JsonUtil.objectToStr(current), TermSuggest.class);
                return Stream.of(termSuggest);
            }).collect(Collectors.toList());
            termSuggestMap.put(k, collect);
        });
        return suggest;
    }

    /**
     * 短语推荐解析
     *
     * @return
     */
    public Suggest analysisOnPhrase() {
        if (!this.pageResult.getSuccess()) {
            throw new BusinessException(this.pageResult.getCode(), this.pageResult.getMessage());
        }
        Suggest suggest = new Suggest();
        Map<String, List<PhraseSuggest>> phraseSuggestMap = new HashMap<>();
        suggest.setPhraseSuggestMap(phraseSuggestMap);
        List<Map<String, Object>> rows = this.pageResult.getRows();
        Map<String, Object> suggestMap = rows.get(0);
        suggestMap.forEach((k, v) -> {
            List<Map<String, Object>> suggests = (List<Map<String, Object>>) v;
            List<PhraseSuggest> collect = suggests.stream().flatMap(current -> {
                PhraseSuggest phraseSuggest = JsonUtil.jsonStrToObject(JsonUtil.objectToStr(current), PhraseSuggest.class);
                return Stream.of(phraseSuggest);
            }).collect(Collectors.toList());
            phraseSuggestMap.put(k, collect);
        });
        return suggest;
    }

    /**
     * 短语推荐解析
     *
     * @return
     */
    public Suggest analysisOnCompletion() {
        if (!this.pageResult.getSuccess()) {
            throw new BusinessException(this.pageResult.getCode(), this.pageResult.getMessage());
        }
        List<Map<String, Object>> rows = this.pageResult.getRows();
        Map<String, Object> suggestMap = rows.get(0);
        Suggest suggest = new Suggest();
        Map<String, List<CompletionSuggest>> completionSuggestMap = new HashMap<>(suggestMap.size());
        suggest.setCompletionSuggestMap(completionSuggestMap);
        suggestMap.forEach((k, v) -> {
            List<Map<String, Object>> suggests = (List<Map<String, Object>>) v;
            List<CompletionSuggest> collect = suggests.stream().flatMap(current -> {
                CompletionSuggest completionSuggest = new CompletionSuggest();
                completionSuggest.setText((String) current.get("text"));
                completionSuggest.setLength((Integer) current.get("length"));
                completionSuggest.setOffset((Integer) current.get("offset"));
                List<Map<String, Object>> optionList = (List<Map<String, Object>>) current.get("options");
                List<CompletionOption> options = optionList.stream().flatMap(option -> {
                    CompletionOption completionOption = new CompletionOption();
                    completionOption.setId((String) option.get("_id"));
                    completionOption.setIndex((String) option.get("_index"));
                    completionOption.setScore((Double) option.get("_score"));
                    completionOption.setText((String) option.get("text"));
                    Map<String, Object> source = (Map<String, Object>) option.get("_source");
                    CompletionSource completionSource = new CompletionSource();
                    completionOption.setSource(completionSource);
                    completionSource.setTitle((String) source.get("title"));
                    completionSource.setBody((String) source.get("body"));
                    return Stream.of(completionOption);
                }).collect(Collectors.toList());
                completionSuggest.setOptions(options);
                return Stream.of(completionSuggest);
            }).collect(Collectors.toList());
            completionSuggestMap.put(k, collect);
        });
        return suggest;
    }

    @Data
    public static class Suggest {
        private Map<String, List<TermSuggest>> termSuggestMap;
        private Map<String, List<PhraseSuggest>> phraseSuggestMap;
        private Map<String, List<CompletionSuggest>> completionSuggestMap;
    }

    @Data
    public static class TermSuggest {
        private String text;
        private Integer offset;
        private Integer length;
        private List<Option> options;
    }

    @Data
    public static class Option {
        private String text;
        private Double score;
        private Double freq;
    }

    @Data
    public static class PhraseSuggest {
        private String text;
        private Integer offset;
        private Integer length;
        private List<Option> options;
    }

    @Data
    public static class CompletionSuggest {
        private String text;
        private Integer offset;
        private Integer length;
        private List<CompletionOption> options;
    }

    @Data
    public static class CompletionOption {
        private String text;
        private String index;
        private String id;
        private Double score;
        private CompletionSource source;
    }

    @Data
    public static class CompletionSource {
        private String title;
        private String body;
    }
}
