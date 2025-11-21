package com.meiya.whalex.interior.db.builder;

import com.meiya.whalex.interior.db.search.in.CompletionSuggest;
import com.meiya.whalex.interior.db.search.in.PhraseSuggest;
import com.meiya.whalex.interior.db.search.in.Suggest;
import com.meiya.whalex.interior.db.search.in.TermSuggest;

import java.util.ArrayList;
import java.util.List;

/**
 * @author 黄河森
 * @date 2022/1/13
 * @package com.meiya.whalex.interior.db.builder
 * @project whalex-data-driver
 */
public class SuggestBuilder {

    private Suggest suggest;

    private SuggestBuilder() {
        this.suggest = new Suggest();
    }

    public static SuggestBuilder builder() {
        return new SuggestBuilder();
    }

    public Suggest build() {
        return this.suggest;
    }

    /**
     * 术语推荐
     *
     * @param suggestName
     * @return
     */
    public TermBuilder term(String suggestName) {
        TermSuggest termSuggest = new TermSuggest();
        termSuggest.setSuggestName(suggestName);
        List<TermSuggest> termSuggests = this.suggest.getTermSuggests();
        if (termSuggests == null) {
            termSuggests = new ArrayList<>();
            this.suggest.setTermSuggests(termSuggests);
        }
        termSuggests.add(termSuggest);
        return TermBuilder.builder(termSuggest, this);
    }

    /**
     * 短语推荐
     * @param suggestName
     * @return
     */
    public PhraseBuilder phrase(String suggestName) {
        PhraseSuggest phraseSuggest = new PhraseSuggest();
        phraseSuggest.setSuggestName(suggestName);
        List<PhraseSuggest> phraseSuggests = this.suggest.getPhraseSuggests();
        if (phraseSuggests == null) {
            phraseSuggests = new ArrayList<>();
            this.suggest.setPhraseSuggests(phraseSuggests);
        }
        phraseSuggests.add(phraseSuggest);
        return PhraseBuilder.builder(phraseSuggest, this);
    }

    public CompletionBuilder completion(String suggestName) {
        CompletionSuggest completionSuggest = new CompletionSuggest();
        completionSuggest.setSuggestName(suggestName);
        List<CompletionSuggest> completionSuggests = this.suggest.getCompletionSuggests();
        if (completionSuggests == null) {
            completionSuggests = new ArrayList<>();
            this.suggest.setCompletionSuggests(completionSuggests);
        }
        completionSuggests.add(completionSuggest);
        return CompletionBuilder.builder(completionSuggest, this);
    }

    /**
     * 术语构造器
     */
    public static class TermBuilder {
        private TermSuggest termSuggest;
        private SuggestBuilder suggestBuilder;

        private TermBuilder(TermSuggest termSuggest, SuggestBuilder suggestBuilder) {
            this.termSuggest = termSuggest;
            this.suggestBuilder = suggestBuilder;
        }

        public static TermBuilder builder(TermSuggest termSuggest, SuggestBuilder suggestBuilder) {
            return new TermBuilder(termSuggest, suggestBuilder);
        }

        public SuggestBuilder build() {
            return this.suggestBuilder;
        }

        public TermBuilder text(String text) {
            this.termSuggest.setText(text);
            return this;
        }

        public TermBuilder field(String field) {
            this.termSuggest.setField(field);
            return this;
        }

        public TermBuilder size(Integer size) {
            this.termSuggest.setSize(size);
            return this;
        }

        public TermBuilder sort(TermSuggest.Sort sort) {
            this.termSuggest.setSort(sort);
            return this;
        }

        public TermBuilder mode(TermSuggest.Mode mode) {
            this.termSuggest.setMode(mode);
            return this;
        }
    }

    /**
     * 短语构造器
     */
    public static class PhraseBuilder {
        private PhraseSuggest phraseSuggest;
        private SuggestBuilder suggestBuilder;

        private PhraseBuilder(PhraseSuggest phraseSuggest, SuggestBuilder suggestBuilder) {
            this.phraseSuggest = phraseSuggest;
            this.suggestBuilder = suggestBuilder;
        }

        public static PhraseBuilder builder(PhraseSuggest phraseSuggest, SuggestBuilder suggestBuilder) {
            return new PhraseBuilder(phraseSuggest, suggestBuilder);
        }

        public PhraseBuilder text(String text) {
            this.phraseSuggest.setText(text);
            return this;
        }

        public PhraseBuilder field(String field) {
            this.phraseSuggest.setField(field);
            return this;
        }

        public PhraseBuilder gramSize(Integer gramSize) {
            this.phraseSuggest.setGramSize(gramSize);
            return this;
        }

        public DirectGeneratorBuilder directGenerator() {
            PhraseSuggest.DirectGenerator directGenerator = new PhraseSuggest.DirectGenerator();
            List<PhraseSuggest.DirectGenerator> directGenerators = this.phraseSuggest.getDirectGenerators();
            if (directGenerators == null) {
                directGenerators = new ArrayList<>();
                this.phraseSuggest.setDirectGenerators(directGenerators);
            }
            directGenerators.add(directGenerator);
            return DirectGeneratorBuilder.builder(directGenerator, this);
        }

        public SuggestBuilder build() {
            return this.suggestBuilder;
        }
    }

    /**
     * 短语推荐生成器配置
     */
    public static class DirectGeneratorBuilder {
        private PhraseSuggest.DirectGenerator directGenerator;
        private PhraseBuilder phraseBuilder;

        private DirectGeneratorBuilder(PhraseSuggest.DirectGenerator directGenerator, PhraseBuilder phraseBuilder) {
            this.directGenerator = directGenerator;
            this.phraseBuilder = phraseBuilder;
        }

        public static DirectGeneratorBuilder builder(PhraseSuggest.DirectGenerator directGenerator, PhraseBuilder phraseBuilder) {
            return new DirectGeneratorBuilder(directGenerator, phraseBuilder);
        }

        public DirectGeneratorBuilder field(String field) {
            this.directGenerator.setField(field);
            return this;
        }

        public DirectGeneratorBuilder mode(PhraseSuggest.Mode mode) {
            this.directGenerator.setMode(mode);
            return this;
        }

        public PhraseBuilder returned() {
            return this.phraseBuilder;
        }
    }

    /**
     * 自动补全构建
     */
    public static class CompletionBuilder {
        private CompletionSuggest completionSuggest;
        private SuggestBuilder suggestBuilder;

        private CompletionBuilder(CompletionSuggest completionSuggest, SuggestBuilder suggestBuilder) {
            this.completionSuggest = completionSuggest;
            this.suggestBuilder = suggestBuilder;
        }

        public static CompletionBuilder builder(CompletionSuggest completionSuggest, SuggestBuilder suggestBuilder) {
            return new CompletionBuilder(completionSuggest, suggestBuilder);
        }

        public SuggestBuilder build() {
            return this.suggestBuilder;
        }

        public CompletionBuilder text(String text) {
            this.completionSuggest.setText(text);
            return this;
        }

        public CompletionBuilder field(String field) {
            this.completionSuggest.setField(field);
            return this;
        }

        public CompletionBuilder size(Integer size) {
            this.completionSuggest.setSize(size);
            return this;
        }

        public CompletionBuilder prefix(String prefix) {
            this.completionSuggest.setPrefix(prefix);
            return this;
        }

        public CompletionBuilder regex(String regex) {
            this.completionSuggest.setRegex(regex);
            return this;
        }

        public CompletionBuilder skipDuplicate(Boolean skipDuplicate) {
            this.completionSuggest.setSkipDuplicate(skipDuplicate);
            return this;
        }

        public FuzzyBuilder fuzzy() {
            CompletionSuggest.Fuzzy fuzzy = new CompletionSuggest.Fuzzy();
            this.completionSuggest.setFuzzyQuery(fuzzy);
            return FuzzyBuilder.builder(this, fuzzy);
        }
    }

    /**
     * 模糊查询
     */
    public static class FuzzyBuilder {
        private CompletionBuilder completionBuilder;
        private CompletionSuggest.Fuzzy fuzzy;

        private FuzzyBuilder(CompletionBuilder completionBuilder, CompletionSuggest.Fuzzy fuzzy) {
            this.completionBuilder = completionBuilder;
            this.fuzzy = fuzzy;
        }

        public static FuzzyBuilder builder(CompletionBuilder completionBuilder, CompletionSuggest.Fuzzy fuzzy) {
            return new FuzzyBuilder(completionBuilder, fuzzy);
        }

        public CompletionBuilder returned() {
            return this.completionBuilder;
        }

        public FuzzyBuilder fuzziness(Integer fuzziness) {
            this.fuzzy.setFuzziness(fuzziness);
            return this;
        }

        public FuzzyBuilder transpositions(Boolean transpositions) {
            this.fuzzy.setTranspositions(transpositions);
            return this;
        }

        public FuzzyBuilder minLength(Integer minLength) {
            this.fuzzy.setMinLength(minLength);
            return this;
        }

        public FuzzyBuilder prefixLength(Integer prefixLength) {
            this.fuzzy.setPrefixLength(prefixLength);
            return this;
        }

        public FuzzyBuilder unicodeAware(Boolean unicodeAware) {
            this.fuzzy.setUnicodeAware(unicodeAware);
            return this;
        }
    }

}
