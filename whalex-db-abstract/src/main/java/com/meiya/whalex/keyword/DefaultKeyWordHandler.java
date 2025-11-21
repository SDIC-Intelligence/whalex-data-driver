package com.meiya.whalex.keyword;

public class DefaultKeyWordHandler implements KeyWordHandler{

    @Override
    public boolean isKeyWord(String field) {
        return false;
    }

    @Override
    public String handler(String field) {
        return field;
    }
}
