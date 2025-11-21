package com.meiya.whalex.keyword;

public interface KeyWordHandler {

    boolean isKeyWord(String field);

    String handler(String field);

}
