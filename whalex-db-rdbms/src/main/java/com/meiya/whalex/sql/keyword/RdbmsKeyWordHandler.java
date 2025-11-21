package com.meiya.whalex.sql.keyword;

import com.meiya.whalex.keyword.KeyWordHandler;
import org.apache.calcite.sql.parser.impl.SqlParserImplConstants;

import java.util.HashSet;
import java.util.Set;

public abstract class RdbmsKeyWordHandler implements KeyWordHandler {

    public static Set<String> keywordPool;

    {
        keywordPool  = new HashSet<>();

        for (String keyword : SqlParserImplConstants.tokenImage) {
            keywordPool.add(keyword.replaceAll("\"", ""));
        }

        keywordPool.remove("*");

        keywordPool.add("COMMENT");
        keywordPool.add("RENAME");
        keywordPool.add("LOAD");
    }

}
