package com.meiya.whalex.db.entity.lucene;

import lombok.Data;

@Data
public class EsRequestParam {

    private String url;
    private String method;
    private String params;

    public EsRequestParam(String url, String method, String params) {
        this.url = url;
        this.method = method;
        this.params = params;
    }

    @Override
    public String toString() {
        return method + " " + url + "\n" + params;
    }
}
