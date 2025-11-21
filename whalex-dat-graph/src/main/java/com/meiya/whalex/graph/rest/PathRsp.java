package com.meiya.whalex.graph.rest;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

/**
 * @author 黄河森
 * @date 2023/5/4
 * @package com.meiya.whalex.graph.rest
 * @project whalex-data-driver
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PathRsp {

    private String start;
    private String end;
    private List<PathEdge> edgeList;
    private List<PathVertex> vertexList;
    private List<String> pathList;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class PathEdge {
        private String id;
        private String start;
        private String end;
        private String label;
        private List<Map<String, Object>> propertyList;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class PathVertex {
        private String id;
        private String label;
        private List<Map<String, Object>> propertyList;
    }

}
