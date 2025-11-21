package com.meiya.whalex.db.entity.graph;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * @author chenjp
 * @date 2/25/2021
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class GraphHas implements Serializable {

    private static final long serialVersionUID = 4566995686965908502L;
    private String label;
    private String key;
    private String value;
}
