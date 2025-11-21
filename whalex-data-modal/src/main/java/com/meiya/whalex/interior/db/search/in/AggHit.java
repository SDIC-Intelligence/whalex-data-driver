package com.meiya.whalex.interior.db.search.in;

import lombok.Data;

import java.util.List;

/**
 * @author 黄河森
 * @date 2022/11/1
 * @package com.meiya.whalex.interior.db.search.in
 * @project whalex-data-driver
 */
@Data
public class AggHit {

    private Integer size;

    private List<Order> orders;

    private List<String> select;

}
