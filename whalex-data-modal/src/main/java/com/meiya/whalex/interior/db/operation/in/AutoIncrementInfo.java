package com.meiya.whalex.interior.db.operation.in;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author 黄河森
 * @date 2023/6/12
 * @package com.meiya.whalex.interior.db.operation.in
 * @project whalex-data-driver
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class AutoIncrementInfo {

    private boolean autoIncrement;

    private Long startIncrement = 1L;

    private Long incrementBy = 1L;

    public AutoIncrementInfo(boolean autoIncrement) {
        this.autoIncrement = autoIncrement;
    }
}
