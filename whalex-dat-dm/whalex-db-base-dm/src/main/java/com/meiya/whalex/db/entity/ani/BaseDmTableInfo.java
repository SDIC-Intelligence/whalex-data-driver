package com.meiya.whalex.db.entity.ani;

import com.meiya.whalex.db.entity.AbstractDbTableInfo;
import lombok.Data;

/**
 * Oracle 表配置信息
 *
 * @author 蔡荣桂
 * @date 2021/4/14
 * @project whale-cloud-platformX
 */
@Data
public class BaseDmTableInfo extends AbstractDbTableInfo {

    private boolean ignoreCase = Boolean.TRUE;

}
