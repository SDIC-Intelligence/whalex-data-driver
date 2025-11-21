package com.meiya.whalex.db.entity.ani;

import com.meiya.whalex.db.entity.AbstractDbTableInfo;
import lombok.Data;

/**
 * PostGre 表配置信息
 *
 * @author 黄河森
 * @date 2019/12/23
 * @project whale-cloud-platformX
 */
@Data
public class S3BucketInfo extends AbstractDbTableInfo {
    private String bucketName;
}
