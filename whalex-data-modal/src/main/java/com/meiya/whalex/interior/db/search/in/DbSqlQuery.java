package com.meiya.whalex.interior.db.search.in;

import cn.hutool.core.codec.Base64;
import com.meiya.whalex.interior.base.BaseQuery;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;

import javax.validation.constraints.NotBlank;

/**
 * 根据 SQL 查询
 *
 * @author 黄河森
 * @date 2019/9/26
 * @project whale-cloud-platformX
 */
@ApiModel(value = "通过SQL查询数据")
@Data
public class DbSqlQuery extends BaseQuery {

    @NotBlank(message = "sql语句不能为空")
    @ApiModelProperty(value = "执行sql", required = true)
    private String sql;

    @NotBlank(message = "数据库标识符不能为空")
    @ApiModelProperty(value = "要执行的库", required = true)
    private String bigdataResourceId;

    @ApiModelProperty(value = "是否需要统计")
    private boolean countFlag = false;

    @ApiModelProperty(value = "是否通过 base64 加密 SQL")
    private boolean isBase64 = false;


    public String getSql() {
        if (isBase64 && StringUtils.isNotBlank(sql)) {
            return Base64.decodeStr(sql);
        } else {
            return sql;
        }
    }
}
