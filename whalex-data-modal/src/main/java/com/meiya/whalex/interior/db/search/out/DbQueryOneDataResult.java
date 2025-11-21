package com.meiya.whalex.interior.db.search.out;

import com.meiya.whalex.interior.base.BaseResult;
import com.meiya.whalex.interior.db.constant.ReturnCodeEnum;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import java.util.ArrayList;
import java.util.List;

/**
 * API 接口数据查询统一返回实体
 *
 * @author Huanghesen
 * @date 2018/9/10
 * @project whale-cloud-api
 */
@ApiModel(value = "单记录数据查询接口返参实体")
public class DbQueryOneDataResult extends BaseResult {

    @ApiModelProperty(value = "数据字段结果集")
    private List<DbQueryDataResult.FieldEntity> data;

    public List<DbQueryDataResult.FieldEntity> getData() {
        return data;
    }

    public void setData(List<DbQueryDataResult.FieldEntity> data) {
        this.data = data;
    }

    public void addResult(DbQueryDataResult.FieldEntity fieldEntity) {
        if (this.data == null) {
            this.data = new ArrayList<>();
        }
        this.data.add(fieldEntity);
    }

    public void addResultList(List<DbQueryDataResult.FieldEntity> fieldEntityList) {
        if (this.data == null) {
            this.data = new ArrayList<>();
        }
        this.data.addAll(fieldEntityList);
    }

    public DbQueryOneDataResult() {
    }

    public DbQueryOneDataResult(int returnCode, String errorMsg) {
        super(returnCode, errorMsg);
    }

    public DbQueryOneDataResult(ReturnCodeEnum returnCodeEnum) {
        super(returnCodeEnum);
    }
}
