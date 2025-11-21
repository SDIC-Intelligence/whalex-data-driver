package com.meiya.whalex.interior.base;

import com.meiya.whalex.interior.db.constant.ReturnCodeEnum;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

/**
 * 基础返回类
 *
 * @author 黄河森
 * @date 2019/9/26
 * @project whale-cloud-platformX
 */
@ApiModel(value = "基础返回报文")
public class BaseResult {

    @ApiModelProperty(value = "接口请求状态")
    private Boolean success = true;

    @ApiModelProperty(value = "状态码")
    private int returnCode;

    @ApiModelProperty(value = "异常信息")
    private String errorMsg;

    @ApiModelProperty(value = "兼容以前版本的接口参数-异常信息", hidden = true)
    private String message;

    @ApiModelProperty(value = "兼容以前版本的接口参数-状态码", hidden = true)
    private int code;

    public int getReturnCode() {
        return returnCode;
    }

    public void setReturnCode(int returnCode) {
        if (returnCode != 0) {
            success = false;
        }
        this.returnCode = returnCode;
        this.code = returnCode;
    }

    public int getCode() {
        return code;
    }

    public String getErrorMsg() {
        return errorMsg;
    }

    public void setErrorMsg(String errorMsg) {
        this.errorMsg = errorMsg;
        this.message = errorMsg;
    }

    public String getMessage() {
        return message;
    }

    public Boolean getSuccess() {
        return success;
    }

    public void setSuccess(Boolean success) {
        this.success = success;
    }

    public BaseResult() {
        success = true;
        this.returnCode = ReturnCodeEnum.CODE_SUCCESS.getCode();
        this.code = ReturnCodeEnum.CODE_SUCCESS.getCode();
        this.errorMsg = ReturnCodeEnum.CODE_SUCCESS.getMsg();
        this.message = ReturnCodeEnum.CODE_SUCCESS.getMsg();
    }

    public BaseResult(int returnCode, String errorMsg) {
        if (returnCode != 0) {
            success = false;
        }
        this.returnCode = returnCode;
        this.code = returnCode;
        this.errorMsg = errorMsg;
        this.message = errorMsg;
    }

    public BaseResult(ReturnCodeEnum returnCodeEnum) {
        if (returnCodeEnum.getCode() != 0) {
            success = false;
        }
        this.returnCode = returnCodeEnum.getCode();
        this.code = returnCodeEnum.getCode();
        this.errorMsg = returnCodeEnum.getMsg();
        this.message = returnCodeEnum.getMsg();
    }
}
