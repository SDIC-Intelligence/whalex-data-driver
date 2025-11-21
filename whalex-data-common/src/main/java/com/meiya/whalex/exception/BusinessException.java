package com.meiya.whalex.exception;

/**
 * 云平台自定义业务异常
 *
 * @author 黄河森
 * @date 2019/9/12
 * @project whale-cloud-platformX
 */
public class BusinessException extends RuntimeException {

	private static final long serialVersionUID = -2573747805547242362L;

	/**
	 * 异常状态码
	 */
	private int code;

	public BusinessException() {
		super();
	}

	public BusinessException(String arg0, Throwable arg1) {
		super(arg0, arg1);
	}

	public BusinessException(String arg0) {
		super(arg0);
	}

	public BusinessException(Throwable arg0) {
		super(arg0);
	}

	public BusinessException(int code, String arg0) {
		super(arg0);
		this.code = code;
	}

	public BusinessException(ExceptionCode exceptionCode, String... info) {
		super(info != null ? String.format(exceptionCode.getMsg(), info) : exceptionCode.getMsg());
		this.code = exceptionCode.getCode();
	}

	public BusinessException(Throwable throwable, ExceptionCode exceptionCode, String... info) {
		super(info != null ? String.format(exceptionCode.getMsg(), info) : exceptionCode.getMsg(), throwable);
		this.code = exceptionCode.getCode();
	}

	public int getCode() {
		return code;
	}

	public void setCode(int code) {
		this.code = code;
	}
}
