package com.meiya.whalex.util.encrypt;

import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

/**
 * @ClassName: AESUtil
 * @Description: AES加解密算法
 * @author Smart
 * @date 2013-09-04 21:56
 * 
 */
public class AESUtil {
	static Logger logger = LoggerFactory.getLogger(AESUtil.class);
	// 密钥(不能随便乱改~)
	// 2cc2251608644bb6c4f6e0465b8300cd
	// 2cc2251608644bb6
	private final static String keyString = "2cc2251608644bb6c4f6e0465b8300cd";

	// 解密密码器
	private static Cipher decipher;

	// 加密密码器
	private static Cipher encipher;
	static {
		try {
			byte[] keyBytes = HexString2Bytes(keyString);
			SecretKeySpec key = new SecretKeySpec(keyBytes, "AES");
			encipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
			encipher.init(Cipher.ENCRYPT_MODE, key);
			decipher = Cipher.getInstance("AES/ECB/PKCS5Padding");// 创建密码器
			decipher.init(Cipher.DECRYPT_MODE, key);// 初始化
		} catch (Exception e) {
			logger.error("AES加解密工具初始化失败", e);
		}
	}

	/**
	 * 加密
	 * 
	 * @param content
	 *            需要加密的内容
	 * @return
	 */
	public static byte[] encrypt(byte[] content) {
		try {
			byte[] result = encipher.doFinal(content);
			return result; // 加密
		} catch (Exception e) {
			logger.error("AES加密失败, error: {}", e.getMessage());
		}
		return null;
	}

	/**
	 * 加密
	 * 
	 * @param content
	 * @return String
	 * @exception
	 * @since 1.0.0
	 */
	public static String encrypt(String content) {
		try {
			byte[] result = encipher.doFinal(content.getBytes());
			byte[] byteBase64 = Base64.encodeBase64(result);
			return new String(byteBase64);
		} catch (Exception e) {
			logger.error("AES加密失败, error: {}", e.getMessage());
		}
		return null;
	}

	/**
	 * 加密
	 * 
	 * @param content
	 * @param charSet
	 * @return String
	 * @exception
	 * @since 1.0.0
	 */
	public static String encrypt(String content, String charSet) {
		try {
			byte[] result = encipher.doFinal(content.getBytes(charSet));
			byte[] byteBase64 = Base64.encodeBase64(result);
			return new String(byteBase64);
		} catch (Exception e) {
			logger.error("AES加密失败, error: {}", e.getMessage());
		}
		return null;
	}

	/**
	 * 解密
	 * 
	 * @param content
	 *            待解密内容
	 * @return
	 */
	public static byte[] decrypt(byte[] content) {
		try {
			byte[] result = decipher.doFinal(content);
			return result; // 加密
		} catch (Exception e) {
			logger.error("AES解密失败, error: {}", e.getMessage());
		}
		return content;
	}

	/**
	 * 解密
	 * 
	 * @param content
	 * @return String
	 * @exception
	 * @since 1.0.0
	 */
	public static String decrypt(String content) {
		try {
			byte[] deContent = Base64.decodeBase64(content.getBytes());
			String result = new String(decipher.doFinal(deContent));
			return result; // 加密
		} catch (Exception e) {
//			logger.error("AES解密失败, error: {}", e.getMessage());
		}
		return content;
	}

	/**
	 * 解密
	 * 
	 * @param content
	 * @param charSet
	 * @return String
	 * @exception
	 * @since 1.0.0
	 */
	public static String decrypt(String content, String charSet) {
		try {
			byte[] deContent = Base64.decodeBase64(content.getBytes());
			byte[] decryptBytes = decipher.doFinal(deContent);
			String result = new String(decryptBytes, charSet);
			return result; // 加密
		} catch (Exception e) {
//			logger.error("AES解密失败, error: {}", e.getMessage());
		}
		return content;
	}

	/**
	 * 将两个ASCII字符合成一个字节； 如："EF"--> 0xEF
	 * 
	 * @param src0
	 *            byte
	 * @param src1
	 *            byte
	 * @return byte
	 */
	public static byte uniteBytes(byte src0, byte src1) {
		byte _b0 = Byte.decode("0x" + new String(new byte[] { src0 })).byteValue();
		_b0 = (byte) (_b0 << 4);
		byte _b1 = Byte.decode("0x" + new String(new byte[] { src1 })).byteValue();
		byte ret = (byte) (_b0 ^ _b1);
		return ret;
	}

	/**
	 * 将指定字符串src，以每两个字符分割转换为16进制形式 如："2B44EFD9" --> byte[]{0x2B, 0x44, 0xEF, 0xD9}
	 * 
	 * @param src
	 *            String
	 * @return byte[]
	 */
	public static byte[] HexString2Bytes(String src) {
		int len = src.length() / 2;
		byte[] ret = new byte[len];
		byte[] tmp = src.getBytes();
		for (int i = 0; i < len; i++) {
			ret[i] = uniteBytes(tmp[i * 2], tmp[i * 2 + 1]);
		}
		return ret;
	}

	@SuppressWarnings("unused")
	private static String showByteArray(byte[] data) {
		if (null == data) {
			return null;
		}
		StringBuilder sb = new StringBuilder("{");
		for (byte b : data) {
			sb.append(b).append(",");
		}
		sb.deleteCharAt(sb.length() - 1);
		sb.append("}");
		return sb.toString();
	}

}