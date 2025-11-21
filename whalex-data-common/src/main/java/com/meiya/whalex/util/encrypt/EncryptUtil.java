package com.meiya.whalex.util.encrypt;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.binary.Base64;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Random;

/**
 * 字符串MD5加密工具
 * 
 * 
 */
@SuppressWarnings("restriction")
@Slf4j
public class EncryptUtil {

    private static Random rd;

	static {
		try {
			rd = SecureRandom.getInstanceStrong();
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 取得随机密码(明文)
	 * 
	 * @return
	 */
	public static String randsPwd() {
		String pwd = ""; // 保存随机数
		// ! " # $ % & ' ( ) * + , - . / (33-47)
		// 0 1 2 3 4 5 6 7 8 9 (48-57)
		// : ; < = > ? @ (58-64)
		// A B C D E F G H I J K L M N O P Q R S T U V W X Y Z (65-90)
		// [ \ ] ^ _ ` (91-96)
		// a b c d e f g h i j k l m n o p q r s t u v w x y z (97-122)
		// { | } ~ (123-126)
		// 每个都设置pwd.length() < 7则 保证每种字符都有一个，想那个类型的字符多一点，可以通过这个控制，也可以控制每种具体的次数
		int flag1 = 0, flag2 = 0, flag3 = 0, flag4 = 0, flag5 = 0, flag6 = 0, flag7 = 0;
//		Random rd = new Random(); // 创建随机对象
		int rdGet; // 取得随机数
		do {
			int rdInt = rd.nextInt();
			if (rdInt % 7 == 1) {
				if (flag1 > 0 && pwd.length() < 7) {
					continue;
				}
				rdGet = Math.abs(rdInt) % 15 + 33; // 产生33到47的随机数
				flag1++;
			} else if (rdInt % 7 == 2) {
				if (flag2 > 0 && pwd.length() < 4) {
					continue;
				}
				rdGet = Math.abs(rdInt) % 10 + 48; // 产生48到57的随机数
				flag2++;
			} else if (rdInt % 7 == 3) {
				if (flag3 > 0 && pwd.length() < 7) {
					continue;
				}
				rdGet = Math.abs(rdInt) % 7 + 58; // 产生58到64的随机数
				flag3++;
			} else if (rdInt % 7 == 4) {
				if (flag4 > 0 && pwd.length() < 4) {
					continue;
				}
				rdGet = Math.abs(rdInt) % 26 + 65; // 产生65到90的随机数
				flag4++;
			} else if (rdInt % 7 == 5) {
				if (flag5 > 0 && pwd.length() < 7) {
					continue;
				}
				rdGet = Math.abs(rdInt) % 6 + 91; // 产生91到96的随机数
				flag5++;
			} else if (rdInt % 7 == 6) {
				if (flag6 > 0 && pwd.length() < 4) {
					continue;
				}
				rdGet = Math.abs(rdInt) % 26 + 97; // 产生97到122的随机数
				flag6++;
			} else {
				if (flag7 > 0 && pwd.length() < 7) {
					continue;
				}
				rdGet = Math.abs(rdInt) % 4 + 123; // 产生122到123的随机数
				flag7++;
			}
			char num1 = (char) rdGet; // int转换char
			String dd = Character.toString(num1);
			pwd += dd;
		} while (pwd.length() < 8);// 设定长度，此处假定长度小于8
		return pwd;
	}

	/**
	 * 取得随机密码（密文）
	 * 
	 * @return
	 */
	public static String encryPwd() {
		return md5(randsPwd());
	}

	/**
	 * MD5加密
	 * 
	 * @param str
	 *            传递过来的字符串
	 * @return 加蜜后的 24 位长度的字符串
	 */
	public static String md5(String str) {
		String returnStr = null;
		try {
			// 选择加密方式
			MessageDigest md5 = MessageDigest.getInstance("MD5");
            try {
				returnStr = Base64.encodeBase64String(md5.digest(str.getBytes("UTF-8")));
			} catch (UnsupportedEncodingException e) {
				log.error(str, e);
			}
		} catch (NoSuchAlgorithmException e) {
			log.error(str, e);
		}
		return returnStr;
	}

}
