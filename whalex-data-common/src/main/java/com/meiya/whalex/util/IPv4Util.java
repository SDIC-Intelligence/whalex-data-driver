package com.meiya.whalex.util;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;

/**
 * IPV4处理工具类
 *
 * @author 黄河森
 * @date 2019/9/10
 * @project whale-cloud-platformX
 */
public class IPv4Util {

	private final static int INADDRSZ = 4;

	/**
	 * 把IP地址转化为字节数组
	 * 
	 * @param ipAddr
	 * @return byte[]
	 */
	public static byte[] ipToBytesByInet(String ipAddr) {
		try {
			return InetAddress.getByName(ipAddr).getAddress();
		} catch (Exception e) {
			throw new IllegalArgumentException(ipAddr + " is invalid IP");
		}
	}

	/**
	 * 把IP地址转化为int
	 * 
	 * @param ipAddr
	 * @return int
	 */
	public static byte[] ipToBytesByReg(String ipAddr) {
		byte[] ret = new byte[4];
		try {
			String[] ipArr = ipAddr.split("\\.");
			ret[0] = (byte) (Integer.parseInt(ipArr[0]) & 0xFF);
			ret[1] = (byte) (Integer.parseInt(ipArr[1]) & 0xFF);
			ret[2] = (byte) (Integer.parseInt(ipArr[2]) & 0xFF);
			ret[3] = (byte) (Integer.parseInt(ipArr[3]) & 0xFF);
			return ret;
		} catch (Exception e) {
			throw new IllegalArgumentException(ipAddr + " is invalid IP");
		}

	}

	/**
	 * 字节数组转化为IP
	 * 
	 * @param bytes
	 * @return int
	 */
	public static String bytesToIp(byte[] bytes) {
		return new StringBuilder().append(bytes[0] & 0xFF).append('.')
				.append(bytes[1] & 0xFF).append('.').append(bytes[2] & 0xFF)
				.append('.').append(bytes[3] & 0xFF).toString();
	}

	/**
	 * 根据位运算把 byte[] -> int
	 * 
	 * @param bytes
	 * @return int
	 */
	public static int bytesToInt(byte[] bytes) {
		int addr = bytes[3] & 0xFF;
		addr |= ((bytes[2] << 8) & 0xFF00);
		addr |= ((bytes[1] << 16) & 0xFF0000);
		addr |= ((bytes[0] << 24) & 0xFF000000);
		return addr;
	}

	/**
	 * 把IP地址转化为int
	 * 
	 * @param ipAddr
	 * @return int
	 */
	public static int ipToInt(String ipAddr) {
		try {
			return bytesToInt(ipToBytesByInet(ipAddr));
		} catch (Exception e) {
			throw new IllegalArgumentException(ipAddr + " is invalid IP");
		}
	}

	/**
	 * ipInt -> byte[]
	 * 
	 * @param ipInt
	 * @return byte[]
	 */
	public static byte[] intToBytes(int ipInt) {
		byte[] ipAddr = new byte[INADDRSZ];
		ipAddr[0] = (byte) ((ipInt >>> 24) & 0xFF);
		ipAddr[1] = (byte) ((ipInt >>> 16) & 0xFF);
		ipAddr[2] = (byte) ((ipInt >>> 8) & 0xFF);
		ipAddr[3] = (byte) (ipInt & 0xFF);
		return ipAddr;
	}

	/**
	 * 把int->ip地址
	 * 
	 * @param ipInt
	 * @return String
	 */
	public static String intToIp(int ipInt) {
		return new StringBuilder().append(((ipInt >> 24) & 0xff)).append('.')
				.append((ipInt >> 16) & 0xff).append('.')
				.append((ipInt >> 8) & 0xff).append('.').append((ipInt & 0xff))
				.toString();
	}

	/**
	 * 把192.168.1.1/24 转化为int数组范围
	 * 
	 * @param ipAndMask
	 * @return int[]
	 */
	public static int[] getIPIntScope(String ipAndMask) {

		String[] ipArr = ipAndMask.split("/");
		if (ipArr.length != 2) {
			throw new IllegalArgumentException("invalid ipAndMask with: "
					+ ipAndMask);
		}
		int netMask = Integer.valueOf(ipArr[1].trim());
		if (netMask < 0 || netMask > 31) {
			throw new IllegalArgumentException("invalid ipAndMask with: "
					+ ipAndMask);
		}
		int ipInt = IPv4Util.ipToInt(ipArr[0]);
		int netIP = ipInt & (0xFFFFFFFF << (32 - netMask));
		int hostScope = (0xFFFFFFFF >>> netMask);
		return new int[] { netIP, netIP + hostScope };

	}

	/**
	 * 把192.168.1.1/24 转化为IP数组范围
	 * 
	 * @param ipAndMask
	 * @return String[]
	 */
	public static String[] getIPAddrScope(String ipAndMask) {
		int[] ipIntArr = IPv4Util.getIPIntScope(ipAndMask);
		return new String[] { IPv4Util.intToIp(ipIntArr[0]),
				IPv4Util.intToIp(ipIntArr[0]) };
	}

	/**
	 * 根据IP 子网掩码（192.168.1.1 255.255.255.0）转化为IP段
	 * 
	 * @param ipAddr
	 *            ipAddr
	 * @param mask
	 *            mask
	 * @return int[]
	 */
	public static int[] getIPIntScope(String ipAddr, String mask) {

		int ipInt;
		int netMaskInt = 0, ipcount = 0;
		try {
			ipInt = IPv4Util.ipToInt(ipAddr);
			if (null == mask || "".equals(mask)) {
				return new int[] { ipInt, ipInt };
			}
			netMaskInt = IPv4Util.ipToInt(mask);
			ipcount = IPv4Util.ipToInt("255.255.255.255") - netMaskInt;
			int netIP = ipInt & netMaskInt;
			int hostScope = netIP + ipcount;
			return new int[] { netIP, hostScope };
		} catch (Exception e) {
			throw new IllegalArgumentException("invalid ip scope express  ip:"
					+ ipAddr + "  mask:" + mask);
		}

	}

	/**
	 * 根据IP 子网掩码（192.168.1.1 255.255.255.0）转化为IP段
	 * 
	 * @param ipAddr
	 *            ipAddr
	 * @param mask
	 *            mask
	 * @return String[]
	 */
	public static String[] getIPStrScope(String ipAddr, String mask) {
		int[] ipIntArr = IPv4Util.getIPIntScope(ipAddr, mask);
		return new String[] { IPv4Util.intToIp(ipIntArr[0]),
				IPv4Util.intToIp(ipIntArr[0]) };
	}

	/**
	 * 将long数据转为IP地址串
	 * @author pengky
	 * @param longIp
	 * @return
	 */
	public static String longToIp(long longIp) {
		StringBuilder sb = new StringBuilder("");
		// 直接右移24位
		sb.append(String.valueOf((longIp >>> 24)));
		sb.append(".");
		// 将高8位置0，然后右移16位
		sb.append(String.valueOf((longIp & 0x00FFFFFF) >>> 16));
		sb.append(".");
		// 将高16位置0，然后右移8位
		sb.append(String.valueOf((longIp & 0x0000FFFF) >>> 8));
		sb.append(".");
		// 将高24位置0
		sb.append(String.valueOf((longIp & 0x000000FF)));
		return sb.toString();
	}
	
	/**
	 * 将IP地址转为long数据
	 * @author pengky
	 * @param strIP
	 * @return
	 */
	public static long ipToLong(String strIP) {
		long[] ip=new long[4];
		int position1=strIP.indexOf(".");
		int position2=strIP.indexOf(".",position1+1);
		int position3=strIP.indexOf(".",position2+1);
		ip[0] = Long.parseLong(strIP.substring(0,position1));
		ip[1] = Long.parseLong(strIP.substring(position1+1,position2));
		ip[2] = Long.parseLong(strIP.substring(position2+1,position3));
		ip[3] = Long.parseLong(strIP.substring(position3+1));
		return (ip[0]<<24)+(ip[1]<<16)+(ip[2]<<8)+ip[3];
	}
	


    /**
     * 判断一个ip是否是内网ip<br/>
     * 私有IP: A类 10.0.0.0 ~ 10.255.255.255 <br/>
     *         B类 172.16.0.0 ～ 172.31.255.255 <br/>
     *         C类 192.168.0.0 ~ 192.168.255.255 <br/>
     * @param ip ip地址
     * @return 是否是内网ip
     */
    public static boolean isInnerIp(String ip){
        return isIpIn(ip, "10.0.0.0", "10.255.255.255")
                || isIpIn(ip, "172.16.0.0", "172.31.255.255")
                || isIpIn(ip, "192.168.0.0", "192.168.255.255");
    }

    /**
     * 判断一个ip是否是环回ip (127.0.0.1 ~ 127.255.255.255是环回地址)
     * @param ip ip地址
     * @return 是否是环回ip
     */
    public static boolean isLoopbackIp(String ip){
        return isIpIn(ip, "127.0.0.1", "127.255.255.255");
    }

    /**
     * 判断ip是否落在范围内
     * @param beginIp 开始的ip
     * @param endIp 结束的ip
     * @return
     */
    public static boolean isIpIn(String targetIp, String beginIp, String endIp){
        long targetIpL = ipToLong(targetIp);
        long beginIpL = ipToLong(beginIp);
        long endIpL = ipToLong(endIp);

        return targetIpL >= beginIpL && targetIpL <= endIpL;
    }

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main2(String[] args) throws Exception {
		String ipAddr = "192.168.8.1";

		byte[] bytearr = IPv4Util.ipToBytesByInet(ipAddr);

		StringBuilder byteStr = new StringBuilder();

		for (byte b : bytearr) {
			if (byteStr.length() == 0) {
				byteStr.append(b);
			} else {
				byteStr.append("," + b);
			}
		}
		System.out.println("IP: " + ipAddr + " ByInet --> byte[]: [ " + byteStr
				+ " ]");

		bytearr = IPv4Util.ipToBytesByReg(ipAddr);
		byteStr = new StringBuilder();

		for (byte b : bytearr) {
			if (byteStr.length() == 0) {
				byteStr.append(b);
			} else {
				byteStr.append("," + b);
			}
		}
		System.out.println("IP: " + ipAddr + " ByReg  --> byte[]: [ " + byteStr
				+ " ]");

		System.out.println("byte[]: " + byteStr + " --> IP: "
				+ IPv4Util.bytesToIp(bytearr));

		int ipInt = IPv4Util.ipToInt(ipAddr);

		System.out.println("IP: " + ipAddr + "  --> int: " + ipInt);

		System.out.println("int: " + ipInt + " --> IP: "
				+ IPv4Util.intToIp(ipInt));

		String ipAndMask = "192.168.1.1/24";

		int[] ipscope = IPv4Util.getIPIntScope(ipAndMask);
		System.out.println(ipAndMask + " --> int地址段：[ " + ipscope[0] + ","
				+ ipscope[1] + " ]");

		System.out.println(ipAndMask + " --> IP 地址段：[ "
				+ IPv4Util.intToIp(ipscope[0]) + ","
				+ IPv4Util.intToIp(ipscope[1]) + " ]");

		String ipAddr1 = "192.168.1.1", ipMask1 = "255.255.255.0";

		int[] ipscope1 = IPv4Util.getIPIntScope(ipAddr1, ipMask1);
		System.out.println(ipAddr1 + " , " + ipMask1 + "  --> int地址段 ：[ "
				+ ipscope1[0] + "," + ipscope1[1] + " ]");

		System.out.println(ipAddr1 + " , " + ipMask1 + "  --> IP地址段 ：[ "
				+ IPv4Util.intToIp(ipscope1[0]) + ","
				+ IPv4Util.intToIp(ipscope1[1]) + " ]");

	}
	
	/**
	 * 获取服务器IP地址
	 * 
	 * @return
	 */
	public static String serverIp() {
		String serverIp = "127.0.0.1";
		try {
			serverIp = InetAddress.getLocalHost().getHostAddress();
		} catch (Exception e) {
			Enumeration<NetworkInterface> netInterEnumeration = null;
			try {
				netInterEnumeration = NetworkInterface.getNetworkInterfaces();
				while (netInterEnumeration.hasMoreElements()) {
					NetworkInterface networkInterface = netInterEnumeration
							.nextElement();
					
					Enumeration<InetAddress> ips = networkInterface.getInetAddresses();
					while (ips.hasMoreElements()) {
						InetAddress ip = (InetAddress)ips.nextElement();
						if (ip!=null && ip instanceof Inet4Address) {
							serverIp = ip.getHostAddress();
							if (!"127.0.0.1".equals(serverIp)) {
								return serverIp;
							}
						}
					}
				}
			} catch (Exception e1) {
				e1.printStackTrace();
			}
		}
		return serverIp;
	}
}