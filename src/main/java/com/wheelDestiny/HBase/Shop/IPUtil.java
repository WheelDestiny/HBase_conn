/**
 * IPUtil.java
 * com.hainiuxy.mrrun.util
 * Copyright (c) 2018, 海牛版权所有.
 * @author   潘牛                      
*/

package com.wheelDestiny.HBase.Shop;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * ip工具
 * 
 * @author 潘牛
 * @Date 2018年11月1日
 */
public class IPUtil {

	private TreeMap<Long, String> ipMap = new TreeMap<>();

	public void loadIPFile() {
		InputStream is = IPUtil.class.getResourceAsStream("/ip.dat");
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new InputStreamReader(is, "UTF-8"));
			String p = "((\\d{1,3}\\.){3}\\d{1,3})\\s+((\\d{1,3}\\.){3}\\d{1,3})\\s+(.+)";
			String line = "";
			Pattern pattern = null;
			Matcher matcher = null;
			String ip2 = "";
			String addr = "";
			long ip2L = 0L;

			while ((line = reader.readLine()) != null) {
				pattern = Pattern.compile(p);
				matcher = pattern.matcher(line);
				if (matcher.find()) {
					ip2 = matcher.group(3);
//					String group1 = matcher.group(1);
//					String group2 = matcher.group(2);
//					String group4 = matcher.group(4);
					String ipStr = matcher.group(5);
					addr = ipStr.split(" ")[0];
					ip2L = IPUtil.ip2long(ip2);
					ipMap.put(Long.valueOf(ip2L), addr);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			if(reader != null){
				try {
					reader.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			if(is != null){
				try {
					is.close();
				} catch (IOException e) {
					
					e.printStackTrace();
					
				}
			}
		}
	}
	
	/**
	 * 根据ip地址获取区域名称
	 * @param ip
	 * @return 区域名称
	*/
	public String getIpArea(String ip){
		long iptemp = IPUtil.ip2long(ip);
		String area = null;
		try{
			Long tempKey = (Long) ipMap.ceilingKey(Long.valueOf(iptemp));
			area = ipMap.get(tempKey);
			
		}catch(Exception e){
			area = "未知地区";
		}
		
		return area;
		
	}

	/**
	 * long类型转ip地址
	 * @param ip ip地址的long类型
	 * @return ip地址
	*/
	public static String long2ip(long ip) {
		int[] b = new int[4];
		String x = "";

		b[0] = (int) ((ip >> 24) & 0xff);
		b[1] = (int) ((ip >> 16) & 0xff);
		b[2] = (int) ((ip >> 8) & 0xff);
		b[3] = (int) (ip & 0xff);
		x = Integer.toString(b[0]) + "." + Integer.toString(b[1]) + "." + Integer.toString(b[2]) + "."
				+ Integer.toString(b[3]);

		return x;
	}
	

	/**
	 * 将ip转化成long型
	 * @param ip 字符串ip地址
	 * @return long 类型的数据
	*/
	public static long ip2long(String ip) {
		String[] fields = ip.split("\\.");
		if (fields.length != 4) {
			return 0L;
		}
		
		long r = Long.parseLong(fields[0]) << 24;
		r |= Long.parseLong(fields[1]) << 16;
		r |= Long.parseLong(fields[2]) << 8;
		r |= Long.parseLong(fields[3]);
//		System.out.println(r);
		return r;
	}
	
	public static void main(String[] args) {
//		IPUtil util = new IPUtil();
//		util.loadIPFile();
//		System.out.println(util.getIpArea("1.24.200.1"));
//		System.out.println(IPUtil.long2ip(3232258050L));
	}

}
