/**
 * OrcFormat.java
 * com.hainiuxy.mr.run.util
 * Copyright (c) 2019, 海牛版权所有.
 * @author   潘牛                      
*/

package com.wheelDestiny.HBase.Util;

/**
 * 
 * @author   潘牛                      
 * @Date	 2019年6月5日 	 
 */
public class OrcFormat {

	public static String SCHEMA = "struct<aid:string,pkgname:string,uptime:bigint,type:int,country:string,gpcategory:string>";

	public static String SCHEMA2 = "struct<aid:string,pkgname:string,uptime:bigint,type:int,country:string>";

	public static String SCHEMA0927 = "struct<uid:string,req_time:string,req_url:string>";
}

