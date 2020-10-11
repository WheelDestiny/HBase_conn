/**
 * Constants.java
 * com.hainiuxy.mrrun.util
 * Copyright (c) 2018, 海牛版权所有.
 * @author   潘牛                      
*/

package com.wheelDestiny.HBase.Util;

/**
 * 常量类
 * @author   潘牛                      
 * @Date	 2018年11月27日 	 
 */
public class Constants {
	
	/**
	 * 个性化的任务id，用-D参数来传
	 */
	public static final String TASK_ID_ATTR = "task.id";
	

	/**
	 * 首个任务输入目录，用-D参数来传，如 -Dtask.input.dir=/tmp/mr/task/input
	 */
	public static final String TASK_INPUT_DIR_ATTR = "task.input.dir";
	
	/**
	 * 输出目录的根目录，用-D参数来传，如 -Dtask.base.dir=/tm/mr/task
	 */
	public static final String TASK_BASE_DIR_ATTR = "task.base.dir";
	/**
	 * hbase表名
	 */
	public static final String HBASE_TABLE_NAME_ATTR = "hbase.table.name";
	/**
	 * 是否创建hbase表
	 */
	public static final String IS_CREATE_HBASE_TABLE_NAME_ATTR = "is.create.table";

	/**
	 * 快照名称
	 */
	public static final String HBASE_TABLE_SNAPSHOT_NAME_ATTR = "hbase.table.snapshot.name";
	/**
	 * 读取hbase快照用到的临时目录
	 */
	public static final String HBASE_TABLE_SNAPSHOT_RESTORE_PATH_ATTR = "hbase.snapshot.restore";


	

			

	
	

}

