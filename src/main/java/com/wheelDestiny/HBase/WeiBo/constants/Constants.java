package com.wheelDestiny.HBase.WeiBo.constants;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

public class Constants {

    //HBase配置信息
    public final static Configuration CONFIGURATION = HBaseConfiguration.create();
    //命名空间
    public final static String NAMESPACE = "weibo";
    //微博内容表
    public final static String CONTENT_TABLE = "weibo:content";
    public final static String CONTENT_TABLE_CF = "info";
    public final static int CONTENT_TABLE_VERSION = 1;


    //用户关系表
    public final static String RELATION_TABLE = "weibo:relation";
    public final static String RELATION_TABLE_CF1 = "attends";
    public final static String RELATION_TABLE_CF2 = "fans";
    public final static int RELATION_TABLE_VERSION = 1;

    //收件箱表
    public final static String INBOX_TABLE = "weibo:inbox";
    public final static String INBOX_TABLE_CF = "info";
    public final static int INBOX_TABLE_VERSION = 2;

}
