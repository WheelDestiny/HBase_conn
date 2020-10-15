package com.wheelDestiny.HBase.Util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * HBase操作工具类
 *
 */
public class HBaseUtil {
    //ThreadLocal
    private static ThreadLocal<Connection> connectionThreadLocal = new ThreadLocal<>();


    private HBaseUtil() {
    }
    /**
     * 获取HBase的链接对象，如果没有创建新的对象，并将连接对象放入ThreadLocal，方便获取
     * @return
     */
    public static void makeHBaseConnection() throws IOException {
        Connection connection = connectionThreadLocal.get();
        if(connection==null){
            Configuration conf = HBaseConfiguration.create();
            connection = ConnectionFactory.createConnection(conf);
            connectionThreadLocal.set(connection);
        }

    }

    public static void insertData(String tableName,String rowKey,String family,String column,String value) throws IOException {
        Connection connection = connectionThreadLocal.get();
        Table table = connection.getTable(TableName.valueOf(tableName));

        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(family),Bytes.toBytes(column),Bytes.toBytes(value));
        table.put(put);
        table.close();
    }

    /**
     * 关闭连接，清空ThreadLocal
     * @throws IOException
     */
    public static void close() throws IOException {
        Connection conn = connectionThreadLocal.get();
        if (conn!=null){
            conn.close();
            connectionThreadLocal.remove();
        }
    }

    /**
     * 生成分区号
     * @param rowKey
     * @param regionCount
     * @return
     */
    public static String genRegionNum(String rowKey,int regionCount){
        int regionNum;
        int hash = rowKey.hashCode();
        if(regionCount>0&&(regionCount&(regionCount-1))==0){
            //2的n次方
            regionNum = hash & (regionCount - 1);
        } else {
            regionNum = hash % (regionCount);
        }
        return regionNum+"_"+rowKey;
    }

    /**
     * 生成分区键
     * @return
     */
    public static byte[][] genRegionKeys(int regionCount){
        byte[][] bs = new byte[regionCount-1][];

        for (int i = 0; i <regionCount-1 ; i++) {
            bs[i] = Bytes.toBytes(i+"|");
        }
        return bs;
    }


}
