package com.wheelDestiny.HBase.Learn;

import com.wheelDestiny.HBase.Util.HBaseUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * 测试Hbase的API
 */
public class TestHbaseAPI_createTable {
    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);
        System.out.println(connection+"!!!!");


//        Admin admin = connection.getAdmin();
//
//        HTableDescriptor td = new HTableDescriptor(TableName.valueOf("wheeldestiny:user1"));
//
//        HColumnDescriptor cd = new HColumnDescriptor("info");
//        td.addFamily(cd);
//
//        /**
//         * 加|的原因是|在ASCII中是124位，相比较它大的只有}和~，所以一般用|来做分割
//         */
//        byte[][] b = new byte[][]{Bytes.toBytes("0|"),Bytes.toBytes("1|")};
//
//        admin.createTable(td,b);

        //增加数据
        Table table = connection.getTable(TableName.valueOf("wheeldestiny:user1"));

        String rowKey = "zhangsan";

        //将rowkey均匀的分配到不同的分区中

        rowKey = HBaseUtil.genRegionNum(rowKey,3);

        Put put = new Put(Bytes.toBytes(rowKey));

        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("age"),Bytes.toBytes("20"));

        table.put(put);


    }
}
