package com.wheelDestiny.HBase.Learn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * 测试Hbase的API
 */
public class TestHbaseAPI {
    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();

        TableName tableName =TableName.valueOf("wheeldestiny:emp");
        boolean exists = admin.tableExists(tableName);
        System.out.println(exists);


        Table table = connection.getTable(tableName);

        Scan scan = new Scan();
//        scan.addFamily(Bytes.toBytes("info"));

//        ByteArrayComparable bc = new BinaryComparator(Bytes.toBytes("1001"));
//        RegexStringComparator regexStringComparator = new RegexStringComparator("^\\d{3}$");
//
//        //FilterList.Operator.MUST_PASS_ALL == and
//        //FilterList.Operator.MUST_PASS_ONE == or
//        Filter f = new RowFilter(CompareFilter.CompareOp.EQUAL,bc);
//        Filter f1 = new RowFilter(CompareFilter.CompareOp.EQUAL,regexStringComparator);
//        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
//        filterList.addFilter(f);
//        filterList.addFilter(f1);

        //扫描时增加过滤器
//        scan.setFilter(filterList);

        ResultScanner scanner = table.getScanner(scan);
        scanner.forEach((res)->{
            for (Cell cell : res.rawCells()) {
                System.out.println("value = "+Bytes.toString(CellUtil.cloneValue(cell)));
                System.out.println("rowkey = "+Bytes.toString(CellUtil.cloneRow(cell)));
                System.out.println("family = "+Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println("column = "+Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println();
            }
        });

    }
}
