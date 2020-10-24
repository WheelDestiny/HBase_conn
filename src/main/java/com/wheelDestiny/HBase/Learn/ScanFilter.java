package com.wheelDestiny.HBase.Learn;

import com.wheelDestiny.HBase.WeiBo.constants.Constants;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class ScanFilter {


    public static void main(String[] args) throws IOException {
        scanTable();
    }

    public static void scanTable() throws IOException {
        try(
                Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);
                Table empTable = connection.getTable(TableName.valueOf("wheeldestiny:emp"));
                ) {
            Scan scan = new Scan();

            SingleColumnValueFilter singleColumnValueFilter =
                    new SingleColumnValueFilter(Bytes.toBytes("info"),
                            Bytes.toBytes("name"), CompareFilter.CompareOp.EQUAL,Bytes.toBytes("feng"));
            scan.setFilter(singleColumnValueFilter);

            ResultScanner scanner = empTable.getScanner(scan);

            scanner.forEach(res -> {
                for (Cell cell : res.rawCells()) {
                    cell.getTimestamp();
                    System.out.println(
                            "RK:" + Bytes.toString(CellUtil.cloneRow(cell)) +
                                    ",CF:" + Bytes.toString(CellUtil.cloneFamily(cell)) +
                                    ",CN:" + Bytes.toString(CellUtil.cloneQualifier(cell)) +
                                    ",VL:" + Bytes.toString(CellUtil.cloneValue(cell))
                    );
                }
            });
        }
    }
}
