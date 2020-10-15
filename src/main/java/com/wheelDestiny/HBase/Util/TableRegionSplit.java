package com.wheelDestiny.HBase.Util;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.RegionSplitter;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

public class TableRegionSplit implements RegionSplitter.SplitAlgorithm {
    @Override
    public byte[] split(byte[] start, byte[] end) {
        return new byte[0];
    }
    //将切分键列表转化为byte类型的二维数组
    @Override
    public byte[][] split(int numRegions) {
        //读取splitdata文件
        Set<String> set =readSplitData();
        byte[][] arr = new byte[set.size()][];
        int index = 0;
        for (String key : set) {
            arr[index++] = Bytes.toBytes(key);
        }
        return arr;
    }
    //读取切分文件
    private Set<String> readSplitData(){
        Set<String> set = new HashSet<>();
        try (
                InputStream is = TableRegionSplit.class.getResourceAsStream("/splitdata");
                BufferedReader reader = new BufferedReader(new InputStreamReader(is,"utf-8"));
                ){
            String line = null;
            while ((line = reader.readLine())!=null){
                set.add(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }


        return set;
    }


    @Override
    public byte[] firstRow() {
        return new byte[0];
    }

    @Override
    public byte[] lastRow() {
        return new byte[0];
    }

    @Override
    public void setFirstRow(String userInput) {

    }

    @Override
    public void setLastRow(String userInput) {

    }

    @Override
    public byte[] strToRow(String input) {
        return new byte[0];
    }

    @Override
    public String rowToStr(byte[] row) {
        return null;
    }

    @Override
    public String separator() {
        return null;
    }

    @Override
    public void setFirstRow(byte[] userInput) {

    }

    @Override
    public void setLastRow(byte[] userInput) {

    }
}
