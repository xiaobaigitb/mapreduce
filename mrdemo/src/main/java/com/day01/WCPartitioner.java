package com.day01;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


public class WCPartitioner extends Partitioner<Text,IntWritable> {
    //int：表示你要返回的分区编号
    public int getPartition(Text key, IntWritable value, int i) {
        String head = key.toString().substring(0, 1).toLowerCase();
        if (head.compareTo("m")>=0){
            return 0;
        }else {
            return 1;
        }
    }
}
