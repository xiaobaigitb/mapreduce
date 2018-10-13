package com.day01;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


public class PhonePartitioner extends Partitioner<Text,IntWritable> {
    //int：表示你要返回的分区编号
    public int getPartition(Text key, IntWritable value, int i) {
        if (key.getLength()==11){
            return 0;
        }else {
            return 1;
        }
    }
}
