package com.day01;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * 用来处理reduce任务：合并（单词计数）
 * 在reduce端框架会将相同的key的value放在一个集合（迭代器）
 */
public class WCCombiner extends Reducer<Text,IntWritable,Text,IntWritable> {

    //每次处理一个key,会被循环调用，有多少个key就会调用几次
    //获取map处理的数据 hello 集合（用于存储key相同的value--1）
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //获取迭代器
        Iterator<IntWritable> iterator = values.iterator();
        int count = 0;
        while (iterator.hasNext()){
            IntWritable one = iterator.next();
            count+=one.get();
        }
        //context的write只接受hadoop的数据类型，不接受java的数据类型
        context.write(key,new IntWritable(count));
    }
}
