package com.day02;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * java基本数据类型步支持序列化--存放在内存中不在磁盘中（不能被持久化）
 * 用来处理map任务：映射
 * map任务接收的是kv,输出的也是kv 1（行号），hello world
 * 第一个泛型表示：输入key的数据类型 输入的数据相当于文件开头的偏移量（行号）没有实际意义
 * 第二个泛型表示：输入value的数据类型 输入的文件的一行内容
 * 第三个泛型表示：输出key的数据类型 输出的key是一个字符串
 * 第四个泛型表示：输出value的数据类型
 *
 * LongWritable：等价于java中的long
 * Text ：等价与java中的string
 * IntWritable:等价于java中的int
 *
 * XXXWritable 是hadoop定义的基本数据类型，相当于对java中的数据类型做一个封装，同时序列化（可以网络传输以及存储到磁盘上）
 *
 */
public class DRMapper extends Mapper<LongWritable,Text,Text,NullWritable> {
    //map方法每次执行一行数据，会被循环调用map方法（有多少行就调用多少次）
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //将一行数据（text类型）变为string类型
        String line = value.toString();
        String[] words = line.split(" ");
        //便利单词，输出word 1
        for (int i = 0; i < words.length; i++) {
            Text keyOut = new Text(words[i]);
            System.out.println(i+"-----"+words[i]);
            //输出word 1---给reduce接着处理
            context.write(keyOut,NullWritable.get());
        }
    }
}
