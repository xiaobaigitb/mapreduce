package com.day02;

import com.google.common.io.Resources;
import com.utils.CDUPUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Iterator;

/**
 * 用MapReduce实现倒排索引
 */
public class ReverseApp {
    public static class ReverseMapper extends Mapper<LongWritable,Text,Text, Text> {
        //map方法每次执行一行数据，会被循环调用map方法（有多少行就调用多少次）
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //获取key所在的文件名
            Path path = ((FileSplit) context.getInputSplit()).getPath();
            String fileName = path.toString();
            String[] strings = value.toString().split(" ");
            for (int i = 0; i < strings.length; i++) {
                context.write(new Text(strings[i]),new Text(fileName));
            }
        }
    }

    public static class ReverseReducer extends Reducer<Text, Text, Text, Text> {
        //每次处理一个key,会被循环调用，有多少个key就会调用几次
        //获取map处理的数据 hello 集合（用于存储key相同的value--1）
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Iterator<Text> iterator = values.iterator();
            StringBuffer buffer = new StringBuffer();
            while (iterator.hasNext()) {
                Text fileName = iterator.next();
                StringBuffer append = buffer.append(fileName.toString()).append("--");
            }
            context.write(key,new Text(String.valueOf(buffer)));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration coreSiteConf = new Configuration();
        coreSiteConf.addResource(Resources.getResource("core-site-master2.xml"));
        //设置一个任务,后面是job的名称
        Job job = Job.getInstance(coreSiteConf, "Reverse");
        //将打的jar包自动上传临时目录，运行之后就删除了--自己看不到
        job.setJar("mrdemo/target/mrdemo-1.0-SNAPSHOT.jar");

        //设置Map和Reduce处理类
        job.setMapperClass(ReverseMapper.class);
        job.setReducerClass(ReverseReducer.class);

        //设置map输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        //设置job/reduce输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //设置任务的输入路径
        FileInputFormat.addInputPath(job, new Path("/wc"));
        //设置任务的输出路径--保存结果(这个目录必须是不存在的目录)
        //删除存在的文件
        CDUPUtils.deleteFileName("/reout");

        FileOutputFormat.setOutputPath(job, new Path("/reout"));
        //运行任务 true：表示打印详情
        boolean flag = job.waitForCompletion(true);
        if (flag){
            System.out.println("使用mapreduce来倒排索引操作的结果...");
            CDUPUtils.readContent("/reout/part-r-00000");
        }else {
            System.out.println(flag+",文件加载失败");
        }
    }
}
